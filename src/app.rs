//! # Processor application — wiring routes to the runtime
//!
//! Reads the routes from `component.instances[]` (overlaid with `component.global.defaults`), and
//! for each route: builds the pipeline, resolves topic templates, opens a bounded channel + a side
//! control channel, and spawns the route worker. Subscriptions are then established **once per
//! unique topic filter**, with the handler applying the UNS **self-echo guard** and fanning each
//! message out to every route that subscribed that filter (so multiple routes can share a topic —
//! ggcommons keys subscriptions by filter). On shutdown it aborts the metric emitter, unsubscribes,
//! closes the channels, and waits for the workers to drain (final aggregate flush).
//!
//! ## UNS observability + control (net-new)
//! Beyond the library-automatic `state` keepalive / `cfg` publisher / `cmd` inbox, the app wires:
//! - per-route [`RouteStats`] counters, surfaced by the `get-stats` command and emitted as the
//!   `metric` class (see [`crate::observe`]);
//! - an [`EvtEmitter`] for the processor's own `evt` health events;
//! - the custom command verbs `flush` / `get-stats` / `pause` / `resume` (registered on
//!   `gg.commands()`), which the built-in `ping` / `reload-config` / `get-configuration` complement.
//!
//! ## Self-echo guard (loop safety)
//! Under a fleet `ecv1/+/+/+/data/#` input a `local` republish onto the processor's own `data`
//! class would be re-consumed → an amplifying loop. The dispatcher restamps `local` output with the
//! processor's identity (see [`crate::proc::route`]) and this fan-out handler drops any inbound
//! message whose `identity` device+component equal the processor's own — so a re-consumed copy is
//! discarded. Cross-device processor chaining still works (a different device does not match).

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use ggcommons::config::model::Config;
use ggcommons::config::template::resolve;
use ggcommons::messaging::message::{Message, MessageIdentity};
use ggcommons::messaging::MessagingService;
use ggcommons::prelude::*;
use ggcommons::uns::reserved_class_of;
use rhai::Engine;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::config::{parse_target, GlobalDefaults, RouteConfig, ScriptEngineKind};
use crate::observe::{spawn_metric_emitter, EvtEmitter, RouteStats};
use crate::proc::route::{run_worker, Dispatcher};
use crate::proc::{now_ms, Control, Pipeline, ProcMsg};

/// Default aggregation/partition key when neither the route nor the global defaults set one.
const DEFAULT_KEY: &str = "body.signal.id";
/// Default route channel capacity (also the broker-side subscribe queue depth).
const DEFAULT_QUEUE: usize = 256;
/// Depth of a route's out-of-band control channel (the `flush` command verb).
const CONTROL_QUEUE: usize = 4;

/// The routes registered on one subscribe filter: each a `(worker data sender, route counters)`
/// pair the fan-out handler forwards to.
type FilterRoutes = Vec<(mpsc::Sender<ProcMsg>, Arc<RouteStats>)>;

/// One fully wired route (produced by [`ProcessorApp::build_route`]).
struct BuiltRoute {
    id: String,
    filters: Vec<String>,
    tx: mpsc::Sender<ProcMsg>,
    control: mpsc::Sender<Control>,
    stats: Arc<RouteStats>,
}

/// A command-facing route handle. Deliberately holds **no** data sender, so the app remains the
/// sole owner of the data channels — dropping [`ProcessorApp::senders`] on shutdown then closes
/// every worker (the control sender kept here does not gate worker shutdown).
struct RouteHandle {
    id: String,
    control: mpsc::Sender<Control>,
    stats: Arc<RouteStats>,
}

/// Per-route-invariant build context: the shared Rhai engine, the script-file loader, the
/// cross-route defaults, and the component identity injected into scripts. Bundled so `build_route`
/// takes the route plus one context, not a long argument list.
struct RouteBuildCtx<'a> {
    engine: &'a Arc<Engine>,
    loader: &'a crate::proc::script::ScriptLoader,
    default_key: &'a str,
    default_target: Option<&'a str>,
    /// `{ThingName}` — raw (not topic-sanitized), injected into scripts as `thingName`.
    thing_name: &'a str,
    /// `{ComponentName}` — the short name (segment after the last `.`), injected as `componentName`.
    component_name: &'a str,
    /// `{ComponentFullName}` — the fully-qualified name, injected as `componentFullName`.
    component_full_name: &'a str,
    /// Default script engine (from `global.defaults.scriptEngine`); a route may override per-route.
    default_script_engine: ScriptEngineKind,
}

/// The running processor: its subscriptions, channel senders, worker tasks, and metric emitter.
pub struct ProcessorApp {
    messaging: Arc<dyn MessagingService>,
    subscriptions: Vec<String>,
    senders: Vec<mpsc::Sender<ProcMsg>>,
    workers: Vec<JoinHandle<()>>,
    metric_task: Option<JoinHandle<()>>,
}

impl ProcessorApp {
    /// Wire and start every configured route.
    pub async fn start(gg: &GgCommons) -> anyhow::Result<Self> {
        let config = gg.config();
        let messaging =
            gg.messaging().map_err(|e| anyhow::anyhow!("messaging transport unavailable: {e}"))?;

        // One Rhai engine shared by all `filter`/`script` stages (bounded to deter runaway scripts).
        let mut engine = Engine::new();
        engine.set_max_operations(1_000_000);
        let engine = Arc::new(engine);

        let defaults: GlobalDefaults = config
            .global()
            .get("defaults")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let default_key = defaults.key.clone().unwrap_or_else(|| DEFAULT_KEY.to_string());
        // Script files (`{"file": "..."}`) resolve relative to this dir (template-substituted).
        let scripts_dir = defaults
            .scripts_dir
            .as_deref()
            .map(|d| resolve(&config, d))
            .unwrap_or_else(|| ".".to_string());
        let loader = crate::proc::script::ScriptLoader::new(scripts_dir);
        // Component identity for the script runtime context. Read the raw values (the template
        // resolver would sanitize them for topic/path safety, which we don't want in a script).
        let component_full_name = config.component_name.clone();
        let component_name =
            component_full_name.rsplit('.').next().unwrap_or(&component_full_name).to_string();
        let thing_name = config.thing_name.clone();
        let ctx = RouteBuildCtx {
            engine: &engine,
            loader: &loader,
            default_key: &default_key,
            default_target: defaults.target.as_deref(),
            thing_name: &thing_name,
            component_name: &component_name,
            component_full_name: &component_full_name,
            default_script_engine: defaults.script_engine.unwrap_or_default(),
        };

        // The processor's own UNS identity — the self-echo guard's match, and the restamp source.
        let own_device = config.identity().device().to_string();
        let own_component = config.identity().component().to_string();
        // The `evt` health-event publisher — a thin wrapper over the library's `events()` facade,
        // bound to the `main` instance (matches the pre-migration `gg.uns()` topic instance).
        let evt = EvtEmitter::new(gg.events());

        let mut app = Self {
            messaging: messaging.clone(),
            subscriptions: Vec::new(),
            senders: Vec::new(),
            workers: Vec::new(),
            metric_task: None,
        };

        // 1. Build each route's worker + channels, collecting the wired routes.
        let mut built: Vec<BuiltRoute> = Vec::new();
        let ids = config.instance_ids();
        if ids.is_empty() {
            tracing::warn!("no routes configured (component.instances[] is empty)");
        }
        for id in ids {
            let Some(raw) = config.instance(&id) else { continue };
            let route: RouteConfig = match serde_json::from_value(raw.clone()) {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(route = %id, error = %e, "invalid route config; skipping");
                    continue;
                }
            };
            match app.build_route(gg, &config, &ctx, &evt, route) {
                Ok(wire) => built.push(wire),
                Err(e) => tracing::error!(error = %e, "failed to build route; skipping"),
            }
        }
        anyhow::ensure!(!app.workers.is_empty(), "no routes started; nothing to do");

        // The app owns the data senders (dropped on shutdown to close the workers).
        app.senders = built.iter().map(|b| b.tx.clone()).collect();

        // 2. Subscribe each UNIQUE filter once, fanning each message out to every route that wants
        //    it (ggcommons keys subscriptions by filter, so a shared filter must be one subscription).
        let mut by_filter: BTreeMap<String, FilterRoutes> = BTreeMap::new();
        for b in &built {
            for f in &b.filters {
                by_filter.entry(f.clone()).or_default().push((b.tx.clone(), b.stats.clone()));
            }
        }
        for (filter, routes) in by_filter {
            self_subscribe(
                &app.messaging,
                &filter,
                routes,
                own_device.clone(),
                own_component.clone(),
                evt.clone(),
            )
            .await?;
            app.subscriptions.push(filter);
        }

        // 3. The periodic `metric`-class emitter (summed per-route counter deltas via gg.metrics()).
        let stats_vec: Vec<Arc<RouteStats>> = built.iter().map(|b| b.stats.clone()).collect();
        app.metric_task = Some(spawn_metric_emitter(gg.metrics(), config.clone(), stats_vec));

        // 4. Register the custom command verbs (flush / get-stats / pause / resume). The built-in
        //    ping / reload-config / get-configuration are already wired by the library.
        let handles: Arc<Vec<RouteHandle>> = Arc::new(
            built
                .iter()
                .map(|b| RouteHandle {
                    id: b.id.clone(),
                    control: b.control.clone(),
                    stats: b.stats.clone(),
                })
                .collect(),
        );
        register_commands(gg, handles);

        tracing::info!(
            routes = app.workers.len(),
            filters = app.subscriptions.len(),
            "telemetry-processor started"
        );
        Ok(app)
    }

    /// Build one route's worker + channels; return the wired route (no subscription yet).
    fn build_route(
        &mut self,
        gg: &GgCommons,
        config: &Config,
        ctx: &RouteBuildCtx<'_>,
        evt: &Arc<EvtEmitter>,
        route: RouteConfig,
    ) -> anyhow::Result<BuiltRoute> {
        let _ = gg; // used only under the `streaming` feature (stream targets)
        let route_key = route.key.clone().unwrap_or_else(|| ctx.default_key.to_string());
        let target_str = route
            .target
            .clone()
            .or_else(|| ctx.default_target.map(String::from))
            .ok_or_else(|| anyhow::anyhow!("route '{}' has no target", route.id))?;
        let target = parse_target(&target_str)?;

        anyhow::ensure!(!route.subscribe.is_empty(), "route '{}' has no subscribe topics", route.id);

        let mut publish = route.publish.clone().unwrap_or_default();
        if let Some(t) = &publish.topic {
            let resolved = resolve(config, t);
            // Defensive: a publish topic that resolves to a reserved UNS class (state|metric|cfg|log)
            // is rejected at publish time by the reserved-class guard (silent drop). Warn at startup.
            if let Some(cls) = reserved_class_of(&resolved, config.effective_include_root()) {
                tracing::warn!(
                    route = %route.id,
                    topic = %resolved,
                    class = cls.token(),
                    "publish.topic targets a RESERVED UNS class; the reserved-class guard will drop \
                     these publishes — target a data/evt/app class instead"
                );
            }
            publish.topic = Some(resolved);
        }

        // Restamp policy: `local` output carries the processor's own identity (instance = route id)
        // — loop-safety for the self-echo guard + correct provenance for the processor's product.
        let restamp: Option<MessageIdentity> = match &target {
            Channel::Local => Some(
                config
                    .identity()
                    .with_instance(&route.id)
                    .map_err(|e| anyhow::anyhow!("route '{}' identity restamp: {e}", route.id))?,
            ),
            _ => None,
        };

        #[cfg(feature = "streaming")]
        let stream = match &target {
            Channel::Stream(name) => Some(
                gg.streams()
                    .stream(name)
                    .map_err(|e| anyhow::anyhow!("stream '{name}' not configured: {e}"))?,
            ),
            _ => None,
        };

        let script_ctx = Arc::new(crate::proc::script::ScriptContext {
            thing_name: ctx.thing_name.to_string(),
            component_name: ctx.component_name.to_string(),
            component_full_name: ctx.component_full_name.to_string(),
            route_id: route.id.clone(),
        });
        let engine_kind = route.script_engine.unwrap_or(ctx.default_script_engine);
        let pipeline = Pipeline::build(
            &route.pipeline,
            &route_key,
            engine_kind,
            ctx.engine,
            ctx.loader,
            &script_ctx,
        )?;

        let stats = RouteStats::new(&route.id);
        let dispatcher = Dispatcher::new(
            self.messaging.clone(),
            target,
            &publish,
            &route_key,
            route.id.clone(),
            stats.clone(),
            evt.clone(),
            restamp,
            #[cfg(feature = "streaming")]
            stream,
        );

        let cap = route.max_queue.map(|n| n as usize).unwrap_or(DEFAULT_QUEUE).max(1);
        let (tx, rx) = mpsc::channel::<ProcMsg>(cap);
        let (control_tx, control_rx) = mpsc::channel::<Control>(CONTROL_QUEUE);
        self.workers.push(tokio::spawn(run_worker(pipeline, rx, control_rx, dispatcher)));

        let filters: Vec<String> = route.subscribe.iter().map(|f| resolve(config, f)).collect();
        for f in &filters {
            tracing::info!(route = %route.id, filter = %f, "route wired");
        }
        Ok(BuiltRoute { id: route.id, filters, tx, control: control_tx, stats })
    }

    /// Run until a shutdown signal, then abort the metric emitter, unsubscribe, close channels, and
    /// drain the workers.
    pub async fn run(mut self, gg: &GgCommons) -> anyhow::Result<()> {
        gg.shutdown_signal().await;
        tracing::info!("shutdown signal received; stopping routes");

        if let Some(task) = self.metric_task.take() {
            task.abort();
        }
        for filt in &self.subscriptions {
            if let Err(e) = self.messaging.unsubscribe(filt).await {
                tracing::warn!(error = %e, filter = %filt, "unsubscribe failed");
            }
        }
        // Close the route channels so each worker drains, does a final flush, and exits.
        drop(self.senders);
        for w in self.workers {
            let _ = w.await;
        }
        Ok(())
    }
}

/// Subscribe one filter with a fan-out handler: apply the UNS self-echo guard, then forward each
/// message to every route channel that registered for it (tallying `messages_in`/`messages_dropped`
/// and the queue-depth gauge, honoring the per-route `paused` flag, and emitting a rate-limited
/// `evt/warning/queue-overflow` on backpressure drops). Concurrency is 1 so ordered consumers get
/// messages in order.
async fn self_subscribe(
    messaging: &Arc<dyn MessagingService>,
    filter: &str,
    routes: FilterRoutes,
    own_device: String,
    own_component: String,
    evt: Arc<EvtEmitter>,
) -> anyhow::Result<()> {
    let filter_owned = filter.to_string();
    messaging
        .subscribe(
            filter,
            message_handler(move |topic, msg| {
                let routes = routes.clone();
                let filter_owned = filter_owned.clone();
                let own_device = own_device.clone();
                let own_component = own_component.clone();
                let evt = evt.clone();
                async move {
                    // Self-echo guard: drop our own re-consumed output (identity device+component
                    // match ours) so a `local` republish onto the consumed `data` class can't loop.
                    if let Some(id) = &msg.identity {
                        if id.device() == own_device && id.component() == own_component {
                            tracing::trace!(topic = %topic, "self-echo dropped (own identity)");
                            return;
                        }
                    }
                    let pm = ProcMsg { topic, msg, recv_ms: now_ms() };
                    for (tx, stats) in &routes {
                        if stats.is_paused() {
                            continue;
                        }
                        match tx.try_send(pm.clone()) {
                            Ok(()) => {
                                stats.messages_in.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                stats.messages_dropped.fetch_add(1, Ordering::Relaxed);
                                tracing::debug!(
                                    filter = %filter_owned,
                                    route = %stats.id,
                                    "route queue full; dropping message"
                                );
                                evt.queue_overflow(&stats.id).await;
                            }
                        }
                        // Update the queue-depth gauge (max_capacity - remaining permits).
                        let depth = tx.max_capacity().saturating_sub(tx.capacity()) as u64;
                        stats.queue_depth.store(depth, Ordering::Relaxed);
                    }
                }
            }),
            DEFAULT_QUEUE,
            1,
        )
        .await?;
    Ok(())
}

/// Register the processor's custom command verbs on the library command inbox (a no-op when no
/// messaging transport wired an inbox). The built-in `ping`/`reload-config`/`get-configuration`
/// verbs are registered by the library and complement these.
fn register_commands(gg: &GgCommons, handles: Arc<Vec<RouteHandle>>) {
    let Some(cmds) = gg.commands() else {
        tracing::debug!("no command inbox (no messaging transport); custom verbs not registered");
        return;
    };

    // get-stats — per-route counters snapshot.
    {
        let handles = handles.clone();
        try_register(&cmds, "get-stats", command_handler(move |_req| {
            let handles = handles.clone();
            async move { Ok(Some(stats_json(&handles))) }
        }));
    }

    // flush — force-close every route's open time windows now; report the total emitted.
    {
        let handles = handles.clone();
        try_register(&cmds, "flush", command_handler(move |_req| {
            let handles = handles.clone();
            async move {
                let mut flushed = 0u64;
                for route in handles.iter() {
                    let (reply_tx, reply_rx) = oneshot::channel();
                    if route.control.send(Control::Flush(reply_tx)).await.is_ok() {
                        if let Ok(n) = reply_rx.await {
                            flushed += n;
                        }
                    }
                }
                Ok(Some(json!({ "flushed": flushed })))
            }
        }));
    }

    // pause — stop enqueuing to a route (body `{route}`), or all routes when omitted.
    {
        let handles = handles.clone();
        try_register(&cmds, "pause", command_handler(move |req| {
            let handles = handles.clone();
            async move { Ok(Some(set_paused(&handles, &req, true))) }
        }));
    }

    // resume — the inverse of pause.
    {
        let handles = handles.clone();
        try_register(&cmds, "resume", command_handler(move |req| {
            let handles = handles.clone();
            async move { Ok(Some(set_paused(&handles, &req, false))) }
        }));
    }
}

/// Register a verb, logging (not failing) if the inbox rejects it.
fn try_register(cmds: &Arc<CommandInbox>, verb: &str, handler: Arc<dyn CommandHandler>) {
    if let Err(e) = cmds.register(verb, handler) {
        tracing::warn!(verb, error = %e, "failed to register command verb");
    }
}

/// The `get-stats` reply body: `{routes: [{id, in, out, dropped, streamAppends, publishFailures,
/// queueDepth, paused}]}`.
fn stats_json(handles: &[RouteHandle]) -> Value {
    let routes: Vec<Value> = handles
        .iter()
        .map(|r| {
            json!({
                "id": r.id,
                "in": r.stats.messages_in.load(Ordering::Relaxed),
                "out": r.stats.messages_out.load(Ordering::Relaxed),
                "dropped": r.stats.messages_dropped.load(Ordering::Relaxed),
                "streamAppends": r.stats.stream_appends.load(Ordering::Relaxed),
                "publishFailures": r.stats.publish_failures.load(Ordering::Relaxed),
                "queueDepth": r.stats.queue_depth.load(Ordering::Relaxed),
                "paused": r.stats.paused.load(Ordering::Relaxed),
            })
        })
        .collect();
    json!({ "routes": routes })
}

/// Apply `paused` to the route named in `request.body.route` (or all routes when absent). Returns
/// `{paused|resumed: [ids...]}`.
fn set_paused(handles: &[RouteHandle], request: &Message, paused: bool) -> Value {
    let route = request.body.get("route").and_then(Value::as_str);
    let mut affected = Vec::new();
    for r in handles {
        if route.is_none() || route == Some(r.id.as_str()) {
            r.stats.paused.store(paused, Ordering::Relaxed);
            affected.push(r.id.clone());
        }
    }
    let key = if paused { "paused" } else { "resumed" };
    json!({ key: affected })
}
