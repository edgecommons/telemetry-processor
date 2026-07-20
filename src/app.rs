//! # Processor application — wiring routes to the runtime
//!
//! Reads the routes from `component.instances[]` (overlaid with `component.global.defaults`), and
//! for each route: builds the pipeline, resolves topic templates, opens a bounded channel + a side
//! control channel, and spawns the route worker. Subscriptions are then established **once per
//! unique topic filter**, with the handler applying the UNS **self-echo guard** and fanning each
//! message out to every route that subscribed that filter (so multiple routes can share a topic —
//! edgecommons keys subscriptions by filter). On shutdown it aborts the metric emitter, unsubscribes,
//! closes the channels, and waits for the workers to drain (final aggregate flush).
//!
//! ## UNS observability + control (net-new)
//! Beyond the library-automatic `state` keepalive / `cfg` publisher / `cmd` inbox, the app wires:
//! - per-route [`RouteStats`] counters, surfaced by the `get-stats` command and emitted as the
//!   `metric` class (see [`crate::observe`]);
//! - an [`EvtEmitter`] for the processor's own `evt` health events;
//! - the custom command verbs `flush` / `get-stats` / `pause` / `resume` (registered on
//!   `gg.commands()`), which the built-in `ping` / `reload-config` / `get-configuration` complement;
//! - two `scope: "component"` edge-console panel descriptors (`overview`, `routes`) bound to those
//!   verbs — an optional enhancement (not a baseline requirement for processors).
//!
//! ## Self-echo guard (loop safety)
//! Under a fleet `ecv1/+/+/+/data/#` input a `local` republish onto the processor's own `data`
//! class would be re-consumed → an amplifying loop. The dispatcher restamps `local` output with the
//! processor's identity (see [`crate::proc::route`]) and the fan-out handler ([`crate::dispatch`])
//! drops any inbound message whose `identity` device+component equal the processor's own — so a
//! re-consumed copy is discarded. Cross-device processor chaining still works (a different device
//! does not match).
//!
//! ## Why this file is the coverage seam
//! `ProcessorApp::start`/`build_route`/`run` are the composition root: they obtain a live
//! `Config`/`Arc<dyn MessagingService>`/`EventsFacade`/`Arc<CommandInbox>`/`Arc<dyn StreamService>`
//! from a real `&EdgeCommons`, none of which has a public (or test) constructor outside the
//! `edgecommons` crate. That makes this file untestable without a live `EdgeCommons` — there is no
//! broker or protocol involved, just library types this crate cannot fabricate. Every piece of
//! *logic* that does not need `&EdgeCommons` directly (the fan-out handler, the command/panel
//! registration, the pure helpers, script-output-topic validation) lives in [`crate::dispatch`]
//! instead, where it is fully unit-tested. See `AGENTS.md` and the `coverage` job in
//! `.github/workflows/ci.yml`, which excludes only this file and `main.rs`.

use std::collections::BTreeMap;
use std::sync::Arc;

use edgecommons::config::model::Config;
use edgecommons::config::template::resolve;
use edgecommons::messaging::message::MessageIdentity;
use edgecommons::messaging::MessagingService;
use edgecommons::prelude::*;
use edgecommons::uns::reserved_class_of;
use rhai::Engine;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::{
    parse_target, GlobalDefaults, RouteConfig, ScriptEngineKind, ScriptStageSpec, StageConfig,
};
use crate::dispatch::{self_subscribe, register_commands, validate_script_output_topic, FilterRoutes, RouteHandle};
use crate::observe::{spawn_metric_emitter, EvtEmitter, RouteStats};
use crate::proc::route::{run_worker, Dispatcher};
use crate::proc::{Control, Pipeline, ProcMsg};

/// Default aggregation/partition key when neither the route nor the global defaults set one.
const DEFAULT_KEY: &str = "body.signal.id";
/// Default route channel capacity (also the broker-side subscribe queue depth).
const DEFAULT_QUEUE: usize = 256;
/// Depth of a route's out-of-band control channel (the `flush` command verb).
const CONTROL_QUEUE: usize = 4;

/// One fully wired route (produced by [`ProcessorApp::build_route`]).
struct BuiltRoute {
    id: String,
    filters: Vec<String>,
    tx: mpsc::Sender<ProcMsg>,
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
    pub async fn start(gg: &EdgeCommons) -> anyhow::Result<Self> {
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
        // The `evt` health-event publisher — a thin wrapper over the library's `events()` facade at
        // component scope (D-U28: no instance token, so events land on `.../telemetry-processor/evt/…`).
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
        //    it (edgecommons keys subscriptions by filter, so a shared filter must be one subscription).
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

        // 4. Register the custom command verbs (flush / get-stats / pause / resume) + the console
        //    panel pair. The built-in ping / reload-config / get-configuration are already wired by
        //    the library. A no-op when no messaging transport wired an inbox.
        if let Some(cmds) = gg.commands() {
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
            register_commands(&cmds, handles);
        } else {
            tracing::debug!("no command inbox (no messaging transport); custom verbs not registered");
        }

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
        gg: &EdgeCommons,
        config: &Config,
        ctx: &RouteBuildCtx<'_>,
        evt: &Arc<EvtEmitter>,
        mut route: RouteConfig,
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
        let filters: Vec<String> = route.subscribe.iter().map(|f| resolve(config, f)).collect();

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

        // Resolve + validate every script stage's explicit output topic: reserved classes and
        // subscribe-overlap feedback loops are startup errors, and a route-level `publish.topic`
        // may not silently override a stage output topic.
        for sc in route.pipeline.iter_mut() {
            let StageConfig::Script(ScriptStageSpec::Spec(sp)) = sc else { continue };
            let Some(out) = sp.output.as_mut() else { continue };
            let resolved = resolve(config, &out.topic);
            validate_script_output_topic(
                &route.id,
                &resolved,
                config.effective_include_root(),
                &filters,
                publish.topic.as_deref(),
            )?;
            out.topic = resolved;
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
            // The producer identity for multi-signal script output envelopes (instance = route id).
            identity: config.identity().with_instance(&route.id).ok(),
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

        for f in &filters {
            tracing::info!(route = %route.id, filter = %f, "route wired");
        }
        Ok(BuiltRoute { id: route.id, filters, tx, control: control_tx, stats })
    }

    /// Run until a shutdown signal, then abort the metric emitter, unsubscribe, close channels, and
    /// drain the workers.
    pub async fn run(mut self, gg: &EdgeCommons) -> anyhow::Result<()> {
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
