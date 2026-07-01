//! # Processor application — wiring routes to the runtime
//!
//! Reads the routes from `component.instances[]` (overlaid with `component.global.defaults`), and
//! for each route: builds the pipeline, resolves topic templates, opens a bounded channel, and
//! spawns the route worker. Subscriptions are then established **once per unique topic filter**,
//! with the handler fanning each message out to every route that subscribed that filter (so
//! multiple routes can share a topic — ggcommons keys subscriptions by filter). On shutdown it
//! unsubscribes, closes the channels, and waits for the workers to drain (final aggregate flush).

use std::collections::BTreeMap;
use std::sync::Arc;

use ggcommons::config::model::Config;
use ggcommons::config::template::resolve;
use ggcommons::messaging::MessagingService;
use ggcommons::prelude::*;
use rhai::Engine;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::{GlobalDefaults, RouteConfig, ScriptEngineKind, Target};
use crate::proc::route::{run_worker, Dispatcher};
use crate::proc::{now_ms, Pipeline, ProcMsg};

/// Default aggregation/partition key when neither the route nor the global defaults set one.
const DEFAULT_KEY: &str = "body.signal.id";
/// Default route channel capacity (also the broker-side subscribe queue depth).
const DEFAULT_QUEUE: usize = 256;

/// One wired route: its resolved subscribe filters and the channel into its worker.
struct RouteWire {
    filters: Vec<String>,
    tx: mpsc::Sender<ProcMsg>,
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

/// The running processor: its subscriptions, channel senders, and worker tasks.
pub struct ProcessorApp {
    messaging: Arc<dyn MessagingService>,
    subscriptions: Vec<String>,
    senders: Vec<mpsc::Sender<ProcMsg>>,
    workers: Vec<JoinHandle<()>>,
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

        let mut app = Self {
            messaging: messaging.clone(),
            subscriptions: Vec::new(),
            senders: Vec::new(),
            workers: Vec::new(),
        };

        // 1. Build each route's worker + channel, collecting its resolved filters.
        let mut wires: Vec<RouteWire> = Vec::new();
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
            match app.build_route(gg, &config, &ctx, route) {
                Ok(wire) => wires.push(wire),
                Err(e) => tracing::error!(error = %e, "failed to build route; skipping"),
            }
        }
        anyhow::ensure!(!app.workers.is_empty(), "no routes started; nothing to do");

        // 2. Subscribe each UNIQUE filter once, fanning each message out to every route that wants
        //    it (ggcommons keys subscriptions by filter, so a shared filter must be one subscription).
        let mut by_filter: BTreeMap<String, Vec<mpsc::Sender<ProcMsg>>> = BTreeMap::new();
        for wire in &wires {
            for f in &wire.filters {
                by_filter.entry(f.clone()).or_default().push(wire.tx.clone());
            }
        }
        for (filter, senders) in by_filter {
            self_subscribe(&app.messaging, &filter, senders).await?;
            app.subscriptions.push(filter);
        }

        tracing::info!(routes = app.workers.len(), filters = app.subscriptions.len(), "telemetry-processor started");
        Ok(app)
    }

    /// Build one route's worker + channel; return its resolved filters (no subscription yet).
    fn build_route(
        &mut self,
        gg: &GgCommons,
        config: &Config,
        ctx: &RouteBuildCtx<'_>,
        route: RouteConfig,
    ) -> anyhow::Result<RouteWire> {
        let _ = gg; // used only under the `streaming` feature (stream targets)
        let route_key = route.key.clone().unwrap_or_else(|| ctx.default_key.to_string());
        let target_str = route
            .target
            .clone()
            .or_else(|| ctx.default_target.map(String::from))
            .ok_or_else(|| anyhow::anyhow!("route '{}' has no target", route.id))?;
        let target = Target::parse(&target_str)?;

        anyhow::ensure!(!route.subscribe.is_empty(), "route '{}' has no subscribe topics", route.id);

        let mut publish = route.publish.clone().unwrap_or_default();
        if let Some(t) = &publish.topic {
            publish.topic = Some(resolve(config, t));
        }

        #[cfg(feature = "streaming")]
        let stream = match &target {
            Target::Stream(name) => Some(
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
        let dispatcher = Dispatcher::new(
            self.messaging.clone(),
            target,
            &publish,
            &route_key,
            #[cfg(feature = "streaming")]
            stream,
        );

        let cap = route.max_queue.map(|n| n as usize).unwrap_or(DEFAULT_QUEUE).max(1);
        let (tx, rx) = mpsc::channel::<ProcMsg>(cap);
        self.workers.push(tokio::spawn(run_worker(pipeline, rx, dispatcher)));
        self.senders.push(tx.clone());

        let filters: Vec<String> = route.subscribe.iter().map(|f| resolve(config, f)).collect();
        for f in &filters {
            tracing::info!(route = %route.id, filter = %f, "route wired");
        }
        Ok(RouteWire { filters, tx })
    }

    /// Run until a shutdown signal, then unsubscribe, close channels, and drain the workers.
    pub async fn run(self, gg: &GgCommons) -> anyhow::Result<()> {
        gg.shutdown_signal().await;
        tracing::info!("shutdown signal received; stopping routes");

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

/// Subscribe one filter with a fan-out handler that forwards each message to every route channel
/// that registered for it. Concurrency is 1 so the ordered consumers get messages in order.
async fn self_subscribe(
    messaging: &Arc<dyn MessagingService>,
    filter: &str,
    senders: Vec<mpsc::Sender<ProcMsg>>,
) -> anyhow::Result<()> {
    let filter_owned = filter.to_string();
    messaging
        .subscribe(
            filter,
            message_handler(move |topic, msg| {
                let senders = senders.clone();
                let filter_owned = filter_owned.clone();
                async move {
                    let pm = ProcMsg { topic, msg, recv_ms: now_ms() };
                    for s in &senders {
                        if s.try_send(pm.clone()).is_err() {
                            tracing::debug!(filter = %filter_owned, "route queue full; dropping message");
                        }
                    }
                }
            }),
            DEFAULT_QUEUE,
            1,
        )
        .await?;
    Ok(())
}
