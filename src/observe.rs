//! # Observability — per-route counters, the `evt` health surface, and `metric` emission
//!
//! The net-new UNS observability the processor gains as a first-class console citizen (the library
//! already gives it the automatic `state` keepalive, the `cfg` publisher, and the `cmd` inbox for
//! free). This module owns three things:
//!
//! - [`RouteStats`] — lock-free per-route counters (`messages_in` / `messages_out` /
//!   `messages_dropped` / `stream_appends` / `publish_failures`) plus a `paused` flag, incremented
//!   by the fan-out handler and the route [`crate::proc::route::Dispatcher`], read by the
//!   `get-stats` command and the metric emitter.
//! - [`EvtEmitter`] — a rate-limited publisher of the processor's own **`evt`** events
//!   (`ecv1/{device}/telemetry-processor/main/evt/<channel>`), built via `gg.uns()` and stamped
//!   with `.from_config()`; used to surface pipeline health (`queue-overflow`, `route-error`,
//!   `stream-unavailable`) to the console.
//! - [`spawn_metric_emitter`] — the periodic task that emits the summed counters as the **`metric`**
//!   class through `gg.metrics()` (interval deltas), mirroring the `uns-bridge` `RelayCounters →
//!   gg.metrics()` pattern. With `metricEmission.target: "messaging"` the messaging metric target
//!   lands them on `ecv1/{device}/telemetry-processor/main/metric/pipeline`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ggcommons::config::model::Config;
use ggcommons::messaging::message::MessageBuilder;
use ggcommons::messaging::MessagingService;
use ggcommons::metrics::{MetricBuilder, MetricService};
use ggcommons::uns::{Uns, UnsClass};
use serde_json::{json, Value};
use tokio::task::JoinHandle;

/// The metric emitted on the UNS `metric` class (channel = the topic's last level).
const PIPELINE_METRIC: &str = "pipeline";
/// Cadence of the metric emission (counters emit as interval deltas every tick).
const METRIC_EMIT_INTERVAL: Duration = Duration::from_secs(30);
/// Per-`evt`-channel cooldown: at most one event of a given channel per this window, so a sustained
/// fault (a full queue, a down stream) can't amplify into an event storm.
const EVT_COOLDOWN: Duration = Duration::from_secs(15);

/// Lock-free per-route counters. `Relaxed` ordering is sufficient — these are monotonic operational
/// tallies read for reporting, not synchronization.
#[derive(Debug)]
pub struct RouteStats {
    /// The owning route id (a `component.instances[].id`).
    pub id: String,
    /// Messages enqueued onto this route's worker channel.
    pub messages_in: AtomicU64,
    /// Messages this route's dispatcher forwarded successfully (published / streamed).
    pub messages_out: AtomicU64,
    /// Messages dropped because this route's worker channel was full (backpressure).
    pub messages_dropped: AtomicU64,
    /// Records this route appended to a durable stream target.
    pub stream_appends: AtomicU64,
    /// Publish failures on the `local` / `northbound` target (and stream-append failures).
    pub publish_failures: AtomicU64,
    /// Current occupancy of this route's worker channel (a gauge the fan-out updates on each
    /// enqueue: `max_capacity - remaining permits`).
    pub queue_depth: AtomicU64,
    /// Whether this route is paused (set by the `pause`/`resume` command verbs); when paused the
    /// fan-out handler skips enqueuing to this route.
    pub paused: AtomicBool,
}

impl RouteStats {
    /// A fresh zeroed counter set for the given route id.
    pub fn new(id: impl Into<String>) -> Arc<RouteStats> {
        Arc::new(RouteStats {
            id: id.into(),
            messages_in: AtomicU64::new(0),
            messages_out: AtomicU64::new(0),
            messages_dropped: AtomicU64::new(0),
            stream_appends: AtomicU64::new(0),
            publish_failures: AtomicU64::new(0),
            queue_depth: AtomicU64::new(0),
            paused: AtomicBool::new(false),
        })
    }

    /// Whether this route is currently paused.
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }
}

/// The processor's rate-limited `evt` publisher. Topics are built through the component-bound
/// [`Uns`] (instance `main`), envelopes are `.from_config()`-stamped, and each event carries the
/// channel as its `header.name`. Publishing is best-effort: `evt` is a non-reserved class, so the
/// reserved-class guard passes; a failed publish is logged at DEBUG and swallowed.
pub struct EvtEmitter {
    messaging: Arc<dyn MessagingService>,
    uns: Uns,
    config: Arc<Config>,
    cooldowns: Mutex<HashMap<String, Instant>>,
}

impl EvtEmitter {
    /// Build an emitter over the component's messaging service, UNS topic builder, and config
    /// snapshot (the last two obtained from `gg.uns()` / `gg.config()`).
    pub fn new(messaging: Arc<dyn MessagingService>, uns: Uns, config: Arc<Config>) -> Arc<EvtEmitter> {
        Arc::new(EvtEmitter { messaging, uns, config, cooldowns: Mutex::new(HashMap::new()) })
    }

    /// Returns `true` if `channel` is outside its cooldown (and records the emit time).
    fn allow(&self, channel: &str) -> bool {
        let mut cds = self.cooldowns.lock().unwrap();
        let now = Instant::now();
        if let Some(last) = cds.get(channel) {
            if now.duration_since(*last) < EVT_COOLDOWN {
                return false;
            }
        }
        cds.insert(channel.to_string(), now);
        true
    }

    /// Publish one event on `evt/<channel>` (rate-limited per channel). No-op while in cooldown.
    async fn emit(&self, channel: &str, body: Value) {
        if !self.allow(channel) {
            return;
        }
        let topic = match self.uns.topic_with_channel(UnsClass::Evt, channel) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, channel, "could not build evt topic; skipping event");
                return;
            }
        };
        let msg = MessageBuilder::new(channel, "1.0").from_config(&self.config).payload(body).build();
        if let Err(e) = self.messaging.publish(&topic, &msg).await {
            tracing::debug!(error = %e, topic = %topic, "evt publish failed");
        }
    }

    /// `evt/queue-overflow` — sustained backpressure dropped a message on `route`.
    pub async fn queue_overflow(&self, route: &str) {
        self.emit("queue-overflow", json!({ "route": route })).await;
    }

    /// `evt/route-error` — a `local`/`northbound` forward failed on `route`.
    pub async fn route_error(&self, route: &str, topic: &str, error: &str) {
        self.emit("route-error", json!({ "route": route, "topic": topic, "error": error })).await;
    }

    /// `evt/stream-unavailable` — a `stream:<name>` target is down / its append failed.
    pub async fn stream_unavailable(&self, route: &str, stream: &str, error: &str) {
        self.emit("stream-unavailable", json!({ "route": route, "stream": stream, "error": error }))
            .await;
    }
}

/// A summed snapshot of every route's counters (fleet-level processor throughput).
#[derive(Default, Clone, Copy)]
struct Totals {
    messages_in: u64,
    messages_out: u64,
    messages_dropped: u64,
    stream_appends: u64,
    publish_failures: u64,
}

impl Totals {
    fn take(stats: &[Arc<RouteStats>]) -> Totals {
        let mut t = Totals::default();
        for s in stats {
            t.messages_in += s.messages_in.load(Ordering::Relaxed);
            t.messages_out += s.messages_out.load(Ordering::Relaxed);
            t.messages_dropped += s.messages_dropped.load(Ordering::Relaxed);
            t.stream_appends += s.stream_appends.load(Ordering::Relaxed);
            t.publish_failures += s.publish_failures.load(Ordering::Relaxed);
        }
        t
    }

    /// The per-interval delta (saturating — counters only grow, but be defensive) as the
    /// measure-name → value map the metric target emits.
    fn delta(&self, prev: &Totals) -> HashMap<String, f64> {
        HashMap::from([
            ("messagesIn".to_string(), self.messages_in.saturating_sub(prev.messages_in) as f64),
            ("messagesOut".to_string(), self.messages_out.saturating_sub(prev.messages_out) as f64),
            (
                "messagesDropped".to_string(),
                self.messages_dropped.saturating_sub(prev.messages_dropped) as f64,
            ),
            (
                "streamAppends".to_string(),
                self.stream_appends.saturating_sub(prev.stream_appends) as f64,
            ),
            (
                "publishFailures".to_string(),
                self.publish_failures.saturating_sub(prev.publish_failures) as f64,
            ),
        ])
    }
}

/// The five measures of the `pipeline` metric (also the `delta` map keys).
const PIPELINE_MEASURES: [&str; 5] =
    ["messagesIn", "messagesOut", "messagesDropped", "streamAppends", "publishFailures"];

/// Spawn the periodic metric-emission task: define the `pipeline` metric once, then every
/// [`METRIC_EMIT_INTERVAL`] emit the summed per-route counter deltas through `gg.metrics()`. The
/// task loops until aborted (its [`JoinHandle`] is dropped/aborted on shutdown). Best-effort: a
/// down bus logs at DEBUG and the task keeps running.
pub fn spawn_metric_emitter(
    metrics: Arc<dyn MetricService>,
    config: Arc<Config>,
    stats: Vec<Arc<RouteStats>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut builder = MetricBuilder::create(PIPELINE_METRIC).with_config(&config);
        for measure in PIPELINE_MEASURES {
            builder = builder.add_measure(measure, "Count", 60);
        }
        metrics.define_metric(builder.build());

        let mut prev = Totals::default();
        let mut tick = tokio::time::interval(METRIC_EMIT_INTERVAL);
        tick.tick().await; // consume the immediate tick — first emission after one interval
        loop {
            tick.tick().await;
            let curr = Totals::take(&stats);
            if let Err(e) = metrics.emit_metric(PIPELINE_METRIC, curr.delta(&prev)).await {
                tracing::debug!(error = %e, "pipeline metric emit failed");
            }
            prev = curr;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn totals_sum_and_delta() {
        let a = RouteStats::new("r1");
        let b = RouteStats::new("r2");
        a.messages_in.store(10, Ordering::Relaxed);
        a.messages_out.store(7, Ordering::Relaxed);
        b.messages_in.store(5, Ordering::Relaxed);
        b.messages_dropped.store(2, Ordering::Relaxed);
        let stats = vec![a.clone(), b.clone()];

        let t1 = Totals::take(&stats);
        assert_eq!(t1.messages_in, 15);
        assert_eq!(t1.messages_out, 7);
        assert_eq!(t1.messages_dropped, 2);

        // A delta over a prior snapshot is the per-interval increment.
        a.messages_in.store(13, Ordering::Relaxed);
        let t2 = Totals::take(&stats);
        let d = t2.delta(&t1);
        assert_eq!(d["messagesIn"], 3.0);
        assert_eq!(d["messagesOut"], 0.0);
    }

    #[test]
    fn evt_cooldown_gates_repeat_channels() {
        // The cooldown gate is pure (no messaging) — allow once, then deny within the window.
        let cds: Mutex<HashMap<String, Instant>> = Mutex::new(HashMap::new());
        let emitter = EvtEmitterProbe { cooldowns: cds };
        assert!(emitter.allow("queue-overflow"));
        assert!(!emitter.allow("queue-overflow"), "second within the window is suppressed");
        assert!(emitter.allow("route-error"), "a different channel has its own cooldown");
    }

    /// A minimal stand-in exercising the same cooldown logic as [`EvtEmitter::allow`] without a
    /// messaging service (which a unit test cannot wire).
    struct EvtEmitterProbe {
        cooldowns: Mutex<HashMap<String, Instant>>,
    }
    impl EvtEmitterProbe {
        fn allow(&self, channel: &str) -> bool {
            let mut cds = self.cooldowns.lock().unwrap();
            let now = Instant::now();
            if let Some(last) = cds.get(channel) {
                if now.duration_since(*last) < EVT_COOLDOWN {
                    return false;
                }
            }
            cds.insert(channel.to_string(), now);
            true
        }
    }
}
