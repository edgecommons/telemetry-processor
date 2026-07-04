//! # Route worker + target dispatch
//!
//! Each route runs one async worker task that owns its [`Pipeline`], a bounded `mpsc` of incoming
//! messages (filled by the subscribe handler), and a side **control** channel (the `flush` command
//! verb). A `tokio::select!` drains the data channel, fires the aggregate flush timer, and services
//! control, so per-key window state is single-threaded and lock-free. Pipeline output is dispatched
//! to the route's [`Channel`] target, tallied into the route's [`RouteStats`], and — on failure —
//! surfaced as an [`EvtEmitter`] `evt` (via the library's `events()` facade).
//!
//! ## Why this dispatcher does not route through `data()`
//! `ggcommons::facades::DataFacade` (the `data()` facade) is a great fit for a southbound adapter
//! that mints a **fresh** `SouthboundSignalUpdate` from a protocol read (DESIGN-class-facades §2.1).
//! It is a poor fit for *this* dispatcher, which republishes an **already-built, possibly
//! non-southbound-shaped** [`Message`] (the processor is deliberately payload-agnostic — see
//! `docs/reference/messaging-interface.md`): `data()` always (a) mints the topic from *its own bound
//! instance* identity, never an arbitrary caller topic (breaking the documented `local`/`northbound`
//! "default = the source topic" bridge — see the sample `alarms-northbound` route, which forwards
//! northbound with no `publish.topic` override, and `docs/explanation.md`'s two-flow diagram),
//! (b) forces the `SouthboundSignalUpdate` header + a `signal.id`/`samples[]` body shape, which would
//! break `ProcessedTelemetry` (aggregate output) and any `project`/`script`-reshaped body, and (c)
//! always stamps *its own* identity — correct for `local` (see below) but wrong for `northbound`/
//! `stream`, which deliberately preserve the **source** identity for provenance. So the dispatcher
//! keeps this lower-level `messaging()`/`streams()` path, exactly as `DESIGN-class-facades.md` §7.2
//! anticipated ("the facade must not fight the restamp; likely the processor keeps a lower-level
//! path here"). What *did* migrate onto a facade is `evt` health events (see
//! [`crate::observe::EvtEmitter`], now a thin wrapper over `gg.events()`) and the routing
//! **vocabulary**: the route `target` is the library's own [`Channel`] (`local` | `northbound` |
//! `stream:<name>`, DESIGN-class-facades §4) instead of a bespoke processor enum.
//!
//! ## UNS provenance (identity restamping)
//! For a `local` republish the dispatcher **restamps the output envelope's `identity` with the
//! processor's own identity** (the `restamp` field): the local output IS the processor's product,
//! and — because the processor consumes the `data` class it republishes onto — restamping is what
//! makes the [`crate::app`] self-echo guard effective (a re-consumed copy now carries *our* identity
//! and is dropped). `northbound`/`stream` targets leave the source identity intact (they never
//! re-enter the local bus, so provenance is preserved for the cloud/archive).

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use ggcommons::facades::Channel;
use ggcommons::messaging::message::{Message, MessageIdentity};
use ggcommons::messaging::{MessagingService, Qos};
use smallvec::SmallVec;
use tokio::sync::mpsc;

use crate::config::PublishConfig;
#[cfg(feature = "streaming")]
use crate::json_path::resolve_first_string;
use crate::observe::{EvtEmitter, RouteStats};
use crate::proc::{now_ms, Control, Out, Pipeline, ProcMsg};

/// Forwards processed messages to a route's target.
pub struct Dispatcher {
    messaging: Arc<dyn MessagingService>,
    target: Channel,
    /// Pre-resolved output topic (templates expanded at startup); `None` → reuse the source topic.
    topic: Option<String>,
    qos: Qos,
    /// Partition-key path for `stream:` targets.
    partition_key_path: String,
    /// The owning route id (for stats/evt context).
    route_id: String,
    /// Per-route counters (shared with the fan-out handler + the metric emitter).
    stats: Arc<RouteStats>,
    /// The processor's `evt` health-event publisher.
    evt: Arc<EvtEmitter>,
    /// When `Some`, restamp the `local` output envelope's identity with this (the processor's own
    /// identity, instance = route id) — see the module docs. Always `None` for non-`local` targets.
    restamp: Option<MessageIdentity>,
    #[cfg(feature = "streaming")]
    stream: Option<ggcommons::streaming::StreamHandle>,
}

impl Dispatcher {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        messaging: Arc<dyn MessagingService>,
        target: Channel,
        publish: &PublishConfig,
        route_key: &str,
        route_id: String,
        stats: Arc<RouteStats>,
        evt: Arc<EvtEmitter>,
        restamp: Option<MessageIdentity>,
        #[cfg(feature = "streaming")] stream: Option<ggcommons::streaming::StreamHandle>,
    ) -> Self {
        let qos = match publish.qos.as_deref() {
            Some(q) if q.eq_ignore_ascii_case("atMostOnce") => Qos::AtMostOnce,
            _ => Qos::AtLeastOnce,
        };
        let partition_key_path =
            publish.partition_key.clone().unwrap_or_else(|| route_key.to_string());
        Self {
            messaging,
            target,
            topic: publish.topic.clone(),
            qos,
            partition_key_path,
            route_id,
            stats,
            evt,
            restamp,
            #[cfg(feature = "streaming")]
            stream,
        }
    }

    fn out_topic(&self, m: &ProcMsg) -> String {
        self.topic.clone().unwrap_or_else(|| m.topic.clone())
    }

    /// Forward one processed message to the target. Errors are tallied + surfaced as `evt`, not
    /// propagated.
    pub async fn dispatch(&self, m: ProcMsg) {
        match &self.target {
            Channel::Local => {
                let topic = self.out_topic(&m);
                // Restamp the local output with the processor's identity (loop-safety + provenance).
                let published = if let Some(id) = &self.restamp {
                    let mut msg = m.msg.clone();
                    msg.identity = Some(id.clone());
                    self.messaging.publish(&topic, &msg).await
                } else {
                    self.messaging.publish(&topic, &m.msg).await
                };
                match published {
                    Ok(()) => {
                        self.stats.messages_out.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        self.stats.publish_failures.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(error = %e, topic = %topic, "local publish failed");
                        self.evt.route_error(&self.route_id, &topic, &e.to_string()).await;
                    }
                }
            }
            Channel::Northbound => {
                let topic = self.out_topic(&m);
                match self.messaging.publish_to_iot_core(&topic, &m.msg, self.qos).await {
                    Ok(()) => {
                        self.stats.messages_out.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        self.stats.publish_failures.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(error = %e, topic = %topic, "northbound publish failed");
                        self.evt.route_error(&self.route_id, &topic, &e.to_string()).await;
                    }
                }
            }
            Channel::Stream(name) => self.dispatch_stream(name, &m.msg, m.recv_ms).await,
        }
    }

    #[cfg(feature = "streaming")]
    async fn dispatch_stream(&self, name: &str, msg: &Message, recv_ms: u64) {
        let Some(handle) = &self.stream else {
            tracing::warn!(stream = %name, "stream target not available; dropping");
            self.stats.publish_failures.fetch_add(1, Ordering::Relaxed);
            self.evt.stream_unavailable(&self.route_id, name, "stream target not configured").await;
            return;
        };
        let pk = resolve_first_string(msg, &self.partition_key_path).unwrap_or_default();
        let payload = match msg.to_vec() {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "failed to serialize message for stream");
                return;
            }
        };
        match handle.append(ggcommons::streaming::StreamRecord::new(pk, recv_ms, payload)) {
            Ok(()) => {
                self.stats.stream_appends.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                self.stats.publish_failures.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(error = %e, stream = %name, "stream append failed");
                self.evt.stream_unavailable(&self.route_id, name, &e.to_string()).await;
            }
        }
    }

    #[cfg(not(feature = "streaming"))]
    async fn dispatch_stream(&self, name: &str, _msg: &Message, _recv_ms: u64) {
        let _ = &self.partition_key_path;
        self.stats.publish_failures.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(stream = %name, "build without the `streaming` feature; stream target dropped");
        self.evt.stream_unavailable(&self.route_id, name, "built without the `streaming` feature").await;
    }
}

/// Run a route's worker until the data channel closes (all subscribe handlers dropped → shutdown).
/// Alongside the data channel it fires the aggregate flush timer and services the out-of-band
/// [`Control`] channel (the `flush` command verb). On shutdown it does a final flush so in-flight
/// aggregate windows are emitted.
pub async fn run_worker(
    mut pipeline: Pipeline,
    mut rx: mpsc::Receiver<ProcMsg>,
    mut control_rx: mpsc::Receiver<Control>,
    dispatcher: Dispatcher,
) {
    // A flush timer is only meaningful when a stage is time-driven; otherwise tick rarely (ticks
    // are no-ops with no aggregate stage).
    let tick_ms = pipeline.min_tick_ms().unwrap_or(3_600_000).max(50);
    let mut interval = tokio::time::interval(Duration::from_millis(tick_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            maybe = rx.recv() => match maybe {
                Some(m) => {
                    let mut input: Out = SmallVec::new();
                    input.push(m);
                    let out = pipeline.run(input, None);
                    for pm in out {
                        dispatcher.dispatch(pm).await;
                    }
                }
                None => break,
            },
            _ = interval.tick() => {
                let out = pipeline.run(SmallVec::new(), Some(now_ms()));
                for pm in out {
                    dispatcher.dispatch(pm).await;
                }
            },
            Some(ctrl) = control_rx.recv() => match ctrl {
                Control::Flush(reply) => {
                    // Force-close open TIME windows now: a max-time tick makes every time window
                    // due (`window_end <= u64::MAX`). Count windows keep their count semantics.
                    let out = pipeline.run(SmallVec::new(), Some(u64::MAX));
                    let n = out.len() as u64;
                    for pm in out {
                        dispatcher.dispatch(pm).await;
                    }
                    let _ = reply.send(n);
                }
            },
        }
    }

    // Final flush on shutdown.
    let out = pipeline.run(SmallVec::new(), Some(now_ms()));
    for pm in out {
        dispatcher.dispatch(pm).await;
    }
}
