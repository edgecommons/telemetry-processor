//! # Route worker + target dispatch
//!
//! Each route runs one async worker task that owns its [`Pipeline`] and a bounded `mpsc` of
//! incoming messages (filled by the subscribe handler). A `tokio::select!` drains the channel and
//! fires the aggregate flush timer, so per-key window state is single-threaded and lock-free.
//! Pipeline output is dispatched to the route's [`Target`].

use std::sync::Arc;
use std::time::Duration;

use ggcommons::messaging::message::Message;
use ggcommons::messaging::{MessagingService, Qos};
use smallvec::SmallVec;
use tokio::sync::mpsc;

use crate::config::{PublishConfig, Target};
use crate::json_path::resolve_first_string;
use crate::proc::{now_ms, Out, Pipeline, ProcMsg};

/// Forwards processed messages to a route's target.
pub struct Dispatcher {
    messaging: Arc<dyn MessagingService>,
    target: Target,
    /// Pre-resolved output topic (templates expanded at startup); `None` → reuse the source topic.
    topic: Option<String>,
    qos: Qos,
    /// Partition-key path for `stream:` targets.
    partition_key_path: String,
    #[cfg(feature = "streaming")]
    stream: Option<ggcommons::streaming::StreamHandle>,
}

impl Dispatcher {
    pub fn new(
        messaging: Arc<dyn MessagingService>,
        target: Target,
        publish: &PublishConfig,
        route_key: &str,
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
            #[cfg(feature = "streaming")]
            stream,
        }
    }

    fn out_topic(&self, m: &ProcMsg) -> String {
        self.topic.clone().unwrap_or_else(|| m.topic.clone())
    }

    /// Forward one processed message to the target. Errors are logged, not propagated.
    pub async fn dispatch(&self, m: ProcMsg) {
        match &self.target {
            Target::Local => {
                let topic = self.out_topic(&m);
                if let Err(e) = self.messaging.publish(&topic, &m.msg).await {
                    tracing::warn!(error = %e, topic = %topic, "local publish failed");
                }
            }
            Target::Northbound => {
                let topic = self.out_topic(&m);
                if let Err(e) = self.messaging.publish_to_iot_core(&topic, &m.msg, self.qos).await {
                    tracing::warn!(error = %e, topic = %topic, "northbound publish failed");
                }
            }
            Target::Stream(name) => self.dispatch_stream(name, &m.msg, m.recv_ms),
        }
    }

    #[cfg(feature = "streaming")]
    fn dispatch_stream(&self, name: &str, msg: &Message, recv_ms: u64) {
        let Some(handle) = &self.stream else {
            tracing::warn!(stream = %name, "stream target not available; dropping");
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
        if let Err(e) = handle.append(ggcommons::streaming::StreamRecord::new(pk, recv_ms, payload)) {
            tracing::warn!(error = %e, stream = %name, "stream append failed");
        }
    }

    #[cfg(not(feature = "streaming"))]
    fn dispatch_stream(&self, name: &str, _msg: &Message, _recv_ms: u64) {
        let _ = &self.partition_key_path;
        tracing::warn!(stream = %name, "build without the `streaming` feature; stream target dropped");
    }
}

/// Run a route's worker until the channel closes (all subscribe handlers dropped → shutdown). On
/// shutdown it does a final flush so in-flight aggregate windows are emitted.
pub async fn run_worker(
    mut pipeline: Pipeline,
    mut rx: mpsc::Receiver<ProcMsg>,
    dispatcher: Dispatcher,
) {
    // A flush timer is only meaningful when a stage is time-driven; otherwise tick rarely (ticks
    // are no-ops with no aggregate stage).
    let tick_ms = pipeline.min_tick_ms().unwrap_or(3_600_000).max(50);
    let mut interval = tokio::time::interval(Duration::from_millis(tick_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        let out: Out = tokio::select! {
            maybe = rx.recv() => match maybe {
                Some(m) => {
                    let mut input: Out = SmallVec::new();
                    input.push(m);
                    pipeline.run(input, None)
                }
                None => break,
            },
            _ = interval.tick() => pipeline.run(SmallVec::new(), Some(now_ms())),
        };
        for pm in out {
            dispatcher.dispatch(pm).await;
        }
    }

    // Final flush on shutdown.
    let out = pipeline.run(SmallVec::new(), Some(now_ms()));
    for pm in out {
        dispatcher.dispatch(pm).await;
    }
}
