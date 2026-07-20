//! # Fan-out, command surface, and edge-console panels
//!
//! Everything in this module is testable **without a live `EdgeCommons`**: it operates over trait
//! objects a downstream test can fake ([`edgecommons::messaging::MessagingService`], the crate's own
//! [`crate::observe::EvtEmitter`]) or is pure. It is split out of [`crate::app`] specifically so the
//! coverage gate can hold this logic to the 90% floor while [`crate::app`]'s composition root
//! (`ProcessorApp::start`/`build_route`/`run`) — which must obtain a live `Config`/`CommandInbox`/
//! `EventsFacade` from a real `EdgeCommons`, none of which has a public test constructor — is
//! excluded as the genuine untestable seam (see `AGENTS.md` and `.github/workflows/ci.yml`).
//!
//! - [`self_subscribe`] wires one subscribe filter's fan-out handler: the UNS self-echo guard, then
//!   per-route delivery honoring `paused` and tallying `messages_in`/`messages_dropped`/`queue_depth`,
//!   with a rate-limited `evt/warning/queue-overflow` on backpressure.
//! - [`register_commands`] wires the custom command verbs (`get-stats`/`flush`/`pause`/`resume`) and
//!   the two edge-console panel descriptors onto the library's command inbox.
//! - [`validate_script_output_topic`] rejects a script stage's `output.topic` that targets a reserved
//!   UNS class, feeds back into the route's own subscribe filters, or collides with a route-level
//!   `publish.topic`.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use edgecommons::messaging::message::Message;
use edgecommons::messaging::{topic_matches, MessagingService};
use edgecommons::prelude::*;
use edgecommons::uns::reserved_class_of;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};

use crate::observe::{EvtEmitter, RouteStats};
use crate::proc::{now_ms, Control, ProcMsg};

/// The routes registered on one subscribe filter: each a `(worker data sender, route counters)`
/// pair the fan-out handler forwards to.
pub(crate) type FilterRoutes = Vec<(mpsc::Sender<ProcMsg>, Arc<RouteStats>)>;

/// A command-facing route handle. Deliberately holds **no** data sender, so [`crate::app::ProcessorApp`]
/// remains the sole owner of the data channels — dropping its senders on shutdown then closes every
/// worker (the control sender kept here does not gate worker shutdown).
pub(crate) struct RouteHandle {
    pub id: String,
    pub control: mpsc::Sender<Control>,
    pub stats: Arc<RouteStats>,
}

/// Subscribe one filter with a fan-out handler: apply the UNS self-echo guard, then forward each
/// message to every route channel that registered for it (tallying `messages_in`/`messages_dropped`
/// and the queue-depth gauge, honoring the per-route `paused` flag, and emitting a rate-limited
/// `evt/warning/queue-overflow` on backpressure drops). Concurrency is 1 so ordered consumers get
/// messages in order.
pub(crate) async fn self_subscribe(
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
            256,
            1,
        )
        .await?;
    Ok(())
}

/// Register the processor's custom command verbs on the library command inbox (a no-op when no
/// messaging transport wired an inbox). The built-in `ping`/`reload-config`/`get-configuration`
/// verbs are registered by the library and complement these.
pub(crate) fn register_commands(cmds: &Arc<CommandInbox>, handles: Arc<Vec<RouteHandle>>) {
    // get-stats — per-route counters snapshot.
    {
        let handles = handles.clone();
        try_register(cmds, "get-stats", command_handler(move |_req| {
            let handles = handles.clone();
            async move { Ok(Some(stats_json(&handles))) }
        }));
    }

    // flush — force-close every route's open time windows now; report the total emitted.
    {
        let handles = handles.clone();
        try_register(cmds, "flush", command_handler(move |_req| {
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
        try_register(cmds, "pause", command_handler(move |req| {
            let handles = handles.clone();
            async move { Ok(Some(set_paused(&handles, &req, true))) }
        }));
    }

    // resume — the inverse of pause.
    {
        let handles = handles.clone();
        try_register(cmds, "resume", command_handler(move |req| {
            let handles = handles.clone();
            async move { Ok(Some(set_paused(&handles, &req, false))) }
        }));
    }

    // The edge-console panel pair (optional enhancement, not a baseline requirement for
    // processors — see DESIGN.md). Component-scoped: the processor has no console-facing UNS
    // instance dimension (routes are internal wiring, addressed by an optional body field, not a
    // topic segment), unlike a southbound adapter's per-device instances.
    for panel in panels() {
        if let Err(e) = cmds.register_panel(panel) {
            tracing::warn!(error = %e, "failed to register edge-console panel");
        }
    }
}

/// The two edge-console panel descriptors: `overview` (fleet-wide totals + flush/pause/resume) and
/// `routes` (per-route counters via `get-stats`). Both `scope: "component"` (see [`register_commands`]).
pub(crate) fn panels() -> Vec<Value> {
    vec![
        json!({
            "id": "overview", "title": "Overview", "order": 10, "scope": "component",
            "widgets": [
                { "kind": "commandSummary", "actions": ["flush", "pause", "resume"] }
            ],
            "verbs": ["get-stats", "flush", "pause", "resume"]
        }),
        json!({
            "id": "routes", "title": "Routes", "order": 20, "scope": "component",
            "widgets": [ { "kind": "table", "source": "get-stats", "path": "routes" } ],
            "verbs": ["get-stats"]
        }),
    ]
}

/// Register a verb, logging (not failing) if the inbox rejects it.
fn try_register(cmds: &Arc<CommandInbox>, verb: &str, handler: Arc<dyn CommandHandler>) {
    if let Err(e) = cmds.register(verb, handler) {
        tracing::warn!(verb, error = %e, "failed to register command verb");
    }
}

/// The `get-stats` reply body: `{routes: [{id, in, out, dropped, streamAppends, publishFailures,
/// queueDepth, paused}]}`.
pub(crate) fn stats_json(handles: &[RouteHandle]) -> Value {
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
pub(crate) fn set_paused(handles: &[RouteHandle], request: &Message, paused: bool) -> Value {
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

/// Validate a script stage's resolved `output.topic` for one route: reject reserved UNS classes,
/// reject an output that any of the route's own subscribe filters would re-consume (a direct
/// feedback loop — cross-route re-consumption is stopped at runtime by the self-echo guard), and
/// reject a route-level `publish.topic` alongside it (the dispatcher's per-route topic would
/// silently override the per-stage output topic).
pub(crate) fn validate_script_output_topic(
    route_id: &str,
    topic: &str,
    include_root: bool,
    filters: &[String],
    route_publish_topic: Option<&str>,
) -> anyhow::Result<()> {
    if let Some(cls) = reserved_class_of(topic, include_root) {
        anyhow::bail!(
            "route '{route_id}': script output.topic '{topic}' targets the RESERVED UNS class \
             '{}' — target a data/evt/app class instead",
            cls.token()
        );
    }
    for f in filters {
        anyhow::ensure!(
            !topic_matches(f, topic),
            "route '{route_id}': script output.topic '{topic}' overlaps this route's subscribe \
             filter '{f}' — a feedback loop; publish the derived signal outside the route's input \
             filters"
        );
    }
    anyhow::ensure!(
        route_publish_topic.is_none(),
        "route '{route_id}': `publish.topic` and a script stage `output.topic` are mutually \
         exclusive — the route-level topic would override the stage output; drop one of the two"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::FakeMessaging;
    use edgecommons::messaging::message::{HierEntry, MessageBuilder, MessageIdentity};

    #[test]
    fn the_two_panels_are_registered_with_the_right_ids_orders_scope_and_verbs() {
        let ps = panels();
        let ids: Vec<&str> = ps.iter().map(|p| p["id"].as_str().unwrap()).collect();
        assert_eq!(ids, vec!["overview", "routes"]);
        let orders: Vec<u64> = ps.iter().map(|p| p["order"].as_u64().unwrap()).collect();
        assert_eq!(orders, vec![10, 20]);
        for p in &ps {
            assert_eq!(p["scope"], json!("component"), "the processor has no console instance dimension");
            assert!(!p["title"].as_str().unwrap().is_empty());
        }
        assert_eq!(ps[0]["verbs"], json!(["get-stats", "flush", "pause", "resume"]));
        assert_eq!(ps[1]["verbs"], json!(["get-stats"]));
    }

    #[test]
    fn script_output_topic_validation() {
        let filters = vec!["ecv1/+/+/+/data/#".to_string()];
        // A data-class topic under the subscribed fleet filter is a feedback loop.
        let err = validate_script_output_topic(
            "r1",
            "ecv1/gw-1/telemetry-processor/r1/data/derived",
            true,
            &filters,
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("feedback loop"), "{err}");

        // A reserved class (`metric`) is rejected outright.
        let err = validate_script_output_topic(
            "r1",
            "ecv1/gw-1/telemetry-processor/r1/metric/derived",
            true,
            &[],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("RESERVED"), "{err}");

        // A route publish.topic alongside a stage output topic is ambiguous.
        let err = validate_script_output_topic(
            "r1",
            "ecv1/gw-1/telemetry-processor/r1/data/derived",
            true,
            &[],
            Some("ecv1/gw-1/telemetry-processor/r1/data/other"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("mutually"), "{err}");

        // A non-overlapping data topic with no publish.topic passes.
        validate_script_output_topic(
            "r1",
            "ecv1/gw-1/telemetry-processor/r1/data/derived",
            true,
            &["ecv1/+/opcua-adapter/+/data/#".to_string()],
            None,
        )
        .unwrap();
    }

    // ---- stats_json / set_paused (pure) -----------------------------------------------------------

    fn a_handle(id: &str) -> RouteHandle {
        let (control, _rx) = mpsc::channel(1);
        RouteHandle { id: id.to_string(), control, stats: RouteStats::new(id) }
    }

    #[test]
    fn stats_json_reports_every_route_counter() {
        let h = a_handle("r1");
        h.stats.messages_in.store(10, Ordering::Relaxed);
        h.stats.messages_out.store(7, Ordering::Relaxed);
        h.stats.messages_dropped.store(1, Ordering::Relaxed);
        h.stats.stream_appends.store(3, Ordering::Relaxed);
        h.stats.publish_failures.store(2, Ordering::Relaxed);
        h.stats.queue_depth.store(5, Ordering::Relaxed);
        h.stats.paused.store(true, Ordering::Relaxed);

        let out = stats_json(&[h]);
        let routes = out["routes"].as_array().unwrap();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0]["id"], json!("r1"));
        assert_eq!(routes[0]["in"], json!(10));
        assert_eq!(routes[0]["out"], json!(7));
        assert_eq!(routes[0]["dropped"], json!(1));
        assert_eq!(routes[0]["streamAppends"], json!(3));
        assert_eq!(routes[0]["publishFailures"], json!(2));
        assert_eq!(routes[0]["queueDepth"], json!(5));
        assert_eq!(routes[0]["paused"], json!(true));
    }

    fn a_request(body: Value) -> Message {
        MessageBuilder::new("cmd", "1.0").command(body).build()
    }

    #[test]
    fn set_paused_targets_one_route_when_named_and_all_when_omitted() {
        let handles = vec![a_handle("r1"), a_handle("r2")];

        let out = set_paused(&handles, &a_request(json!({ "route": "r1" })), true);
        assert_eq!(out["paused"], json!(["r1"]));
        assert!(handles[0].stats.is_paused());
        assert!(!handles[1].stats.is_paused());

        let out = set_paused(&handles, &a_request(json!({})), true);
        assert_eq!(out["paused"].as_array().unwrap().len(), 2, "no route -> every route");
        assert!(handles[1].stats.is_paused());

        let out = set_paused(&handles, &a_request(json!({})), false);
        assert_eq!(out["resumed"].as_array().unwrap().len(), 2);
        assert!(!handles[0].stats.is_paused());
        assert!(!handles[1].stats.is_paused());
    }

    // ---- self_subscribe (the fan-out handler) -----------------------------------------------------

    fn southbound(signal_id: &str) -> Message {
        MessageBuilder::new("SouthboundSignalUpdate", "1.0")
            .southbound_signal_update(json!({
                "signal": { "id": signal_id, "name": signal_id },
                "samples": [{ "value": 1.0, "quality": "GOOD" }]
            }))
            .build()
    }

    #[tokio::test]
    async fn fans_a_message_out_to_every_route_registered_on_the_filter() {
        let fake = FakeMessaging::new();
        let messaging: Arc<dyn MessagingService> = fake.clone();
        let (_rec, evt) = EvtEmitter::recording();
        let (tx1, mut rx1) = mpsc::channel(4);
        let (tx2, mut rx2) = mpsc::channel(4);
        let s1 = RouteStats::new("r1");
        let s2 = RouteStats::new("r2");

        self_subscribe(
            &messaging,
            "ecv1/+/+/+/data/#",
            vec![(tx1, s1.clone()), (tx2, s2.clone())],
            "my-device".into(),
            "telemetry-processor".into(),
            evt,
        )
        .await
        .unwrap();
        assert!(fake.is_subscribed("ecv1/+/+/+/data/#"));

        fake.deliver("ecv1/+/+/+/data/#", "ecv1/other/opcua-adapter/kep1/data/x", southbound("x")).await;

        assert_eq!(rx1.try_recv().unwrap().topic, "ecv1/other/opcua-adapter/kep1/data/x");
        assert_eq!(rx2.try_recv().unwrap().topic, "ecv1/other/opcua-adapter/kep1/data/x");
        assert_eq!(s1.messages_in.load(Ordering::Relaxed), 1);
        assert_eq!(s2.messages_in.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn drops_a_re_consumed_message_carrying_its_own_identity() {
        let fake = FakeMessaging::new();
        let messaging: Arc<dyn MessagingService> = fake.clone();
        let (_rec, evt) = EvtEmitter::recording();
        let (tx, mut rx) = mpsc::channel(4);
        let stats = RouteStats::new("r1");

        self_subscribe(
            &messaging,
            "ecv1/+/+/+/data/#",
            vec![(tx, stats.clone())],
            "my-device".into(),
            "telemetry-processor".into(),
            evt,
        )
        .await
        .unwrap();

        let mut own_echo = southbound("downsampled");
        own_echo.identity = Some(
            MessageIdentity::new(
                vec![HierEntry { level: "device".into(), value: "my-device".into() }],
                "telemetry-processor",
                Some("r1".into()),
            )
            .unwrap(),
        );
        fake.deliver("ecv1/+/+/+/data/#", "ecv1/my-device/telemetry-processor/r1/data/x", own_echo).await;

        assert!(rx.try_recv().is_err(), "the self-echo must be dropped, not forwarded");
        assert_eq!(stats.messages_in.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn a_full_queue_drops_and_counts_and_emits_once() {
        let fake = FakeMessaging::new();
        let messaging: Arc<dyn MessagingService> = fake.clone();
        let (rec, evt) = EvtEmitter::recording();
        let (tx, _rx) = mpsc::channel(1); // capacity 1 — the second delivery finds it full
        let stats = RouteStats::new("r1");

        self_subscribe(
            &messaging,
            "ecv1/+/+/+/data/#",
            vec![(tx, stats.clone())],
            "my-device".into(),
            "telemetry-processor".into(),
            evt,
        )
        .await
        .unwrap();

        // Fill the one-slot queue, then overflow it.
        fake.deliver("ecv1/+/+/+/data/#", "t1", southbound("a")).await;
        fake.deliver("ecv1/+/+/+/data/#", "t2", southbound("b")).await;
        assert_eq!(stats.messages_in.load(Ordering::Relaxed), 1);
        assert_eq!(stats.messages_dropped.load(Ordering::Relaxed), 1);
        assert_eq!(rec.lock().unwrap().len(), 1, "queue-overflow evt emitted once");
    }

    #[tokio::test]
    async fn a_paused_route_is_skipped_entirely() {
        let fake = FakeMessaging::new();
        let messaging: Arc<dyn MessagingService> = fake.clone();
        let (rec, evt) = EvtEmitter::recording();
        let (tx, _rx) = mpsc::channel(4);
        let paused_stats = RouteStats::new("r2");
        paused_stats.paused.store(true, Ordering::Relaxed);

        self_subscribe(
            &messaging,
            "ecv1/+/opcua-adapter/+/data/#",
            vec![(tx, paused_stats.clone())],
            "my-device".into(),
            "telemetry-processor".into(),
            evt,
        )
        .await
        .unwrap();

        fake.deliver("ecv1/+/opcua-adapter/+/data/#", "t3", southbound("c")).await;
        assert_eq!(paused_stats.messages_in.load(Ordering::Relaxed), 0);
        assert_eq!(paused_stats.messages_dropped.load(Ordering::Relaxed), 0);
        assert!(rec.lock().unwrap().is_empty(), "a skipped route never overflows");
    }
}
