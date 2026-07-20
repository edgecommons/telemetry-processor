//! # Route-build decision logic (testable without a live `EdgeCommons`)
//!
//! Everything here operates on [`Config`] — publicly constructible via `Config::from_value`, so a
//! unit test can build one without a live `EdgeCommons` — plus other plain or already-fakeable
//! inputs ([`Channel`], [`RouteConfig`]). It is split out of
//! [`crate::app::ProcessorApp::start`]/`build_route` specifically so these decision branches stay in
//! the coverage gate's denominator instead of being dropped alongside the genuine composition-root
//! seam (`src/app.rs`'s module doc explains what remains there and why):
//!
//! - [`resolve_global_wiring`] — the cross-route defaulting rules (`global.defaults.key`/
//!   `scriptsDir`/`target`/`scriptEngine`), the component-name split (`ComponentFullName` →
//!   `ComponentName`), and the processor's own UNS identity strings (the self-echo guard's match).
//! - [`resolve_target`] — a route's target channel, falling back to the global default.
//! - [`resolve_filters`] — a route's `subscribe` filters, template-resolved, non-empty required.
//! - [`resolve_publish_topic`] — a route's `publish.topic` template resolution, warning (not
//!   failing) when it resolves to a reserved UNS class.
//! - [`resolve_script_output_topics`] — every script stage's explicit `output.topic`: resolved and
//!   validated (reserved class / subscribe-filter feedback loop / `publish.topic` collision).
//! - [`compute_restamp`] — the `local`-target identity restamp policy.
//!
//! The one piece of route building that genuinely cannot move here is the
//! `#[cfg(feature = "streaming")] gg.streams()` call for a `Channel::Stream` target (`src/app.rs`'s
//! `build_route`) — it needs a live `&EdgeCommons`-obtained `Arc<dyn StreamService>` handle, which no
//! test can fabricate (the library exposes no public/test constructor for one).

use edgecommons::config::model::Config;
use edgecommons::config::template::resolve;
use edgecommons::facades::Channel;
use edgecommons::messaging::message::MessageIdentity;
use edgecommons::uns::reserved_class_of;

use crate::config::{parse_target, GlobalDefaults, PublishConfig, RouteConfig, ScriptEngineKind, ScriptStageSpec, StageConfig};
use crate::dispatch::validate_script_output_topic;

/// Default aggregation/partition key when neither the route nor the global defaults set one.
pub(crate) const DEFAULT_KEY: &str = "body.signal.id";

/// Cross-route defaults + the processor's own identity strings, resolved once from `Config` at
/// startup (no live `EdgeCommons` needed).
pub(crate) struct GlobalWiring {
    pub default_key: String,
    pub scripts_dir: String,
    pub default_target: Option<String>,
    pub default_script_engine: ScriptEngineKind,
    /// `{ComponentName}` — the short name (segment after the last `.`).
    pub component_name: String,
    /// `{ComponentFullName}` — the fully-qualified component name.
    pub component_full_name: String,
    pub thing_name: String,
    /// The processor's own UNS device token — the self-echo guard's match.
    pub own_device: String,
    /// The processor's own UNS component token — the self-echo guard's match.
    pub own_component: String,
}

/// Resolve [`GlobalWiring`] from `component.global.defaults` + the config's own identity/name.
pub(crate) fn resolve_global_wiring(config: &Config) -> GlobalWiring {
    let defaults: GlobalDefaults = config
        .global()
        .get("defaults")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();
    let default_key = defaults.key.clone().unwrap_or_else(|| DEFAULT_KEY.to_string());
    // Script files (`{"file": "..."}`) resolve relative to this dir (template-substituted).
    let scripts_dir =
        defaults.scripts_dir.as_deref().map(|d| resolve(config, d)).unwrap_or_else(|| ".".to_string());
    // Component identity for the script runtime context. Read the raw values (the template
    // resolver would sanitize them for topic/path safety, which we don't want in a script).
    let component_full_name = config.component_name.clone();
    let component_name =
        component_full_name.rsplit('.').next().unwrap_or(&component_full_name).to_string();
    let thing_name = config.thing_name.clone();
    // The processor's own UNS identity — the self-echo guard's match, and the restamp source.
    let own_device = config.identity().device().to_string();
    let own_component = config.identity().component().to_string();
    GlobalWiring {
        default_key,
        scripts_dir,
        default_target: defaults.target.clone(),
        default_script_engine: defaults.script_engine.unwrap_or_default(),
        component_name,
        component_full_name,
        thing_name,
        own_device,
        own_component,
    }
}

/// Resolve a route's target channel: the route's own `target`, falling back to
/// `global.defaults.target`. `Err` when neither is set, or the string doesn't parse.
pub(crate) fn resolve_target(route: &RouteConfig, default_target: Option<&str>) -> anyhow::Result<Channel> {
    let target_str = route
        .target
        .clone()
        .or_else(|| default_target.map(String::from))
        .ok_or_else(|| anyhow::anyhow!("route '{}' has no target", route.id))?;
    parse_target(&target_str)
}

/// Resolve a route's `subscribe` filters (template-substituted). `Err` when the list is empty.
pub(crate) fn resolve_filters(config: &Config, route: &RouteConfig) -> anyhow::Result<Vec<String>> {
    anyhow::ensure!(!route.subscribe.is_empty(), "route '{}' has no subscribe topics", route.id);
    Ok(route.subscribe.iter().map(|f| resolve(config, f)).collect())
}

/// Resolve a route's `publish.topic` template. Defensive-only: a topic that resolves to a reserved
/// UNS class (`state`/`metric`/`cfg`/`log`) is rejected at publish time by the reserved-class guard
/// (silent drop) — this only warns at startup so the drop isn't a silent surprise later.
pub(crate) fn resolve_publish_topic(
    config: &Config,
    route_id: &str,
    mut publish: PublishConfig,
) -> PublishConfig {
    if let Some(t) = &publish.topic {
        let resolved = resolve(config, t);
        if let Some(cls) = reserved_class_of(&resolved, config.effective_include_root()) {
            tracing::warn!(
                route = %route_id,
                topic = %resolved,
                class = cls.token(),
                "publish.topic targets a RESERVED UNS class; the reserved-class guard will drop \
                 these publishes — target a data/evt/app class instead"
            );
        }
        publish.topic = Some(resolved);
    }
    publish
}

/// Resolve + validate every script stage's explicit `output.topic` in `route.pipeline` (mutates it
/// in place): reserved classes and subscribe-overlap feedback loops are startup errors, and a
/// route-level `publish.topic` may not silently override a stage output topic.
pub(crate) fn resolve_script_output_topics(
    config: &Config,
    route: &mut RouteConfig,
    filters: &[String],
    publish_topic: Option<&str>,
) -> anyhow::Result<()> {
    for sc in route.pipeline.iter_mut() {
        let StageConfig::Script(ScriptStageSpec::Spec(sp)) = sc else { continue };
        let Some(out) = sp.output.as_mut() else { continue };
        let resolved = resolve(config, &out.topic);
        validate_script_output_topic(
            &route.id,
            &resolved,
            config.effective_include_root(),
            filters,
            publish_topic,
        )?;
        out.topic = resolved;
    }
    Ok(())
}

/// The `local`-target identity restamp policy: `Some(own identity, instance = route id)` for
/// `Channel::Local` (loop-safety for the self-echo guard + correct provenance for the processor's
/// product); `None` for every other target (they preserve the source identity for provenance).
pub(crate) fn compute_restamp(
    config: &Config,
    target: &Channel,
    route_id: &str,
) -> anyhow::Result<Option<MessageIdentity>> {
    match target {
        Channel::Local => Ok(Some(
            config
                .identity()
                .with_instance(route_id)
                .map_err(|e| anyhow::anyhow!("route '{route_id}': identity restamp: {e}"))?,
        )),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn config(raw: serde_json::Value) -> Config {
        Config::from_value("com.mbreissi.edgecommons.TelemetryProcessor", "gw-01", raw).unwrap()
    }

    fn route(v: serde_json::Value) -> RouteConfig {
        serde_json::from_value(v).unwrap()
    }

    // ---- resolve_global_wiring ----------------------------------------------------------------

    #[test]
    fn global_wiring_defaults_when_component_global_is_absent() {
        let cfg = config(json!({}));
        let w = resolve_global_wiring(&cfg);
        assert_eq!(w.default_key, DEFAULT_KEY);
        assert_eq!(w.scripts_dir, ".");
        assert_eq!(w.default_target, None);
        assert_eq!(w.default_script_engine, ScriptEngineKind::Rhai);
        assert_eq!(w.component_full_name, "com.mbreissi.edgecommons.TelemetryProcessor");
        assert_eq!(w.component_name, "TelemetryProcessor", "the short name after the last '.'");
        assert_eq!(w.thing_name, "gw-01");
        assert_eq!(w.own_device, "gw-01", "zero-config identity defaults device to the thing name");
    }

    #[test]
    fn global_wiring_reads_component_global_defaults() {
        let cfg = config(json!({
            "component": { "global": { "defaults": {
                "key": "body.custom.id",
                "target": "northbound",
                "scriptEngine": "lua"
            } } }
        }));
        let w = resolve_global_wiring(&cfg);
        assert_eq!(w.default_key, "body.custom.id");
        assert_eq!(w.default_target.as_deref(), Some("northbound"));
        assert_eq!(w.default_script_engine, ScriptEngineKind::Lua);
    }

    #[test]
    fn global_wiring_resolves_a_templated_scripts_dir() {
        let cfg = config(json!({
            "tags": { "site": "dallas" },
            "component": { "global": { "defaults": { "scriptsDir": "scripts/{site}" } } }
        }));
        let w = resolve_global_wiring(&cfg);
        assert_eq!(w.scripts_dir, "scripts/dallas");
    }

    // ---- resolve_target ------------------------------------------------------------------------

    #[test]
    fn resolve_target_uses_the_route_target_or_falls_back_to_the_default() {
        let r = route(json!({ "id": "r1", "subscribe": ["a"], "target": "local" }));
        assert_eq!(resolve_target(&r, None).unwrap(), Channel::Local);

        let r = route(json!({ "id": "r1", "subscribe": ["a"] }));
        assert_eq!(resolve_target(&r, Some("northbound")).unwrap(), Channel::Northbound);

        let r = route(json!({ "id": "r1", "subscribe": ["a"] }));
        let err = resolve_target(&r, None).unwrap_err();
        assert!(err.to_string().contains("no target"));
    }

    // ---- resolve_filters -----------------------------------------------------------------------

    #[test]
    fn resolve_filters_resolves_templates_and_rejects_an_empty_list() {
        let cfg = config(json!({}));
        let r = route(json!({ "id": "r1", "subscribe": ["ecv1/+/+/+/data/#"] }));
        assert_eq!(resolve_filters(&cfg, &r).unwrap(), vec!["ecv1/+/+/+/data/#".to_string()]);

        let r = route(json!({ "id": "r1", "subscribe": [] }));
        let err = resolve_filters(&cfg, &r).unwrap_err();
        assert!(err.to_string().contains("no subscribe topics"));
    }

    // ---- resolve_publish_topic -------------------------------------------------------------------

    #[test]
    fn resolve_publish_topic_resolves_templates_and_passes_through_a_reserved_class() {
        let cfg = config(json!({ "tags": { "site": "dallas" } }));
        let pub_cfg = PublishConfig {
            topic: Some("ecv1/{ThingName}/telemetry-processor/data/{site}".to_string()),
            partition_key: None,
            qos: None,
        };
        let out = resolve_publish_topic(&cfg, "r1", pub_cfg);
        assert_eq!(out.topic.as_deref(), Some("ecv1/gw-01/telemetry-processor/data/dallas"));

        // A reserved-class topic still resolves (only a startup WARN, not a failure — the
        // reserved-class guard is what actually drops the publish at runtime).
        let reserved = PublishConfig {
            topic: Some("ecv1/{ThingName}/telemetry-processor/metric/derived".to_string()),
            partition_key: None,
            qos: None,
        };
        let out = resolve_publish_topic(&cfg, "r1", reserved);
        assert_eq!(out.topic.as_deref(), Some("ecv1/gw-01/telemetry-processor/metric/derived"));
    }

    #[test]
    fn resolve_publish_topic_is_a_passthrough_with_no_topic() {
        let cfg = config(json!({}));
        let out = resolve_publish_topic(&cfg, "r1", PublishConfig::default());
        assert_eq!(out.topic, None);
    }

    // ---- resolve_script_output_topics -----------------------------------------------------------

    #[test]
    fn resolve_script_output_topics_resolves_and_validates_in_place() {
        let cfg = config(json!({}));
        let mut r = route(json!({
            "id": "r1",
            "subscribe": ["a"],
            "pipeline": [ { "script": {
                "source": "body",
                "output": { "topic": "ecv1/{ThingName}/telemetry-processor/r1/data/derived" }
            } } ]
        }));
        resolve_script_output_topics(&cfg, &mut r, &[], None).unwrap();
        let StageConfig::Script(ScriptStageSpec::Spec(sp)) = &r.pipeline[0] else { panic!() };
        assert_eq!(
            sp.output.as_ref().unwrap().topic,
            "ecv1/gw-01/telemetry-processor/r1/data/derived",
            "the template must be resolved in place"
        );
    }

    #[test]
    fn resolve_script_output_topics_rejects_a_feedback_loop() {
        let cfg = config(json!({}));
        let mut r = route(json!({
            "id": "r1",
            "subscribe": ["ecv1/+/+/+/data/#"],
            "pipeline": [ { "script": {
                "source": "body",
                "output": { "topic": "ecv1/gw-01/telemetry-processor/r1/data/derived" }
            } } ]
        }));
        let filters = vec!["ecv1/+/+/+/data/#".to_string()];
        let err = resolve_script_output_topics(&cfg, &mut r, &filters, None).unwrap_err();
        assert!(err.to_string().contains("feedback loop"), "{err}");
    }

    #[test]
    fn resolve_script_output_topics_is_a_noop_without_a_multi_signal_output() {
        let cfg = config(json!({}));
        let mut r = route(json!({
            "id": "r1",
            "subscribe": ["a"],
            "pipeline": [ { "script": "body" } ]
        }));
        resolve_script_output_topics(&cfg, &mut r, &[], None).unwrap();
    }

    // ---- compute_restamp -----------------------------------------------------------------------

    #[test]
    fn compute_restamp_restamps_only_the_local_target() {
        let cfg = config(json!({}));
        let restamp = compute_restamp(&cfg, &Channel::Local, "r1").unwrap();
        let identity = restamp.expect("local target restamps");
        assert_eq!(identity.instance(), Some("r1"));
        assert_eq!(identity.device(), "gw-01");

        assert!(compute_restamp(&cfg, &Channel::Northbound, "r1").unwrap().is_none());
        assert!(compute_restamp(&cfg, &Channel::Stream("archive".into()), "r1").unwrap().is_none());
    }
}
