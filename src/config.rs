//! # Route configuration (the `component.instances[]` model)
//!
//! Each route is one `component.instances[]` entry: `{ id, subscribe[], pipeline[], target,
//! publish, key, maxQueue }`. Cross-route defaults come from `component.global.defaults` and are
//! overlaid per route (`global ⊕ instance`). All numeric fields accept an integer **or** an
//! integer-valued float, because Greengrass delivers config numbers as doubles.

use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};

fn lenient_opt_u64<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    match Option::<Value>::deserialize(d)? {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n
            .as_u64()
            .or_else(|| n.as_f64().map(|f| f as u64))
            .map(Some)
            .ok_or_else(|| serde::de::Error::custom("expected a non-negative integer")),
        Some(other) => Err(serde::de::Error::custom(format!("expected a number, got {other}"))),
    }
}

/// Cross-route defaults under `component.global.defaults`, overlaid by each route.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GlobalDefaults {
    /// Default aggregation/partition key path (e.g. `body.tag.id`).
    pub key: Option<String>,
    /// Default route worker flush cadence (ms) — the aggregate window tick.
    #[serde(deserialize_with = "lenient_opt_u64")]
    pub flush_ms: Option<u64>,
    /// Default target when a route omits one.
    pub target: Option<String>,
}

/// One route (a `component.instances[]` entry).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteConfig {
    pub id: String,
    #[serde(default)]
    pub subscribe: Vec<String>,
    #[serde(default)]
    pub pipeline: Vec<StageConfig>,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub publish: Option<PublishConfig>,
    /// Aggregation/partition key path; falls back to the global default, then `body.tag.id`.
    #[serde(default)]
    pub key: Option<String>,
    /// Subscribe queue depth (drop-on-full at the broker edge).
    #[serde(default, deserialize_with = "lenient_opt_u64")]
    pub max_queue: Option<u64>,
}

/// One pipeline stage — externally tagged (`{"filter": {...}}`, `{"sample": {...}}`, …).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StageConfig {
    Filter(FilterSpec),
    Sample(SampleSpec),
    Aggregate(AggregateSpec),
    Project(ProjectSpec),
    /// A Rhai transform: `{"script": "<expr or statements>"}`. The script sees `topic` + the message
    /// fields and returns a new body map, or `()` to drop the message.
    Script(String),
}

/// `filter` stage. Exactly one form applies, checked in order: `script` (Rhai predicate) →
/// `quality` shorthand → `field`/`op`/`value` predicate.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct FilterSpec {
    /// A Rhai boolean expression over the message view; keep the message when it returns `true`.
    pub script: Option<String>,
    /// Shorthand: keep the message only when every `body.samples[].quality` equals this.
    pub quality: Option<String>,
    /// Built-in predicate: a dotted path (supports `[]` array spread).
    pub field: Option<String>,
    /// Comparison op: `eq`, `ne`, `gt`, `lt`, `ge`, `le`, `exists`, `contains`.
    pub op: Option<String>,
    /// Right-hand value for the comparison.
    pub value: Option<Value>,
}

/// `sample` stage: per-key downsampling by time (`everyMs`) or count (`everyN`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SampleSpec {
    #[serde(deserialize_with = "lenient_opt_u64")]
    pub every_ms: Option<u64>,
    #[serde(deserialize_with = "lenient_opt_u64")]
    pub every_n: Option<u64>,
    /// Key path for per-key sampling; falls back to the route key.
    pub by: Option<String>,
}

/// `aggregate` stage: tumbling windows (time `"10s"`/`"500ms"` or a bare count `"100"`), keyed,
/// reduced by the listed functions.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregateSpec {
    pub window: String,
    #[serde(default)]
    pub by: Option<String>,
    /// Reducers: `avg`, `max`, `min`, `sum`, `count`, `first`, `last`.
    #[serde(rename = "fn")]
    pub fns: Vec<String>,
}

/// `project` stage: keep a whitelist of body paths and/or set literal fields.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ProjectSpec {
    /// Body paths to keep (relative to `body`), e.g. `["tag.id", "tag.name", "samples"]`.
    pub keep: Option<Vec<String>>,
    /// Literal fields to set on the body.
    pub set: Option<Map<String, Value>>,
}

/// Per-route target/publish options.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PublishConfig {
    /// Target topic template for `local`/`northbound` (default: the source topic).
    pub topic: Option<String>,
    /// Partition-key path for `stream:<name>` (default: the route key).
    pub partition_key: Option<String>,
    /// QoS for `northbound`: `atLeastOnce` (default) or `atMostOnce`.
    pub qos: Option<String>,
}

/// Where a route forwards its output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// Republish on the local bus.
    Local,
    /// Publish to IoT Core / a northbound MQTT broker.
    Northbound,
    /// Append to a durable stream (exports to Kinesis/Kafka/file).
    Stream(String),
}

impl Target {
    /// Parse a `target` string: `local` | `northbound` | `stream:<name>`.
    pub fn parse(s: &str) -> anyhow::Result<Target> {
        let s = s.trim();
        if let Some(name) = s.strip_prefix("stream:") {
            let name = name.trim();
            anyhow::ensure!(!name.is_empty(), "target 'stream:' requires a stream name");
            Ok(Target::Stream(name.to_string()))
        } else if s.eq_ignore_ascii_case("local") {
            Ok(Target::Local)
        } else if s.eq_ignore_ascii_case("northbound") {
            Ok(Target::Northbound)
        } else {
            anyhow::bail!("unknown target '{s}' (expected local | northbound | stream:<name>)")
        }
    }
}

/// A window spec parsed from `aggregate.window`: a duration in ms, or a record count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Window {
    Time { ms: u64 },
    Count { n: u64 },
}

impl Window {
    /// Parse `"10s"` / `"500ms"` (time) or `"100"` (count).
    pub fn parse(s: &str) -> anyhow::Result<Window> {
        let s = s.trim();
        if let Some(v) = s.strip_suffix("ms") {
            Ok(Window::Time { ms: v.trim().parse()? })
        } else if let Some(v) = s.strip_suffix('s') {
            let secs: u64 = v.trim().parse()?;
            Ok(Window::Time { ms: secs.saturating_mul(1000) })
        } else {
            Ok(Window::Count { n: s.parse()? })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_route_with_pipeline() {
        let v = json!({
            "id": "r1",
            "subscribe": ["southbound/+/+/+/+"],
            "pipeline": [
                { "filter": { "quality": "GOOD" } },
                { "sample": { "everyMs": 1000.0 } },
                { "aggregate": { "window": "10s", "by": "body.tag.id", "fn": ["avg", "max"] } },
                { "project": { "keep": ["tag.id", "samples"] } },
                { "script": "body" }
            ],
            "target": "stream:archive"
        });
        let r: RouteConfig = serde_json::from_value(v).unwrap();
        assert_eq!(r.id, "r1");
        assert_eq!(r.pipeline.len(), 5);
        assert!(matches!(r.pipeline[0], StageConfig::Filter(_)));
        assert!(matches!(r.pipeline[4], StageConfig::Script(_)));
        assert_eq!(Target::parse(r.target.as_deref().unwrap()).unwrap(), Target::Stream("archive".into()));
    }

    #[test]
    fn lenient_numbers_for_greengrass_doubles() {
        let v = json!({ "id": "r", "pipeline": [ { "sample": { "everyN": 100.0 } } ] });
        let r: RouteConfig = serde_json::from_value(v).unwrap();
        match &r.pipeline[0] {
            StageConfig::Sample(s) => assert_eq!(s.every_n, Some(100)),
            _ => panic!(),
        }
    }

    #[test]
    fn window_parsing() {
        assert_eq!(Window::parse("10s").unwrap(), Window::Time { ms: 10_000 });
        assert_eq!(Window::parse("250ms").unwrap(), Window::Time { ms: 250 });
        assert_eq!(Window::parse("500").unwrap(), Window::Count { n: 500 });
    }

    #[test]
    fn target_parsing_errors() {
        assert!(Target::parse("bogus").is_err());
        assert!(Target::parse("stream:").is_err());
    }
}
