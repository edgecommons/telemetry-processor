//! # `aggregate` stage — tumbling windowed reduction
//!
//! Folds sample values into per-key tumbling windows (time `"10s"`/`"500ms"` or a bare count
//! `"100"`) and emits one `ProcessedTelemetry` message per `(key, window)` on close. Reducers:
//! `avg`, `max`, `min`, `sum`, `count`, `first`, `last`. Time windows close on the worker tick
//! (`on_tick`) or when a later-window message arrives; count windows close in `process` at N.
//!
//! The emitted body carries `samples[0].value` = the **primary** (first-listed) reducer (so the
//! file sink's `rows` mode lands a value), plus the full reducer set under `agg` and a `window`
//! block (fully preserved in `raw` mode / downstream).

use std::collections::HashMap;

use ggcommons::messaging::message::Message;
use serde_json::{json, Map, Value};
use smallvec::SmallVec;

use crate::config::{AggregateSpec, Window};
use crate::json_path::{resolve_first_string, resolve_values};
use crate::proc::{Out, ProcMsg, Processor};

#[derive(Clone)]
struct Acc {
    count: u64,
    n_num: u64,
    sum: f64,
    min: f64,
    max: f64,
    first: Option<Value>,
    last: Option<Value>,
    base: Message,
    topic: String,
    window_start: u64,
    window_end: u64,
}

impl Acc {
    fn new(base: Message, topic: String, window_start: u64, window_end: u64) -> Self {
        Self {
            count: 0,
            n_num: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            first: None,
            last: None,
            base,
            topic,
            window_start,
            window_end,
        }
    }

    /// Fold one value into the accumulator. An **array** value is folded element-wise (each element
    /// counts and contributes to the numeric reducers), so an array-typed signal — an OPC UA array
    /// node, or a `value` path that resolves to an array — aggregates across its elements instead of
    /// being dropped as non-numeric. Nested arrays recurse; an empty array folds nothing.
    fn fold_value(&mut self, v: &Value) {
        if let Some(arr) = v.as_array() {
            for el in arr {
                self.fold_value(el);
            }
            return;
        }
        self.count += 1;
        if self.first.is_none() {
            self.first = Some(v.clone());
        }
        self.last = Some(v.clone());
        if let Some(n) = v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())) {
            self.n_num += 1;
            self.sum += n;
            self.min = self.min.min(n);
            self.max = self.max.max(n);
        }
    }
}

/// An `aggregate` pipeline stage.
pub struct AggregateStage {
    window: Window,
    key_path: String,
    /// Path to the value(s) to fold; `None` → the default (`body.samples[].value` / whole body).
    value_path: Option<String>,
    fns: Vec<String>,
    accs: HashMap<String, Acc>,
}

impl AggregateStage {
    pub fn build(spec: &AggregateSpec, route_key: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(!spec.fns.is_empty(), "aggregate stage needs at least one `fn`");
        Ok(Self {
            window: Window::parse(&spec.window)?,
            key_path: spec.by.clone().unwrap_or_else(|| route_key.to_string()),
            value_path: spec.value.clone(),
            fns: spec.fns.clone(),
            accs: HashMap::new(),
        })
    }

    fn key(&self, m: &ProcMsg) -> String {
        resolve_first_string(&m.msg, &self.key_path).unwrap_or_default()
    }

    /// The value(s) of `m` to fold this message: the configured `value` path (supports `[]`), or —
    /// by default — every `body.samples[].value`, falling back to the whole body for payloads with
    /// no `samples`. Returns owned values so the caller folds them without holding a `self` borrow.
    fn extract_values(&self, m: &ProcMsg) -> Vec<Value> {
        if let Some(path) = &self.value_path {
            return resolve_values(&m.msg, path);
        }
        match m.msg.body.get("samples").and_then(|s| s.as_array()) {
            Some(samples) => samples.iter().filter_map(|s| s.get("value").cloned()).collect(),
            None => vec![m.msg.body.clone()],
        }
    }

    /// Build the emitted message from a closed accumulator.
    fn emit(&self, acc: Acc, key: &str) -> ProcMsg {
        let mut agg = Map::new();
        for f in &self.fns {
            let v = match f.as_str() {
                "count" => json!(acc.count),
                "sum" if acc.n_num > 0 => json!(acc.sum),
                "avg" if acc.n_num > 0 => json!(acc.sum / acc.n_num as f64),
                "min" if acc.n_num > 0 => json!(acc.min),
                "max" if acc.n_num > 0 => json!(acc.max),
                "first" => acc.first.clone().unwrap_or(Value::Null),
                "last" => acc.last.clone().unwrap_or(Value::Null),
                _ => Value::Null,
            };
            agg.insert(f.clone(), v);
        }
        let primary = agg.get(&self.fns[0]).cloned().unwrap_or(Value::Null);

        // Preserve the source signal identity where present.
        let signal = acc
            .base
            .body
            .get("signal")
            .cloned()
            .unwrap_or_else(|| json!({ "id": key }));

        let body = json!({
            "signal": signal,
            "samples": [ { "value": primary, "quality": "GOOD" } ],
            "agg": Value::Object(agg),
            "window": { "startMs": acc.window_start, "endMs": acc.window_end, "count": acc.count }
        });

        let mut out = acc.base;
        out.header.name = "ProcessedTelemetry".to_string();
        out.body = body;
        ProcMsg { topic: acc.topic, msg: out, recv_ms: acc.window_end }
    }

    fn flush_due(&mut self, now_ms: u64, force: bool) -> Out {
        let due: Vec<String> = self
            .accs
            .iter()
            .filter(|(_, a)| force || a.window_end <= now_ms)
            .map(|(k, _)| k.clone())
            .collect();
        let mut out: Out = SmallVec::new();
        for k in due {
            if let Some(acc) = self.accs.remove(&k) {
                out.push(self.emit(acc, &k));
            }
        }
        out
    }
}

impl Processor for AggregateStage {
    fn process(&mut self, m: ProcMsg) -> Out {
        let key = self.key(&m);
        let values = self.extract_values(&m);
        let mut out: Out = SmallVec::new();

        match self.window {
            Window::Time { ms } => {
                let ws = (m.recv_ms / ms) * ms;
                let we = ws + ms;
                // A message for a newer window closes the prior one for this key.
                if let Some(existing) = self.accs.get(&key) {
                    if existing.window_end != we {
                        if let Some(acc) = self.accs.remove(&key) {
                            out.push(self.emit(acc, &key));
                        }
                    }
                }
                let acc = self
                    .accs
                    .entry(key.clone())
                    .or_insert_with(|| Acc::new(m.msg.clone(), m.topic.clone(), ws, we));
                for v in &values {
                    acc.fold_value(v);
                }
            }
            Window::Count { n } => {
                let acc = self
                    .accs
                    .entry(key.clone())
                    .or_insert_with(|| Acc::new(m.msg.clone(), m.topic.clone(), m.recv_ms, m.recv_ms));
                for v in &values {
                    acc.fold_value(v);
                }
                if acc.count >= n {
                    if let Some(acc) = self.accs.remove(&key) {
                        out.push(self.emit(acc, &key));
                    }
                }
            }
        }
        out
    }

    fn on_tick(&mut self, now_ms: u64) -> Out {
        match self.window {
            Window::Time { .. } => self.flush_due(now_ms, false),
            Window::Count { .. } => SmallVec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ggcommons::messaging::message::MessageBuilder;
    use serde_json::json;

    fn msg(signal: &str, value: f64, recv_ms: u64) -> ProcMsg {
        let m = MessageBuilder::new("SouthboundSignalUpdate", "1.0")
            .payload(json!({ "signal": { "id": signal }, "samples": [ { "value": value, "quality": "GOOD" } ] }))
            .build();
        ProcMsg { topic: "t".into(), msg: m, recv_ms }
    }

    #[test]
    fn count_window_emits_at_n_with_reducers() {
        let spec = AggregateSpec {
            window: "3".into(),
            by: None,
            fns: vec!["avg".into(), "max".into(), "min".into(), "count".into()],
            value: None,
        };
        let mut s = AggregateStage::build(&spec, "body.signal.id").unwrap();
        assert!(s.process(msg("a", 10.0, 1)).is_empty());
        assert!(s.process(msg("a", 20.0, 2)).is_empty());
        let out = s.process(msg("a", 30.0, 3));
        assert_eq!(out.len(), 1);
        let agg = &out[0].msg.body["agg"];
        assert_eq!(agg["avg"], json!(20.0));
        assert_eq!(agg["max"], json!(30.0));
        assert_eq!(agg["min"], json!(10.0));
        assert_eq!(agg["count"], json!(3));
        // primary (first fn = avg) lands in samples[0].value for the file sink rows mode.
        assert_eq!(out[0].msg.body["samples"][0]["value"], json!(20.0));
    }

    #[test]
    fn time_window_closes_on_tick() {
        let spec = AggregateSpec { window: "1s".into(), by: None, fns: vec!["count".into()], value: None };
        let mut s = AggregateStage::build(&spec, "body.signal.id").unwrap();
        // Two messages in window [0,1000)
        assert!(s.process(msg("a", 1.0, 100)).is_empty());
        assert!(s.process(msg("a", 2.0, 900)).is_empty());
        // Tick before window end → nothing.
        assert!(s.on_tick(999).is_empty());
        // Tick at/after window end → flush.
        let out = s.on_tick(1000);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].msg.body["agg"]["count"], json!(2));
    }

    #[test]
    fn time_window_closes_on_newer_window_message() {
        let spec = AggregateSpec { window: "1s".into(), by: None, fns: vec!["count".into()], value: None };
        let mut s = AggregateStage::build(&spec, "body.signal.id").unwrap();
        assert!(s.process(msg("a", 1.0, 500)).is_empty()); // window [0,1000)
        let out = s.process(msg("a", 2.0, 1500)); // window [1000,2000) → closes prior
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].msg.body["agg"]["count"], json!(1));
    }

    #[test]
    fn aggregate_folds_array_values_elementwise() {
        // An array-typed signal (e.g. an OPC UA array node): each sample's value is an array. The
        // reducers fold across ALL elements of ALL samples in the window (a time window here, so the
        // close is time-driven and independent of how many elements each message carries).
        let spec = AggregateSpec {
            window: "1s".into(),
            by: None,
            fns: vec!["avg".into(), "min".into(), "max".into(), "sum".into(), "count".into()],
            value: None,
        };
        let mut s = AggregateStage::build(&spec, "body.signal.id").unwrap();
        let mk = |arr: serde_json::Value, recv: u64| {
            let m = MessageBuilder::new("SouthboundSignalUpdate", "1.0")
                .payload(json!({ "signal": { "id": "a" }, "samples": [ { "value": arr, "quality": "GOOD" } ] }))
                .build();
            ProcMsg { topic: "t".into(), msg: m, recv_ms: recv }
        };
        // Window [0,1000): two array-valued messages.
        assert!(s.process(mk(json!([1.0, 2.0, 3.0]), 100)).is_empty());
        assert!(s.process(mk(json!([4.0, 5.0]), 200)).is_empty());
        let out = s.on_tick(1000);
        assert_eq!(out.len(), 1);
        let agg = &out[0].msg.body["agg"];
        // elements folded across both messages: 1,2,3,4,5  → avg 3, min 1, max 5, sum 15, count 5
        assert_eq!(agg["avg"], json!(3.0));
        assert_eq!(agg["min"], json!(1.0));
        assert_eq!(agg["max"], json!(5.0));
        assert_eq!(agg["sum"], json!(15.0));
        assert_eq!(agg["count"], json!(5));
    }

    #[test]
    fn aggregate_custom_value_path_non_southbound() {
        // A non-SouthboundSignalUpdate payload: aggregate `body.temp`, keyed by `body.id`.
        let spec = AggregateSpec {
            window: "2".into(),
            by: Some("body.id".into()),
            fns: vec!["avg".into(), "count".into()],
            value: Some("body.temp".into()),
        };
        let mut s = AggregateStage::build(&spec, "body.id").unwrap();
        let mk = |id: &str, temp: f64| {
            let m = MessageBuilder::new("Custom", "1.0")
                .payload(json!({ "id": id, "temp": temp }))
                .build();
            ProcMsg { topic: "t".into(), msg: m, recv_ms: 1 }
        };
        assert!(s.process(mk("d", 10.0)).is_empty());
        let out = s.process(mk("d", 20.0));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].msg.body["agg"]["avg"], json!(15.0));
        assert_eq!(out[0].msg.body["agg"]["count"], json!(2));
    }
}
