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
use crate::json_path::resolve_first_string;
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

    fn fold_value(&mut self, v: &Value) {
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
    fns: Vec<String>,
    accs: HashMap<String, Acc>,
}

impl AggregateStage {
    pub fn build(spec: &AggregateSpec, route_key: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(!spec.fns.is_empty(), "aggregate stage needs at least one `fn`");
        Ok(Self {
            window: Window::parse(&spec.window)?,
            key_path: spec.by.clone().unwrap_or_else(|| route_key.to_string()),
            fns: spec.fns.clone(),
            accs: HashMap::new(),
        })
    }

    fn key(&self, m: &ProcMsg) -> String {
        resolve_first_string(&m.msg, &self.key_path).unwrap_or_default()
    }

    /// Fold every `body.samples[].value` of `m` into the accumulator.
    fn fold_message(acc: &mut Acc, m: &ProcMsg) {
        match m.msg.body.get("samples").and_then(|s| s.as_array()) {
            Some(samples) => {
                for s in samples {
                    if let Some(v) = s.get("value") {
                        acc.fold_value(v);
                    }
                }
            }
            None => {
                // Non-southbound shape: fold the whole body as a single value.
                acc.fold_value(&m.msg.body);
            }
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

        // Preserve the source tag identity where present.
        let tag = acc
            .base
            .body
            .get("tag")
            .cloned()
            .unwrap_or_else(|| json!({ "id": key }));

        let body = json!({
            "tag": tag,
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
                Self::fold_message(acc, &m);
            }
            Window::Count { n } => {
                let acc = self
                    .accs
                    .entry(key.clone())
                    .or_insert_with(|| Acc::new(m.msg.clone(), m.topic.clone(), m.recv_ms, m.recv_ms));
                Self::fold_message(acc, &m);
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

    fn msg(tag: &str, value: f64, recv_ms: u64) -> ProcMsg {
        let m = MessageBuilder::new("SouthboundTagUpdate", "1.0")
            .payload(json!({ "tag": { "id": tag }, "samples": [ { "value": value, "quality": "GOOD" } ] }))
            .build();
        ProcMsg { topic: "t".into(), msg: m, recv_ms }
    }

    #[test]
    fn count_window_emits_at_n_with_reducers() {
        let spec = AggregateSpec {
            window: "3".into(),
            by: None,
            fns: vec!["avg".into(), "max".into(), "min".into(), "count".into()],
        };
        let mut s = AggregateStage::build(&spec, "body.tag.id").unwrap();
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
        let spec = AggregateSpec { window: "1s".into(), by: None, fns: vec!["count".into()] };
        let mut s = AggregateStage::build(&spec, "body.tag.id").unwrap();
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
        let spec = AggregateSpec { window: "1s".into(), by: None, fns: vec!["count".into()] };
        let mut s = AggregateStage::build(&spec, "body.tag.id").unwrap();
        assert!(s.process(msg("a", 1.0, 500)).is_empty()); // window [0,1000)
        let out = s.process(msg("a", 2.0, 1500)); // window [1000,2000) → closes prior
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].msg.body["agg"]["count"], json!(1));
    }
}
