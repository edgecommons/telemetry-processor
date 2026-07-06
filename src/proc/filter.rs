//! # `filter` stage — keep/drop whole messages
//!
//! Three forms (checked in order): a Rhai boolean predicate (`script`), a `quality` shorthand
//! (keep only when every sample's quality matches), or a built-in `field`/`op`/`value` predicate
//! over a dotted path (with `[]` array spread → any-element match). Built-in predicates compile to
//! a fixed form at build time — no per-message parsing.

use std::sync::Arc;

use rhai::Engine;
use serde_json::Value;
use smallvec::smallvec;

use crate::config::{FilterSpec, ScriptEngineKind};
use crate::json_path::resolve_values;
use crate::proc::script::{build_engine, ScriptContext, ScriptEngine, ScriptLoader};
use crate::proc::{Out, ProcMsg, Processor};

enum Op {
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
    Exists,
    Contains,
}

enum Predicate {
    /// A Rhai or Lua boolean predicate over the message view.
    Script(Box<dyn ScriptEngine>),
    /// Keep only when **every** `body.samples[].quality` equals this string (and ≥1 sample exists).
    QualityAll(String),
    /// Keep when **any** resolved value at `path` satisfies `op` against `value`.
    Field { path: String, op: Op, value: Value },
}

/// A `filter` pipeline stage.
pub struct FilterStage {
    pred: Predicate,
}

impl FilterStage {
    pub fn build(
        spec: &FilterSpec,
        kind: ScriptEngineKind,
        engine: &Arc<Engine>,
        loader: &ScriptLoader,
        ctx: &Arc<ScriptContext>,
    ) -> anyhow::Result<Self> {
        let pred = if let Some(src) = &spec.script {
            Predicate::Script(build_engine(kind, &loader.load(src)?, engine, ctx)?)
        } else if let Some(q) = &spec.quality {
            Predicate::QualityAll(q.clone())
        } else if let Some(field) = &spec.field {
            let op = Op::parse(spec.op.as_deref().unwrap_or("eq"))?;
            let value = spec.value.clone().unwrap_or(Value::Null);
            Predicate::Field { path: field.clone(), op, value }
        } else {
            anyhow::bail!("filter stage needs one of: `script` | `quality` | `field`(+`op`/`value`)");
        };
        Ok(Self { pred })
    }

    fn keep(&self, m: &ProcMsg) -> bool {
        match &self.pred {
            Predicate::Script(e) => e.eval_bool(m),
            Predicate::QualityAll(q) => {
                let qs = resolve_values(&m.msg, "body.samples[].quality");
                !qs.is_empty() && qs.iter().all(|v| v.as_str() == Some(q.as_str()))
            }
            Predicate::Field { path, op, value } => {
                let vals = resolve_values(&m.msg, path);
                match op {
                    Op::Exists => !vals.is_empty(),
                    _ => vals.iter().any(|v| op.test(v, value)),
                }
            }
        }
    }
}

impl Processor for FilterStage {
    fn process(&mut self, m: ProcMsg) -> Out {
        if self.keep(&m) {
            smallvec![m]
        } else {
            smallvec![]
        }
    }
}

impl Op {
    fn parse(s: &str) -> anyhow::Result<Op> {
        Ok(match s.trim().to_ascii_lowercase().as_str() {
            "eq" | "==" | "=" => Op::Eq,
            "ne" | "!=" => Op::Ne,
            "gt" | ">" => Op::Gt,
            "lt" | "<" => Op::Lt,
            "ge" | ">=" => Op::Ge,
            "le" | "<=" => Op::Le,
            "exists" => Op::Exists,
            "contains" => Op::Contains,
            other => anyhow::bail!("unknown filter op '{other}'"),
        })
    }

    fn test(&self, left: &Value, right: &Value) -> bool {
        match self {
            Op::Eq => values_eq(left, right),
            Op::Ne => !values_eq(left, right),
            Op::Exists => true,
            Op::Contains => left
                .as_str()
                .zip(right.as_str())
                .map(|(l, r)| l.contains(r))
                .unwrap_or(false),
            Op::Gt | Op::Lt | Op::Ge | Op::Le => match (as_num(left), as_num(right)) {
                (Some(l), Some(r)) => match self {
                    Op::Gt => l > r,
                    Op::Lt => l < r,
                    Op::Ge => l >= r,
                    Op::Le => l <= r,
                    _ => unreachable!(),
                },
                _ => false,
            },
        }
    }
}

fn values_eq(a: &Value, b: &Value) -> bool {
    if let (Some(x), Some(y)) = (as_num(a), as_num(b)) {
        return (x - y).abs() < f64::EPSILON;
    }
    a == b
}

fn as_num(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ScriptSource;
    use crate::proc::now_ms;
    use edgecommons::messaging::message::MessageBuilder;
    use serde_json::json;

    fn msg(samples: Value) -> ProcMsg {
        let m = MessageBuilder::new("SouthboundSignalUpdate", "1.0")
            .payload(json!({ "signal": { "id": "t1" }, "samples": samples }))
            .build();
        ProcMsg { topic: "ecv1/gw-01/opcua-adapter/kep1/data/x".into(), msg: m, recv_ms: now_ms() }
    }

    fn ctx() -> Arc<ScriptContext> {
        Arc::new(ScriptContext::default())
    }

    fn engine() -> Arc<Engine> {
        Arc::new(Engine::new())
    }

    #[test]
    fn quality_all_keeps_only_all_good() {
        let spec = FilterSpec { quality: Some("GOOD".into()), ..Default::default() };
        let mut s = FilterStage::build(&spec, ScriptEngineKind::Rhai, &engine(), &ScriptLoader::default(), &ctx()).unwrap();
        let good = msg(json!([{ "value": 1, "quality": "GOOD" }, { "value": 2, "quality": "GOOD" }]));
        let mixed = msg(json!([{ "value": 1, "quality": "GOOD" }, { "value": 2, "quality": "BAD" }]));
        assert_eq!(s.process(good).len(), 1);
        assert_eq!(s.process(mixed).len(), 0);
    }

    #[test]
    fn field_op_value_numeric() {
        let spec = FilterSpec {
            field: Some("body.samples[].value".into()),
            op: Some("gt".into()),
            value: Some(json!(50)),
            ..Default::default()
        };
        let mut s = FilterStage::build(&spec, ScriptEngineKind::Rhai, &engine(), &ScriptLoader::default(), &ctx()).unwrap();
        assert_eq!(s.process(msg(json!([{ "value": 99 }]))).len(), 1);
        assert_eq!(s.process(msg(json!([{ "value": 10 }]))).len(), 0);
    }

    #[test]
    fn rhai_filter_predicate() {
        let spec = FilterSpec {
            script: Some(ScriptSource::Inline("samples.all(|s| s.quality == \"GOOD\")".into())),
            ..Default::default()
        };
        let mut s = FilterStage::build(&spec, ScriptEngineKind::Rhai, &engine(), &ScriptLoader::default(), &ctx()).unwrap();
        assert_eq!(s.process(msg(json!([{ "value": 1, "quality": "GOOD" }]))).len(), 1);
        assert_eq!(s.process(msg(json!([{ "value": 1, "quality": "BAD" }]))).len(), 0);
    }

    fn mk(field: &str, op: &str, value: Option<Value>) -> FilterStage {
        FilterStage::build(
            &FilterSpec {
                field: Some(field.into()),
                op: Some(op.into()),
                value,
                ..Default::default()
            },
            ScriptEngineKind::Rhai,
            &engine(),
            &ScriptLoader::default(),
            &ctx(),
        )
        .unwrap()
    }

    #[test]
    fn all_comparison_ops() {
        let m = || msg(json!([{ "value": 30, "quality": "GOOD" }]));
        assert_eq!(mk("body.samples[].value", "lt", Some(json!(50))).process(m()).len(), 1);
        assert_eq!(mk("body.samples[].value", "gt", Some(json!(50))).process(m()).len(), 0);
        assert_eq!(mk("body.samples[].value", "ge", Some(json!(30))).process(m()).len(), 1);
        assert_eq!(mk("body.samples[].value", "le", Some(json!(10))).process(m()).len(), 0);
        assert_eq!(mk("body.samples[].quality", "ne", Some(json!("BAD"))).process(m()).len(), 1);
        assert_eq!(mk("body.samples[].quality", "eq", Some(json!("GOOD"))).process(m()).len(), 1);
        assert_eq!(mk("body.signal.id", "exists", None).process(m()).len(), 1);
        assert_eq!(mk("body.missing", "exists", None).process(m()).len(), 0);
        assert_eq!(mk("body.signal.id", "contains", Some(json!("t"))).process(m()).len(), 1);
        assert_eq!(mk("body.signal.id", "contains", Some(json!("zzz"))).process(m()).len(), 0);
    }

    #[test]
    fn build_and_op_parse_errors() {
        // No predicate form configured.
        assert!(FilterStage::build(&FilterSpec::default(), ScriptEngineKind::Rhai, &engine(), &ScriptLoader::default(), &ctx()).is_err());
        // Unknown op.
        assert!(Op::parse("bogus").is_err());
        // Symbolic aliases parse.
        assert!(Op::parse(">=").is_ok());
        assert!(Op::parse("!=").is_ok());
    }
}
