//! # Rhai stage + the shared Rhai evaluator
//!
//! The `script` stage runs a Rhai program per message that returns a new body (or `()` to drop the
//! message). The same [`RhaiEval`] backs the Rhai `filter` option (evaluating to a boolean). The
//! engine is shared across all routes; each stage compiles its source to an `AST` once at build.
//!
//! Scope exposed to a script: `topic` (string), `body` / `tags` (maps), `samples` (array), and the
//! convenience bindings `value` / `quality` (the first sample's). A `filter` script returns a
//! boolean; a `script` stage returns the new body map (or `()` to drop).

use std::sync::Arc;

use rhai::{Dynamic, Engine, Scope, AST};
use serde_json::Value;
use smallvec::smallvec;

use crate::proc::{Out, ProcMsg, Processor};

/// A compiled Rhai program plus the shared engine.
pub struct RhaiEval {
    engine: Arc<Engine>,
    ast: AST,
}

impl RhaiEval {
    /// Compile `src` against the shared engine.
    pub fn compile(engine: &Arc<Engine>, src: &str) -> anyhow::Result<Self> {
        let ast = engine
            .compile(src)
            .map_err(|e| anyhow::anyhow!("rhai compile error in `{src}`: {e}"))?;
        Ok(Self { engine: engine.clone(), ast })
    }

    fn scope_for(&self, m: &ProcMsg) -> Scope<'static> {
        let mut scope = Scope::new();
        scope.push("topic", m.topic.clone());
        scope.push_dynamic("body", to_dyn(&m.msg.body));
        if let Ok(tags) = serde_json::to_value(&m.msg.tags) {
            scope.push_dynamic("tags", to_dyn(&tags));
        }
        let samples = m.msg.body.get("samples").cloned().unwrap_or(Value::Array(vec![]));
        scope.push_dynamic("samples", to_dyn(&samples));
        // Convenience bindings: the first sample's value + quality.
        let first = m.msg.body.get("samples").and_then(|s| s.as_array()).and_then(|a| a.first());
        let value = first.and_then(|s| s.get("value")).cloned().unwrap_or(Value::Null);
        let quality =
            first.and_then(|s| s.get("quality")).and_then(|q| q.as_str()).unwrap_or("").to_string();
        scope.push_dynamic("value", to_dyn(&value));
        scope.push("quality", quality);
        scope
    }

    /// Evaluate to a boolean (the Rhai `filter` option). Errors → `false` (drop), logged.
    pub fn eval_bool(&self, m: &ProcMsg) -> bool {
        let mut scope = self.scope_for(m);
        match self.engine.eval_ast_with_scope::<Dynamic>(&mut scope, &self.ast) {
            Ok(d) => d.as_bool().unwrap_or(false),
            Err(e) => {
                tracing::warn!(error = %e, "rhai filter eval error; dropping message");
                false
            }
        }
    }

    /// Evaluate to a new body (the `script` stage). `()` → drop; non-convertible/error → drop, logged.
    pub fn eval_body(&self, m: &ProcMsg) -> Option<Value> {
        let mut scope = self.scope_for(m);
        match self.engine.eval_ast_with_scope::<Dynamic>(&mut scope, &self.ast) {
            Ok(d) if d.is_unit() => None,
            Ok(d) => match rhai::serde::from_dynamic::<Value>(&d) {
                Ok(v) => Some(v),
                Err(e) => {
                    tracing::warn!(error = %e, "rhai result not convertible to JSON; dropping");
                    None
                }
            },
            Err(e) => {
                tracing::warn!(error = %e, "rhai script eval error; dropping message");
                None
            }
        }
    }
}

fn to_dyn(v: &Value) -> Dynamic {
    rhai::serde::to_dynamic(v.clone()).unwrap_or(Dynamic::UNIT)
}

/// A `script` pipeline stage: replace the message body with the script's result, or drop it.
pub struct ScriptStage {
    eval: RhaiEval,
}

impl ScriptStage {
    pub fn build(src: &str, engine: &Arc<Engine>) -> anyhow::Result<Self> {
        Ok(Self { eval: RhaiEval::compile(engine, src)? })
    }
}

impl Processor for ScriptStage {
    fn process(&mut self, mut m: ProcMsg) -> Out {
        match self.eval.eval_body(&m) {
            Some(new_body) => {
                m.msg.body = new_body;
                smallvec![m]
            }
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proc::now_ms;
    use ggcommons::messaging::message::MessageBuilder;
    use serde_json::json;

    fn pm(body: Value) -> ProcMsg {
        let m = MessageBuilder::new("X", "1.0").thing_name("thing-1").payload(body).build();
        ProcMsg { topic: "t".into(), msg: m, recv_ms: now_ms() }
    }

    fn engine() -> Arc<Engine> {
        Arc::new(Engine::new())
    }

    #[test]
    fn script_transforms_body_using_value_binding() {
        let mut s = ScriptStage::build(r#"#{ "doubled": value * 2 }"#, &engine()).unwrap();
        let out = s.process(pm(json!({ "samples": [{ "value": 21, "quality": "GOOD" }] })));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].msg.body["doubled"], json!(42));
    }

    #[test]
    fn script_can_read_topic_and_tags() {
        let mut s = ScriptStage::build(r#"#{ "thing": tags.thing, "q": quality }"#, &engine()).unwrap();
        let out = s.process(pm(json!({ "samples": [{ "value": 1, "quality": "GOOD" }] })));
        assert_eq!(out[0].msg.body["thing"], json!("thing-1"));
        assert_eq!(out[0].msg.body["q"], json!("GOOD"));
    }

    #[test]
    fn script_unit_result_drops_message() {
        let mut s = ScriptStage::build("()", &engine()).unwrap();
        assert_eq!(s.process(pm(json!({ "samples": [] }))).len(), 0);
    }

    #[test]
    fn compile_error_is_reported() {
        assert!(ScriptStage::build("this is not valid rhai @@", &engine()).is_err());
    }
}
