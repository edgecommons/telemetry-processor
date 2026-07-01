//! # Rhai stage + the shared Rhai evaluator
//!
//! The `script` stage runs a Rhai program per message that returns a new body (or `()` to drop the
//! message). The same [`RhaiEval`] backs the Rhai `filter` option (evaluating to a boolean). The
//! engine is shared across all routes; each stage compiles its source to an `AST` once at build.
//!
//! Scope exposed to a script: the message view (`topic`, the `header` / `body` / `tags` maps,
//! `samples` array, and the convenience bindings `value` / `quality` — the first sample's), plus the
//! **runtime context**
//! (`thingName`, `componentName`, `componentFullName`, `routeId`, `recvMs`) so a generic script can
//! branch on which component/route/thing it runs in. A `filter` script returns a boolean; a `script`
//! stage returns the new body map (or `()` to drop). Array-valued fields arrive as Rhai arrays, so a
//! script can `for`/`map`/`filter`/`reduce` over them like any other collection.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use rhai::{Dynamic, Engine, Scope, AST};
use serde_json::Value;
use smallvec::smallvec;

use crate::config::ScriptSource;
use crate::proc::{Out, ProcMsg, Processor};

/// Per-route runtime context injected into every `filter`/`script` evaluation as **constant**
/// bindings, alongside the per-message view. It carries the component identity and the route id so a
/// single reusable script can behave differently per component/route (e.g. stamp `thingName`, or gate
/// on `routeId`) without hard-coding those values in the script text.
///
/// The identity mirrors the config template variables: `thing_name` = `{ThingName}`, `component_name`
/// = `{ComponentName}` (the short name — the segment after the last `.`), `component_full_name` =
/// `{ComponentFullName}`. Values are the **raw** identity (not topic-sanitized like the template
/// resolver's output). Cheap to clone; the app builds one `Arc<ScriptContext>` per route.
#[derive(Debug, Default, Clone)]
pub struct ScriptContext {
    /// IoT Thing name — exposed to scripts as `thingName`.
    pub thing_name: String,
    /// Short component name (after the last `.`) — exposed as `componentName`.
    pub component_name: String,
    /// Fully-qualified component name — exposed as `componentFullName`.
    pub component_full_name: String,
    /// The owning route's id — exposed as `routeId`.
    pub route_id: String,
}

/// Resolves [`ScriptSource`]s to Rhai source text. `File` paths resolve against `base` (the
/// `global.defaults.scriptsDir`) when relative, or are used as-is when absolute.
pub struct ScriptLoader {
    base: PathBuf,
}

impl ScriptLoader {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self { base: base_dir.into() }
    }

    /// Load a script source to its Rhai text (reading the file for [`ScriptSource::File`]).
    pub fn load(&self, src: &ScriptSource) -> anyhow::Result<String> {
        match src {
            ScriptSource::Inline(s) => Ok(s.clone()),
            ScriptSource::File { file } => {
                let p = Path::new(file);
                let path = if p.is_absolute() { p.to_path_buf() } else { self.base.join(p) };
                std::fs::read_to_string(&path)
                    .with_context(|| format!("reading script file {}", path.display()))
            }
        }
    }
}

impl Default for ScriptLoader {
    fn default() -> Self {
        Self::new(".")
    }
}

/// A compiled Rhai program plus the shared engine and the per-route runtime context.
pub struct RhaiEval {
    engine: Arc<Engine>,
    ast: AST,
    ctx: Arc<ScriptContext>,
}

impl RhaiEval {
    /// Compile `src` against the shared engine, binding the runtime `ctx` into every evaluation.
    pub fn compile(
        engine: &Arc<Engine>,
        src: &str,
        ctx: &Arc<ScriptContext>,
    ) -> anyhow::Result<Self> {
        let ast = engine
            .compile(src)
            .map_err(|e| anyhow::anyhow!("rhai compile error in `{src}`: {e}"))?;
        Ok(Self { engine: engine.clone(), ast, ctx: ctx.clone() })
    }

    fn scope_for(&self, m: &ProcMsg) -> Scope<'static> {
        let mut scope = Scope::new();
        scope.push("topic", m.topic.clone());
        // Runtime context — constant per route, so a generic/reused script can branch on identity.
        scope.push("thingName", self.ctx.thing_name.clone());
        scope.push("componentName", self.ctx.component_name.clone());
        scope.push("componentFullName", self.ctx.component_full_name.clone());
        scope.push("routeId", self.ctx.route_id.clone());
        scope.push("recvMs", m.recv_ms as i64);
        scope.push_dynamic("body", to_dyn(&m.msg.body));
        // The whole message envelope, symmetric with `body`/`tags`: `header.name`, `header.version`,
        // `header.timestamp`, `header.uuid`, `header.correlation_id`, `header.reply_to`.
        if let Ok(header) = serde_json::to_value(&m.msg.header) {
            scope.push_dynamic("header", to_dyn(&header));
        }
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
    pub fn build(
        src: &ScriptSource,
        engine: &Arc<Engine>,
        loader: &ScriptLoader,
        ctx: &Arc<ScriptContext>,
    ) -> anyhow::Result<Self> {
        let text = loader.load(src)?;
        Ok(Self { eval: RhaiEval::compile(engine, &text, ctx)? })
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

    fn ctx() -> Arc<ScriptContext> {
        Arc::new(ScriptContext::default())
    }

    #[test]
    fn script_transforms_body_using_value_binding() {
        let mut s = ScriptStage::build(
            &ScriptSource::Inline(r#"#{ "doubled": value * 2 }"#.into()),
            &engine(),
            &ScriptLoader::default(),
            &ctx(),
        )
        .unwrap();
        let out = s.process(pm(json!({ "samples": [{ "value": 21, "quality": "GOOD" }] })));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].msg.body["doubled"], json!(42));
    }

    #[test]
    fn script_can_read_topic_and_tags() {
        let mut s = ScriptStage::build(
            &ScriptSource::Inline(r#"#{ "thing": tags.thing, "q": quality }"#.into()),
            &engine(),
            &ScriptLoader::default(),
            &ctx(),
        )
        .unwrap();
        let out = s.process(pm(json!({ "samples": [{ "value": 1, "quality": "GOOD" }] })));
        assert_eq!(out[0].msg.body["thing"], json!("thing-1"));
        assert_eq!(out[0].msg.body["q"], json!("GOOD"));
    }

    #[test]
    fn script_unit_result_drops_message() {
        let mut s = ScriptStage::build(
            &ScriptSource::Inline("()".into()),
            &engine(),
            &ScriptLoader::default(),
            &ctx(),
        )
        .unwrap();
        assert_eq!(s.process(pm(json!({ "samples": [] }))).len(), 0);
    }

    #[test]
    fn compile_error_is_reported() {
        assert!(ScriptStage::build(
            &ScriptSource::Inline("this is not valid rhai @@".into()),
            &engine(),
            &ScriptLoader::default(),
            &ctx(),
        )
        .is_err());
    }

    #[test]
    fn loads_script_from_external_file() {
        // A `{"file": "..."}` source is read relative to the loader base dir.
        let dir = std::env::temp_dir().join("tp-script-load-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("double.rhai");
        std::fs::write(&path, r#"#{ "doubled": value * 2 }"#).unwrap();

        let loader = ScriptLoader::new(&dir);
        let src = ScriptSource::File { file: "double.rhai".into() };
        let mut s = ScriptStage::build(&src, &engine(), &loader, &ctx()).unwrap();
        let out = s.process(pm(json!({ "samples": [{ "value": 21, "quality": "GOOD" }] })));
        assert_eq!(out[0].msg.body["doubled"], json!(42));

        // A missing file is a build error (surfaced at startup, not silently ignored).
        let missing = ScriptSource::File { file: "nope.rhai".into() };
        assert!(ScriptStage::build(&missing, &engine(), &loader, &ctx()).is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    // A `ScriptContext` with real identity, for the runtime-context test.
    fn ctx_with(thing: &str, comp: &str, full: &str, route: &str) -> Arc<ScriptContext> {
        Arc::new(ScriptContext {
            thing_name: thing.into(),
            component_name: comp.into(),
            component_full_name: full.into(),
            route_id: route.into(),
        })
    }

    #[test]
    fn script_sees_runtime_context() {
        // The runtime identity is available so one generic script can stamp/branch on where it runs.
        let src = ScriptSource::Inline(
            r#"#{ "t": thingName, "c": componentName, "cf": componentFullName, "r": routeId, "gotTs": recvMs > 0 }"#
                .into(),
        );
        let ctx = ctx_with("edge-42", "TelemetryProcessor", "com.acme.TelemetryProcessor", "archive");
        let mut s = ScriptStage::build(&src, &engine(), &ScriptLoader::default(), &ctx).unwrap();
        let out = s.process(pm(json!({ "samples": [] })));
        let b = &out[0].msg.body;
        assert_eq!(b["t"], json!("edge-42"));
        assert_eq!(b["c"], json!("TelemetryProcessor"));
        assert_eq!(b["cf"], json!("com.acme.TelemetryProcessor"));
        assert_eq!(b["r"], json!("archive"));
        assert_eq!(b["gotTs"], json!(true));
    }

    #[test]
    fn script_processes_array_value_with_fn_and_loop() {
        // An array-typed sample value arrives as a Rhai array; a user fn + `for` loop reduce it.
        // Goal: emit the mean and peak of an OPC UA array node's readings.
        let src = ScriptSource::Inline(
            r#"
            fn mean(xs) {
                if xs.is_empty() { return 0.0; }
                let s = 0.0;
                for x in xs { s += x; }
                s / xs.len()
            }
            let readings = value;              // the first sample's value — an array here
            let peak = readings[0];
            for x in readings { if x > peak { peak = x; } }
            #{ "mean": mean(readings), "peak": peak, "n": readings.len() }
            "#
            .into(),
        );
        let mut s = ScriptStage::build(&src, &engine(), &ScriptLoader::default(), &ctx()).unwrap();
        let out = s.process(pm(json!({ "samples": [{ "value": [10.0, 20.0, 30.0], "quality": "GOOD" }] })));
        let b = &out[0].msg.body;
        assert_eq!(b["mean"], json!(20.0));
        assert_eq!(b["peak"], json!(30.0));
        assert_eq!(b["n"], json!(3));
    }

    #[test]
    fn script_filters_array_with_map_filter() {
        // A filter script over an array value: keep only when ≥2 elements exceed a threshold.
        // Demonstrates array `.filter` + `.len` in a boolean predicate.
        let src = ScriptSource::Inline(
            r#"value.filter(|x| x > 50).len() >= 2"#.into(),
        );
        let e = RhaiEval::compile(&engine(), &loader_load(&src), &ctx()).unwrap();
        let keep = pm(json!({ "samples": [{ "value": [10, 60, 70, 20], "quality": "GOOD" }] }));
        let drop = pm(json!({ "samples": [{ "value": [10, 60, 20, 30], "quality": "GOOD" }] }));
        assert!(e.eval_bool(&keep));
        assert!(!e.eval_bool(&drop));
    }

    #[test]
    fn script_maps_status_with_switch() {
        // Map a vendor status string to a numeric code with a `switch` expression.
        let src = ScriptSource::Inline(
            r#"
            let code = switch body.status {
                "RUNNING" => 1,
                "IDLE" => 0,
                "FAULT" => -1,
                _ => 99,
            };
            #{ "statusCode": code }
            "#
            .into(),
        );
        let mut s = ScriptStage::build(&src, &engine(), &ScriptLoader::default(), &ctx()).unwrap();
        let out = s.process(pm(json!({ "status": "FAULT", "samples": [] })));
        assert_eq!(out[0].msg.body["statusCode"], json!(-1));
    }

    #[test]
    fn script_reduces_array_with_reduce() {
        // Sum an array value with `reduce` (seed 0.0).
        let src =
            ScriptSource::Inline(r#"#{ "total": value.reduce(|a, v| a + v, 0.0) }"#.into());
        let mut s = ScriptStage::build(&src, &engine(), &ScriptLoader::default(), &ctx()).unwrap();
        let out = s.process(pm(json!({ "samples": [{ "value": [1.5, 2.5, 4.0], "quality": "GOOD" }] })));
        assert_eq!(out[0].msg.body["total"], json!(8.0));
    }

    #[test]
    fn script_computes_deltas_over_samples() {
        // Rate-of-change: the delta between each pair of consecutive samples, via a range loop.
        let src = ScriptSource::Inline(
            r#"
            let deltas = [];
            for i in 1..samples.len() {
                deltas.push(samples[i].value - samples[i - 1].value);
            }
            #{ "deltas": deltas }
            "#
            .into(),
        );
        let mut s = ScriptStage::build(&src, &engine(), &ScriptLoader::default(), &ctx()).unwrap();
        let out = s.process(pm(json!({
            "samples": [ { "value": 10.0 }, { "value": 13.0 }, { "value": 12.0 } ]
        })));
        assert_eq!(out[0].msg.body["deltas"], json!([3.0, -1.0]));
    }

    #[test]
    fn script_normalizes_non_southbound_payload() {
        // Reshape a vendor body into the southbound signal shape so downstream stages/sinks work.
        let src = ScriptSource::Inline(
            r#"
            #{
                "signal": #{ "id": body.dev, "name": body.metric },
                "samples": [ #{ "value": body.raw * 0.1, "quality": "GOOD" } ]
            }
            "#
            .into(),
        );
        let mut s = ScriptStage::build(&src, &engine(), &ScriptLoader::default(), &ctx()).unwrap();
        let out = s.process(pm(json!({ "dev": "pump-7", "metric": "vibration", "raw": 325 })));
        let b = &out[0].msg.body;
        assert_eq!(b["signal"]["id"], json!("pump-7"));
        assert_eq!(b["signal"]["name"], json!("vibration"));
        assert_eq!(b["samples"][0]["value"], json!(32.5));
    }

    #[test]
    fn script_derives_unit_with_helper_and_guard() {
        // A helper fn for the conversion + an early guard that drops a reading-less message.
        let src = ScriptSource::Inline(
            r#"
            fn to_fahrenheit(c) { c * 1.8 + 32.0 }
            if samples.is_empty() { return (); }
            #{ "signal": body.signal, "tempF": to_fahrenheit(value) }
            "#
            .into(),
        );
        let mut s = ScriptStage::build(&src, &engine(), &ScriptLoader::default(), &ctx()).unwrap();
        let out = s.process(pm(json!({ "signal": { "id": "t" }, "samples": [{ "value": 20.0 }] })));
        assert_eq!(out[0].msg.body["tempF"], json!(68.0));
        // No sample → the guard drops the message.
        assert_eq!(s.process(pm(json!({ "signal": { "id": "t" }, "samples": [] }))).len(), 0);
    }

    #[test]
    fn script_computes_rms_with_sqrt() {
        // Root-mean-square across an array value — uses the float `.sqrt()` from Rhai's math package.
        let src = ScriptSource::Inline(
            r#"
            let sumsq = 0.0;
            for x in value { sumsq += x * x; }
            #{ "rms": (sumsq / value.len()).sqrt() }
            "#
            .into(),
        );
        let mut s = ScriptStage::build(&src, &engine(), &ScriptLoader::default(), &ctx()).unwrap();
        let out = s.process(pm(json!({ "samples": [{ "value": [3.0, 4.0], "quality": "GOOD" }] })));
        let rms = out[0].msg.body["rms"].as_f64().unwrap();
        assert!((rms - 3.535_533).abs() < 1e-4, "rms was {rms}");
    }

    #[test]
    fn script_reads_message_header() {
        // The whole envelope header is available — name/version/uuid/timestamp/correlation_id —
        // for provenance, dedup, tracing, or branching on the message type.
        let src = ScriptSource::Inline(
            r#"
            #{
                "name": header.name,
                "version": header.version,
                "corr": header.correlation_id,
                "hasUuid": header.uuid != "",
                "hasTs": header.timestamp != ""
            }
            "#
            .into(),
        );
        let m = MessageBuilder::new("ProcessedTelemetry", "2.0")
            .correlation_id("corr-123")
            .payload(json!({ "samples": [] }))
            .build();
        let mut s = ScriptStage::build(&src, &engine(), &ScriptLoader::default(), &ctx()).unwrap();
        let out = s.process(ProcMsg { topic: "t".into(), msg: m, recv_ms: now_ms() });
        let b = &out[0].msg.body;
        assert_eq!(b["name"], json!("ProcessedTelemetry"));
        assert_eq!(b["version"], json!("2.0"));
        assert_eq!(b["corr"], json!("corr-123"));
        assert_eq!(b["hasUuid"], json!(true));
        assert_eq!(b["hasTs"], json!(true));
    }

    #[test]
    fn filter_script_gates_on_message_type() {
        // A filter that keeps only a specific envelope type — routing by `header.name`.
        let e = RhaiEval::compile(
            &engine(),
            &loader_load(&ScriptSource::Inline(r#"header.name == "SouthboundSignalUpdate""#.into())),
            &ctx(),
        )
        .unwrap();
        let sig = ProcMsg {
            topic: "t".into(),
            msg: MessageBuilder::new("SouthboundSignalUpdate", "1.0").payload(json!({})).build(),
            recv_ms: now_ms(),
        };
        let proc = ProcMsg {
            topic: "t".into(),
            msg: MessageBuilder::new("ProcessedTelemetry", "1.0").payload(json!({})).build(),
            recv_ms: now_ms(),
        };
        assert!(e.eval_bool(&sig));
        assert!(!e.eval_bool(&proc));
    }

    // Resolve an inline ScriptSource to text for a direct RhaiEval compile in tests.
    fn loader_load(src: &ScriptSource) -> String {
        ScriptLoader::default().load(src).unwrap()
    }
}
