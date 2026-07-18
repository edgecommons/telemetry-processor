//! # Multi-signal `script` stage — stateful named inputs + explicit output
//!
//! The extended `script` stage form: the stage declares **named inputs**, each bound to one signal
//! by a selector (`device`/`component`/`instance` against the envelope identity, `signalId`/
//! `signalName` against `body.signal`, and/or an MQTT-style `topic` filter). The stage caches the
//! latest value/quality/timestamps of every input and evaluates the script when a matched input's
//! **value or quality changes**, binding a consistent snapshot of all inputs as `inputs` and the
//! firing input as `trigger` (alongside the ordinary per-message bindings for the triggering
//! message). The script does not run until every `required` input has been initialized.
//!
//! Cached state is **partitioned by the source device** (the envelope identity's deepest hierarchy
//! value), so two devices publishing the same signal ids can never contaminate each other's
//! snapshot. Identity-based selectors only match messages that carry an envelope identity;
//! identity-less sources are selected by explicit `topic` filters and share one partition. State is
//! in-memory and restart-empty: after a restart the stage deterministically re-awaits every
//! required input before the first evaluation.
//!
//! With an `output` configured, each successful evaluation is published as a **new** EdgeCommons
//! envelope on `output.topic`: the body is the script result, the producer identity is the
//! processor's own (instance = route id), and `correlation_id` carries the triggering message's
//! `uuid` for provenance. The triggering input message itself is consumed, never republished.
//! Without an `output`, the result replaces the triggering message's body in place (the classic
//! single-message `script` behavior). A message that matches no input is consumed silently.

use std::collections::HashMap;
use std::sync::Arc;

use edgecommons::messaging::message::MessageBuilder;
use edgecommons::messaging::topic_matches;
use rhai::Engine;
use serde_json::{json, Map, Value};
use smallvec::smallvec;

use crate::config::{InputSelector, OutputSpec, ScriptEngineKind, ScriptSpec};
use crate::proc::script::{build_engine, MultiBindings, ScriptContext, ScriptEngine, ScriptLoader};
use crate::proc::{Out, ProcMsg, Processor};

/// Default envelope header name/version for configured-output result messages.
const DEFAULT_OUTPUT_NAME: &str = "ScriptResult";
const DEFAULT_OUTPUT_VERSION: &str = "1.0";

/// One configured input: its name, selector, and whether it gates the first evaluation.
struct CompiledInput {
    name: String,
    sel: InputSelector,
    required: bool,
}

/// The cached latest observation of one input within one partition.
#[derive(Clone)]
struct InputEntry {
    value: Value,
    quality: String,
    /// The source timestamp (the first sample's `timestamp`), when the payload carries one.
    timestamp: Option<Value>,
    recv_ms: u64,
    topic: String,
}

impl InputEntry {
    fn to_json(&self) -> Value {
        json!({
            "value": self.value,
            "quality": self.quality,
            "timestamp": self.timestamp,
            "recvMs": self.recv_ms,
            "topic": self.topic,
        })
    }
}

/// The stateful multi-signal `script` stage (also the carrier of the explicit-output behavior for
/// an output-only spec with no `inputs`).
pub struct MultiScriptStage {
    eval: Box<dyn ScriptEngine>,
    /// Name-ordered (BTreeMap config order), so multi-match trigger resolution is deterministic.
    inputs: Vec<CompiledInput>,
    output: Option<OutputSpec>,
    ctx: Arc<ScriptContext>,
    /// partition (source device) → input name → latest entry.
    state: HashMap<String, HashMap<String, InputEntry>>,
}

impl MultiScriptStage {
    pub fn build(
        spec: &ScriptSpec,
        kind: ScriptEngineKind,
        engine: &Arc<Engine>,
        loader: &ScriptLoader,
        ctx: &Arc<ScriptContext>,
    ) -> anyhow::Result<Self> {
        let text = loader.load(&spec.script_source()?)?;
        let mut inputs: Vec<CompiledInput> = Vec::new();
        if let Some(map) = &spec.inputs {
            anyhow::ensure!(!map.is_empty(), "script `inputs` must not be empty when present");
            for (name, sel) in map {
                validate_selector(name, sel)?;
                inputs.push(CompiledInput {
                    name: name.clone(),
                    sel: sel.clone(),
                    required: sel.required.unwrap_or(true),
                });
            }
            for i in 0..inputs.len() {
                for j in (i + 1)..inputs.len() {
                    anyhow::ensure!(
                        !same_selector(&inputs[i].sel, &inputs[j].sel),
                        "script inputs '{}' and '{}' have identical selectors — each input must \
                         select a distinct signal",
                        inputs[i].name,
                        inputs[j].name
                    );
                }
            }
        }
        if let Some(out) = &spec.output {
            anyhow::ensure!(
                !out.topic.trim().is_empty(),
                "script `output.topic` must be non-empty"
            );
        }
        Ok(Self {
            eval: build_engine(kind, &text, engine, ctx)?,
            inputs,
            output: spec.output.clone(),
            ctx: ctx.clone(),
            state: HashMap::new(),
        })
    }

    /// Wrap a successful result: a new envelope on the configured output topic, or the in-place
    /// body replacement on the triggering message when no output is configured.
    fn finish(&self, body: Value, mut m: ProcMsg) -> Out {
        match &self.output {
            Some(out) => {
                let mut b = MessageBuilder::new(
                    out.name.as_deref().unwrap_or(DEFAULT_OUTPUT_NAME),
                    out.version.as_deref().unwrap_or(DEFAULT_OUTPUT_VERSION),
                )
                .payload(body)
                // Provenance: correlate the derived output to the triggering message.
                .correlation_id(m.msg.header.uuid.clone());
                if let Some(id) = &self.ctx.identity {
                    b = b.identity(id.clone());
                }
                smallvec![ProcMsg { topic: out.topic.clone(), msg: b.build(), recv_ms: m.recv_ms }]
            }
            None => {
                m.msg.body = body;
                smallvec![m]
            }
        }
    }
}

impl Processor for MultiScriptStage {
    fn process(&mut self, m: ProcMsg) -> Out {
        // Output-only form (no `inputs`): evaluate every message, wrap the result.
        if self.inputs.is_empty() {
            return match self.eval.eval_body(&m) {
                Some(body) => self.finish(body, m),
                None => smallvec![],
            };
        }

        // 1. Which inputs does this message bind? None → consumed, contributes nothing.
        let matched: Vec<usize> = self
            .inputs
            .iter()
            .enumerate()
            .filter(|(_, ci)| selector_matches(&ci.sel, &m))
            .map(|(i, _)| i)
            .collect();
        if matched.is_empty() {
            return smallvec![];
        }

        // 2. Update the cached entries; only a changed value/quality (or first init) fires.
        let entry = extract_entry(&m);
        let part = self.state.entry(partition_key(&m)).or_default();
        let mut changed = false;
        for &i in &matched {
            let name = &self.inputs[i].name;
            if let Some(prev) = part.get_mut(name) {
                if prev.value == entry.value && prev.quality == entry.quality {
                    // Unchanged: refresh the timestamps, do not re-evaluate.
                    prev.timestamp = entry.timestamp.clone();
                    prev.recv_ms = entry.recv_ms;
                    prev.topic = entry.topic.clone();
                } else {
                    *prev = entry.clone();
                    changed = true;
                }
            } else {
                tracing::debug!(input = %name, "multi-signal input initialized");
                part.insert(name.clone(), entry.clone());
                changed = true;
            }
        }
        if !changed {
            return smallvec![];
        }

        // 3. Gate: every required input must be initialized before the first evaluation.
        let missing: Vec<&str> = self
            .inputs
            .iter()
            .filter(|ci| ci.required && !part.contains_key(&ci.name))
            .map(|ci| ci.name.as_str())
            .collect();
        if !missing.is_empty() {
            tracing::debug!(missing = ?missing, "multi-signal evaluation deferred: awaiting inputs");
            return smallvec![];
        }

        // 4. Evaluate with a consistent snapshot + the trigger view. On a multi-input match the
        //    trigger is the first matching input in name order (deterministic).
        let mut snapshot = Map::new();
        for ci in &self.inputs {
            if let Some(e) = part.get(&ci.name) {
                snapshot.insert(ci.name.clone(), e.to_json());
            }
        }
        let trigger_name = &self.inputs[matched[0]].name;
        let mut trigger = entry.to_json();
        trigger["name"] = json!(trigger_name);
        let bindings = MultiBindings { inputs: Value::Object(snapshot), trigger };
        match self.eval.eval_body_with(&m, Some(&bindings)) {
            Some(body) => self.finish(body, m),
            None => smallvec![],
        }
    }
}

/// A selector must discriminate on at least one signal-identifying field.
fn validate_selector(name: &str, sel: &InputSelector) -> anyhow::Result<()> {
    anyhow::ensure!(
        sel.signal_id.is_some() || sel.signal_name.is_some() || sel.topic.is_some(),
        "script input '{name}': selector needs at least one of `signalId`, `signalName`, `topic`"
    );
    if let Some(t) = &sel.topic {
        anyhow::ensure!(!t.trim().is_empty(), "script input '{name}': `topic` must be non-empty");
    }
    Ok(())
}

/// Selector equality for the duplicate-input check (ignores `required`).
fn same_selector(a: &InputSelector, b: &InputSelector) -> bool {
    InputSelector { required: None, ..a.clone() } == InputSelector { required: None, ..b.clone() }
}

/// Does `m` bind to this selector? Identity-based fields match only against a present envelope
/// identity (an identity-less message never matches them).
fn selector_matches(sel: &InputSelector, m: &ProcMsg) -> bool {
    if sel.device.is_some() || sel.component.is_some() || sel.instance.is_some() {
        let Some(id) = &m.msg.identity else { return false };
        if sel.device.as_deref().is_some_and(|d| id.device() != d) {
            return false;
        }
        if sel.component.as_deref().is_some_and(|c| id.component() != c) {
            return false;
        }
        if sel.instance.as_deref().is_some_and(|i| id.instance() != Some(i)) {
            return false;
        }
    }
    let signal = m.msg.body.get("signal");
    if let Some(sid) = sel.signal_id.as_deref() {
        if signal.and_then(|s| s.get("id")).and_then(Value::as_str) != Some(sid) {
            return false;
        }
    }
    if let Some(sname) = sel.signal_name.as_deref() {
        if signal.and_then(|s| s.get("name")).and_then(Value::as_str) != Some(sname) {
            return false;
        }
    }
    if let Some(f) = sel.topic.as_deref() {
        if !topic_matches(f, &m.topic) {
            return false;
        }
    }
    true
}

/// The state-partition key: the source device from the envelope identity. Identity-less messages
/// share one partition (select them with explicit `topic` filters).
fn partition_key(m: &ProcMsg) -> String {
    m.msg.identity.as_ref().map(|id| id.device().to_string()).unwrap_or_default()
}

/// The observation carried by one message: the first sample's value/quality/timestamp
/// (`SouthboundSignalUpdate` shape), falling back to the whole body for non-sample payloads.
fn extract_entry(m: &ProcMsg) -> InputEntry {
    let first = m.msg.body.get("samples").and_then(Value::as_array).and_then(|a| a.first());
    match first {
        Some(s) => InputEntry {
            value: s.get("value").cloned().unwrap_or(Value::Null),
            quality: s.get("quality").and_then(Value::as_str).unwrap_or("").to_string(),
            timestamp: s.get("timestamp").cloned(),
            recv_ms: m.recv_ms,
            topic: m.topic.clone(),
        },
        None => InputEntry {
            value: m.msg.body.clone(),
            quality: String::new(),
            timestamp: None,
            recv_ms: m.recv_ms,
            topic: m.topic.clone(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::config::ScriptStageSpec;
    use crate::proc::now_ms;
    use edgecommons::messaging::message::{HierEntry, MessageBuilder, MessageIdentity};
    use serde_json::json;

    fn identity(device: &str, component: &str, instance: Option<&str>) -> MessageIdentity {
        MessageIdentity::new(
            vec![HierEntry { level: "device".into(), value: device.into() }],
            component,
            instance.map(String::from),
        )
        .unwrap()
    }

    /// A `SouthboundSignalUpdate`-shaped message from `device` for `signal` with one sample.
    fn signal_msg(device: &str, signal: &str, value: Value, quality: &str) -> ProcMsg {
        let m = MessageBuilder::new("SouthboundSignalUpdate", "1.0")
            .identity(identity(device, "opcua-adapter", Some("kep1")))
            .payload(json!({
                "signal": { "id": signal, "name": signal },
                "samples": [{ "value": value, "quality": quality, "timestamp": "2026-07-18T12:00:00Z" }]
            }))
            .build();
        ProcMsg {
            topic: format!("ecv1/{device}/opcua-adapter/kep1/data/{signal}"),
            msg: m,
            recv_ms: now_ms(),
        }
    }

    fn spec_from(v: Value) -> ScriptSpec {
        match serde_json::from_value::<ScriptStageSpec>(v).unwrap() {
            ScriptStageSpec::Spec(s) => s,
            _ => panic!("expected the object form"),
        }
    }

    fn build(spec: &ScriptSpec) -> MultiScriptStage {
        try_build(spec).unwrap()
    }

    fn try_build(spec: &ScriptSpec) -> anyhow::Result<MultiScriptStage> {
        let ctx = Arc::new(ScriptContext {
            identity: Some(identity("edge-proc", "telemetry-processor", Some("r1"))),
            route_id: "r1".into(),
            ..Default::default()
        });
        MultiScriptStage::build(
            spec,
            ScriptEngineKind::Rhai,
            &Arc::new(Engine::new()),
            &ScriptLoader::default(),
            &ctx,
        )
    }

    /// A two-input spec (a + b) whose script returns both values plus the trigger name.
    fn two_input_spec(output: bool) -> ScriptSpec {
        let mut v = json!({
            "source": r#"#{ "a": inputs.a.value, "b": inputs.b.value, "by": trigger.name }"#,
            "inputs": {
                "a": { "device": "gw-1", "signalId": "A" },
                "b": { "device": "gw-1", "signalId": "B" }
            }
        });
        if output {
            v["output"] = json!({ "topic": "ecv1/gw-1/telemetry-processor/r1/data/derived" });
        }
        spec_from(v)
    }

    #[test]
    fn does_not_evaluate_until_all_required_inputs_initialized() {
        let mut s = build(&two_input_spec(false));
        // Only `a` has arrived → no evaluation, out-of-order init is fine.
        assert!(s.process(signal_msg("gw-1", "A", json!(1), "GOOD")).is_empty());
        // `b` arrives → the snapshot is complete and the script runs once.
        let out = s.process(signal_msg("gw-1", "B", json!(2), "GOOD"));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].msg.body, json!({ "a": 1, "b": 2, "by": "b" }));
    }

    #[test]
    fn any_input_update_reevaluates_with_the_full_snapshot() {
        let mut s = build(&two_input_spec(false));
        s.process(signal_msg("gw-1", "A", json!(1), "GOOD"));
        s.process(signal_msg("gw-1", "B", json!(2), "GOOD"));
        // Updating `a` re-fires with b's latest cached value; the trigger is identified.
        let out = s.process(signal_msg("gw-1", "A", json!(10), "GOOD"));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].msg.body, json!({ "a": 10, "b": 2, "by": "a" }));
    }

    #[test]
    fn repeated_unchanged_values_do_not_reevaluate() {
        let mut s = build(&two_input_spec(false));
        s.process(signal_msg("gw-1", "A", json!(1), "GOOD"));
        s.process(signal_msg("gw-1", "B", json!(2), "GOOD"));
        // The same value + quality again → no evaluation.
        assert!(s.process(signal_msg("gw-1", "A", json!(1), "GOOD")).is_empty());
        // A quality change alone IS a change (scripts gate on bad quality themselves).
        let out = s.process(signal_msg("gw-1", "A", json!(1), "BAD"));
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn quality_and_timestamps_available_to_the_script() {
        let spec = spec_from(json!({
            "source": r#"#{ "q": inputs.a.quality, "ts": inputs.a.timestamp,
                            "recv": inputs.a.recvMs > 0, "tq": trigger.quality }"#,
            "inputs": { "a": { "device": "gw-1", "signalId": "A" } }
        }));
        let mut s = build(&spec);
        let out = s.process(signal_msg("gw-1", "A", json!(5), "BAD"));
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].msg.body,
            json!({ "q": "BAD", "ts": "2026-07-18T12:00:00Z", "recv": true, "tq": "BAD" })
        );
    }

    #[test]
    fn configured_output_publishes_a_new_envelope_and_consumes_the_trigger() {
        let mut s = build(&two_input_spec(true));
        s.process(signal_msg("gw-1", "A", json!(1), "GOOD"));
        let trigger = signal_msg("gw-1", "B", json!(2), "GOOD");
        let trigger_uuid = trigger.msg.header.uuid.clone();
        let out = s.process(trigger);
        // Exactly one message: the derived output — the trigger is not republished.
        assert_eq!(out.len(), 1);
        let pm = &out[0];
        assert_eq!(pm.topic, "ecv1/gw-1/telemetry-processor/r1/data/derived");
        assert_eq!(pm.msg.body, json!({ "a": 1, "b": 2, "by": "b" }));
        // A valid envelope: the processor (instance = route id) is the producer…
        let id = pm.msg.identity.as_ref().unwrap();
        assert_eq!(id.device(), "edge-proc");
        assert_eq!(id.component(), "telemetry-processor");
        assert_eq!(id.instance(), Some("r1"));
        // …with default header name/version and trigger provenance via the correlation id.
        assert_eq!(pm.msg.header.name, "ScriptResult");
        assert_eq!(pm.msg.header.version, "1.0");
        assert_eq!(pm.msg.header.correlation_id, trigger_uuid);
        assert!(!pm.msg.header.uuid.is_empty() && pm.msg.header.uuid != trigger_uuid);
    }

    #[test]
    fn output_envelope_name_and_version_are_configurable() {
        let mut spec = two_input_spec(true);
        spec.output.as_mut().unwrap().name = Some("OeeSnapshot".into());
        spec.output.as_mut().unwrap().version = Some("2.0".into());
        let mut s = build(&spec);
        s.process(signal_msg("gw-1", "A", json!(1), "GOOD"));
        let out = s.process(signal_msg("gw-1", "B", json!(2), "GOOD"));
        assert_eq!(out[0].msg.header.name, "OeeSnapshot");
        assert_eq!(out[0].msg.header.version, "2.0");
    }

    #[test]
    fn in_place_mode_without_output_keeps_the_trigger_message() {
        let mut s = build(&two_input_spec(false));
        s.process(signal_msg("gw-1", "A", json!(1), "GOOD"));
        let trigger = signal_msg("gw-1", "B", json!(2), "GOOD");
        let (topic, uuid) = (trigger.topic.clone(), trigger.msg.header.uuid.clone());
        let out = s.process(trigger);
        // The classic in-place contract: same message/topic, new body.
        assert_eq!(out[0].topic, topic);
        assert_eq!(out[0].msg.header.uuid, uuid);
        assert_eq!(out[0].msg.body["a"], json!(1));
    }

    #[test]
    fn state_is_partitioned_by_source_device() {
        let mut spec = two_input_spec(false);
        // Fleet-style selectors: by signal id only, no device pin.
        spec.inputs = Some(BTreeMap::from([
            ("a".into(), InputSelector { signal_id: Some("A".into()), ..Default::default() }),
            ("b".into(), InputSelector { signal_id: Some("B".into()), ..Default::default() }),
        ]));
        let mut s = build(&spec);
        s.process(signal_msg("gw-1", "A", json!(1), "GOOD"));
        // gw-2 publishing the same signal ids must not complete gw-1's snapshot…
        assert!(s.process(signal_msg("gw-2", "B", json!(99), "GOOD")).is_empty());
        // …and gw-1's own B completes gw-1's partition with gw-1's values only.
        let out = s.process(signal_msg("gw-1", "B", json!(2), "GOOD"));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].msg.body, json!({ "a": 1, "b": 2, "by": "b" }));
        // gw-2's partition still awaits its own A.
        assert!(s.process(signal_msg("gw-2", "B", json!(100), "GOOD")).is_empty());
    }

    #[test]
    fn non_matching_messages_are_consumed() {
        let mut s = build(&two_input_spec(false));
        assert!(s.process(signal_msg("gw-1", "Other", json!(7), "GOOD")).is_empty());
        assert!(s.process(signal_msg("gw-9", "A", json!(7), "GOOD")).is_empty());
    }

    #[test]
    fn optional_inputs_do_not_gate_evaluation() {
        let spec = spec_from(json!({
            "source": r#"if "c" in inputs { #{ "c": inputs.c.value } } else { #{ "a": inputs.a.value } }"#,
            "inputs": {
                "a": { "device": "gw-1", "signalId": "A" },
                "c": { "device": "gw-1", "signalId": "C", "required": false }
            }
        }));
        let mut s = build(&spec);
        // The optional `c` is absent from the snapshot; the required `a` alone evaluates.
        let out = s.process(signal_msg("gw-1", "A", json!(3), "GOOD"));
        assert_eq!(out[0].msg.body, json!({ "a": 3 }));
        // Once `c` arrives it appears in the snapshot.
        let out = s.process(signal_msg("gw-1", "C", json!(4), "GOOD"));
        assert_eq!(out[0].msg.body, json!({ "c": 4 }));
    }

    #[test]
    fn topic_filter_selectors_match_identityless_messages() {
        let spec = spec_from(json!({
            "source": r#"#{ "v": inputs.raw.value }"#,
            "inputs": { "raw": { "topic": "plant/+/counter" } }
        }));
        let mut s = build(&spec);
        // A foreign message with no envelope identity, selected purely by topic filter.
        let m = MessageBuilder::new("X", "1.0").payload(json!({ "n": 41 })).build();
        let pm = ProcMsg { topic: "plant/line1/counter".into(), msg: m, recv_ms: now_ms() };
        let out = s.process(pm);
        // No samples array → the whole body is the value.
        assert_eq!(out[0].msg.body, json!({ "v": { "n": 41 } }));
    }

    #[test]
    fn output_only_spec_wraps_every_result() {
        let spec = spec_from(json!({
            "source": r#"#{ "doubled": value * 2 }"#,
            "output": { "topic": "ecv1/edge-proc/telemetry-processor/r1/data/doubled" }
        }));
        let mut s = build(&spec);
        let out = s.process(signal_msg("gw-1", "A", json!(21), "GOOD"));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].topic, "ecv1/edge-proc/telemetry-processor/r1/data/doubled");
        assert_eq!(out[0].msg.body, json!({ "doubled": 42 }));
        assert_eq!(out[0].msg.identity.as_ref().unwrap().instance(), Some("r1"));
    }

    #[test]
    fn build_rejects_bad_specs() {
        // A selector without any signal-identifying field.
        let s = spec_from(json!({
            "source": "1", "inputs": { "a": { "device": "gw-1" } },
        }));
        assert!(try_build(&s).is_err());
        // Two inputs with identical selectors (ambiguous binding).
        let s = spec_from(json!({
            "source": "1",
            "inputs": {
                "a": { "device": "gw-1", "signalId": "X" },
                "b": { "device": "gw-1", "signalId": "X" }
            },
        }));
        assert!(try_build(&s).is_err());
        // An empty inputs map.
        let s = spec_from(json!({ "source": "1", "inputs": {} }));
        assert!(try_build(&s).is_err());
        // An empty output topic.
        let s = spec_from(json!({ "source": "1", "output": { "topic": "  " } }));
        assert!(try_build(&s).is_err());
    }

    #[cfg(feature = "scripting-lua")]
    mod lua_tests {
        use super::*;

        /// The OEE shape from the design: named inputs, a Lua calculation, an explicit output.
        #[test]
        fn lua_oee_calculation_over_named_inputs() {
            let spec = spec_from(json!({
                "source": r#"
                    if inputs.running.value ~= true then return nil end
                    local avail = inputs.totalCount.value / inputs.plannedCount.value
                    return { oee = avail, by = trigger.name }
                "#,
                "inputs": {
                    "running":      { "device": "gw-fill-01", "signalId": "FillerRunning" },
                    "totalCount":   { "device": "gw-fill-01", "signalId": "TotalBottleCount" },
                    "plannedCount": { "device": "gw-fill-01", "signalId": "PlannedBottleCount" }
                },
                "output": { "topic": "ecv1/gw-fill-01/telemetry-processor/oee/data/current" }
            }));
            let ctx = Arc::new(ScriptContext {
                identity: Some(identity("edge-proc", "telemetry-processor", Some("oee"))),
                route_id: "oee".into(),
                ..Default::default()
            });
            let mut s = MultiScriptStage::build(
                &spec,
                ScriptEngineKind::Lua,
                &Arc::new(Engine::new()),
                &ScriptLoader::default(),
                &ctx,
            )
            .unwrap();
            assert!(s.process(signal_msg("gw-fill-01", "FillerRunning", json!(true), "GOOD")).is_empty());
            assert!(s.process(signal_msg("gw-fill-01", "PlannedBottleCount", json!(100), "GOOD")).is_empty());
            let out = s.process(signal_msg("gw-fill-01", "TotalBottleCount", json!(80), "GOOD"));
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].topic, "ecv1/gw-fill-01/telemetry-processor/oee/data/current");
            assert_eq!(out[0].msg.body["oee"], json!(0.8));
            assert_eq!(out[0].msg.body["by"], json!("totalCount"));

            // `running` flips false → the script returns nil → no output published.
            let none = s.process(signal_msg("gw-fill-01", "FillerRunning", json!(false), "GOOD"));
            assert!(none.is_empty());
        }
    }
}
