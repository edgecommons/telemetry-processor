//! # Processing engine — the `Processor` trait and the per-route pipeline
//!
//! A route's `pipeline` is an ordered list of [`Processor`] stages built from config. Each stage
//! transforms a stream of [`ProcMsg`]s: `process` handles each arriving message (0..N out), and
//! `on_tick` lets stateful stages (aggregate windows) flush on a timer. The route worker
//! ([`crate::proc::route`]) owns the pipeline in a single task, so per-key state is lock-free.

use std::sync::Arc;

use ggcommons::messaging::message::Message;
use rhai::Engine;
use smallvec::SmallVec;

use crate::config::{ScriptEngineKind, StageConfig, Window};

pub mod aggregate;
pub mod filter;
pub mod project;
pub mod route;
pub mod sample;
pub mod script;

/// A message flowing through a route pipeline: the source topic, the message, and the receive time.
#[derive(Clone)]
pub struct ProcMsg {
    pub topic: String,
    pub msg: Message,
    pub recv_ms: u64,
}

/// Stage output — usually 0 or 1 messages; an aggregate flush emits several (spills to the heap).
pub type Out = SmallVec<[ProcMsg; 1]>;

/// One pipeline stage. Built-ins and the Rhai stage implement this uniformly.
pub trait Processor: Send {
    /// Transform one arriving message into 0..N output messages.
    fn process(&mut self, m: ProcMsg) -> Out;

    /// Emit any time-driven output (e.g. a closed aggregation window). Default: nothing.
    fn on_tick(&mut self, _now_ms: u64) -> Out {
        SmallVec::new()
    }
}

/// An ordered chain of stages, run in a single task.
pub struct Pipeline {
    stages: Vec<Box<dyn Processor>>,
    /// Per-stage tick cadence hints (ms), collected at build time from time-windowed aggregates.
    tick_hints: Vec<u64>,
}

impl Pipeline {
    /// Build the pipeline from config. `route_key` is the default aggregation/sample key;
    /// `engine_kind` selects the script engine (Rhai or Lua) for this route's `filter`/`script`
    /// stages; `engine` is the shared Rhai engine and `ctx` the per-route runtime context (identity +
    /// route id) bound into every evaluation.
    pub fn build(
        stages: &[StageConfig],
        route_key: &str,
        engine_kind: ScriptEngineKind,
        engine: &Arc<Engine>,
        loader: &script::ScriptLoader,
        ctx: &Arc<script::ScriptContext>,
    ) -> anyhow::Result<Self> {
        let mut built: Vec<Box<dyn Processor>> = Vec::with_capacity(stages.len());
        let mut tick_hints: Vec<u64> = Vec::new();
        for sc in stages {
            let stage: Box<dyn Processor> = match sc {
                StageConfig::Filter(spec) => {
                    Box::new(filter::FilterStage::build(spec, engine_kind, engine, loader, ctx)?)
                }
                StageConfig::Sample(spec) => Box::new(sample::SampleStage::build(spec, route_key)?),
                StageConfig::Aggregate(spec) => {
                    if let Window::Time { ms } = Window::parse(&spec.window)? {
                        tick_hints.push(ms.max(1));
                    }
                    Box::new(aggregate::AggregateStage::build(spec, route_key)?)
                }
                StageConfig::Project(spec) => Box::new(project::ProjectStage::build(spec)),
                StageConfig::Script(src) => {
                    Box::new(script::ScriptStage::build(src, engine_kind, engine, loader, ctx)?)
                }
            };
            built.push(stage);
        }
        Ok(Self { stages: built, tick_hints })
    }

    /// The smallest tick cadence (ms) any stage needs, if any (the min aggregate window). `None`
    /// means no stage is time-driven, so the worker need not run a flush timer.
    pub fn min_tick_ms(&self) -> Option<u64> {
        self.tick_hints.iter().copied().min()
    }

    /// Run `input` through every stage. When `tick` is `Some(now)`, each stage also flushes its
    /// time-driven output. Upstream flush output flows downstream through later stages' `process`.
    pub fn run(&mut self, input: Out, tick: Option<u64>) -> Out {
        let mut carry = input;
        for stage in &mut self.stages {
            let mut out: Out = SmallVec::new();
            for m in carry.drain(..) {
                out.extend(stage.process(m));
            }
            if let Some(now) = tick {
                out.extend(stage.on_tick(now));
            }
            carry = out;
        }
        carry
    }
}

/// Current Unix time in milliseconds.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AggregateSpec, FilterSpec, SampleSpec, StageConfig};
    use ggcommons::messaging::message::MessageBuilder;
    use serde_json::json;

    fn engine() -> Arc<Engine> {
        Arc::new(Engine::new())
    }

    fn one(signal: &str, val: f64, q: &str, recv: u64) -> Out {
        let m = MessageBuilder::new("SouthboundSignalUpdate", "1.0")
            .payload(json!({ "signal": { "id": signal }, "samples": [{ "value": val, "quality": q }] }))
            .build();
        let mut s: Out = SmallVec::new();
        s.push(ProcMsg { topic: "t".into(), msg: m, recv_ms: recv });
        s
    }

    #[test]
    fn builds_and_runs_filter_then_count_aggregate() {
        let stages = vec![
            StageConfig::Filter(FilterSpec { quality: Some("GOOD".into()), ..Default::default() }),
            StageConfig::Aggregate(AggregateSpec {
                window: "2".into(),
                by: None,
                fns: vec!["count".into(), "avg".into()],
                value: None,
            }),
        ];
        let mut p = Pipeline::build(&stages, "body.signal.id", ScriptEngineKind::Rhai, &engine(), &script::ScriptLoader::default(), &Arc::new(script::ScriptContext::default())).unwrap();
        assert_eq!(p.min_tick_ms(), None, "a count window needs no flush timer");

        // BAD is filtered out; two GOODs fill the count=2 window and emit on the second.
        assert!(p.run(one("a", 10.0, "BAD", 1), None).is_empty());
        assert!(p.run(one("a", 20.0, "GOOD", 2), None).is_empty());
        let out = p.run(one("a", 30.0, "GOOD", 3), None);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].msg.body["agg"]["count"], json!(2));
        assert_eq!(out[0].msg.body["agg"]["avg"], json!(25.0));
    }

    #[test]
    fn time_window_flushes_on_tick_through_pipeline() {
        let stages = vec![
            StageConfig::Sample(SampleSpec { every_ms: Some(0), ..Default::default() }),
            StageConfig::Aggregate(AggregateSpec {
                window: "1s".into(),
                by: None,
                fns: vec!["count".into()],
                value: None,
            }),
        ];
        let mut p = Pipeline::build(&stages, "body.signal.id", ScriptEngineKind::Rhai, &engine(), &script::ScriptLoader::default(), &Arc::new(script::ScriptContext::default())).unwrap();
        assert_eq!(p.min_tick_ms(), Some(1000));
        p.run(one("a", 1.0, "GOOD", 100), None);
        let out = p.run(SmallVec::new(), Some(2000));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].msg.body["agg"]["count"], json!(1));
    }
}
