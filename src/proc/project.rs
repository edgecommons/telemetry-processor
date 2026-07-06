//! # `project` stage — reshape / whitelist the body
//!
//! `keep` retains a set of top-level body keys (the first segment of each listed path); `set`
//! overlays literal fields. With neither, the body passes through unchanged.

use serde_json::{Map, Value};
use smallvec::smallvec;

use crate::config::ProjectSpec;
use crate::proc::{Out, ProcMsg, Processor};

/// A `project` pipeline stage.
pub struct ProjectStage {
    keep: Option<Vec<String>>,
    set: Option<Map<String, Value>>,
}

impl ProjectStage {
    pub fn build(spec: &ProjectSpec) -> Self {
        Self { keep: spec.keep.clone(), set: spec.set.clone() }
    }
}

impl Processor for ProjectStage {
    fn process(&mut self, mut m: ProcMsg) -> Out {
        let mut new_body = if let Some(keep) = &self.keep {
            let mut obj = Map::new();
            if let Value::Object(body) = &m.msg.body {
                for path in keep {
                    let key = path.split('.').next().unwrap_or(path);
                    if !obj.contains_key(key) {
                        if let Some(v) = body.get(key) {
                            obj.insert(key.to_string(), v.clone());
                        }
                    }
                }
            }
            Value::Object(obj)
        } else {
            m.msg.body.clone()
        };

        if let Some(set) = &self.set {
            if let Value::Object(o) = &mut new_body {
                for (k, v) in set {
                    o.insert(k.clone(), v.clone());
                }
            }
        }

        m.msg.body = new_body;
        smallvec![m]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proc::now_ms;
    use edgecommons::messaging::message::MessageBuilder;
    use serde_json::json;

    fn msg() -> ProcMsg {
        let m = MessageBuilder::new("SouthboundSignalUpdate", "1.0")
            .payload(json!({ "signal": { "id": "t1", "name": "Temp" }, "samples": [1, 2], "noise": "drop me" }))
            .build();
        ProcMsg { topic: "t".into(), msg: m, recv_ms: now_ms() }
    }

    #[test]
    fn keep_whitelists_top_level_keys() {
        let mut s = ProjectStage::build(&ProjectSpec {
            keep: Some(vec!["signal.id".into(), "samples".into()]),
            set: None,
        });
        let out = s.process(msg());
        let body = &out[0].msg.body;
        assert!(body.get("signal").is_some());
        assert!(body.get("samples").is_some());
        assert!(body.get("noise").is_none());
    }

    #[test]
    fn set_overlays_literals() {
        let mut set = Map::new();
        set.insert("origin".into(), json!("processor"));
        let mut s = ProjectStage::build(&ProjectSpec { keep: None, set: Some(set) });
        let out = s.process(msg());
        assert_eq!(out[0].msg.body["origin"], json!("processor"));
    }
}
