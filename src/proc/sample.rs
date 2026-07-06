//! # `sample` stage — per-key downsampling
//!
//! Keep one message per key per time window (`everyMs`) or one in every N (`everyN`). State is a
//! per-key map owned by the single route worker, so it is lock-free. The key path defaults to the
//! route key (e.g. `body.signal.id`).

use std::collections::HashMap;

use smallvec::smallvec;

use crate::config::SampleSpec;
use crate::json_path::resolve_first_string;
use crate::proc::{Out, ProcMsg, Processor};

enum Mode {
    EveryMs(u64),
    EveryN(u64),
}

/// A `sample` pipeline stage.
pub struct SampleStage {
    mode: Mode,
    key_path: String,
    last_ms: HashMap<String, u64>,
    count: HashMap<String, u64>,
}

impl SampleStage {
    pub fn build(spec: &SampleSpec, route_key: &str) -> anyhow::Result<Self> {
        let mode = if let Some(ms) = spec.every_ms {
            Mode::EveryMs(ms.max(1))
        } else if let Some(n) = spec.every_n {
            Mode::EveryN(n.max(1))
        } else {
            anyhow::bail!("sample stage needs `everyMs` or `everyN`");
        };
        let key_path = spec.by.clone().unwrap_or_else(|| route_key.to_string());
        Ok(Self { mode, key_path, last_ms: HashMap::new(), count: HashMap::new() })
    }

    fn key(&self, m: &ProcMsg) -> String {
        resolve_first_string(&m.msg, &self.key_path).unwrap_or_default()
    }
}

impl Processor for SampleStage {
    fn process(&mut self, m: ProcMsg) -> Out {
        let key = self.key(&m);
        let keep = match self.mode {
            Mode::EveryMs(ms) => match self.last_ms.get(&key).copied() {
                Some(prev) if m.recv_ms.saturating_sub(prev) < ms => false,
                _ => {
                    self.last_ms.insert(key, m.recv_ms);
                    true
                }
            },
            Mode::EveryN(n) => {
                let c = self.count.entry(key).or_insert(0);
                let emit = *c % n == 0;
                *c += 1;
                emit
            }
        };
        if keep {
            smallvec![m]
        } else {
            smallvec![]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgecommons::messaging::message::MessageBuilder;
    use serde_json::json;

    fn msg(signal: &str, recv_ms: u64) -> ProcMsg {
        let m = MessageBuilder::new("SouthboundSignalUpdate", "1.0")
            .payload(json!({ "signal": { "id": signal } }))
            .build();
        ProcMsg { topic: "t".into(), msg: m, recv_ms }
    }

    #[test]
    fn every_n_keeps_one_in_n_per_key() {
        let spec = SampleSpec { every_n: Some(3), ..Default::default() };
        let mut s = SampleStage::build(&spec, "body.signal.id").unwrap();
        let kept: usize =
            (0..9).map(|i| s.process(msg("a", i)).len()).sum();
        assert_eq!(kept, 3); // i = 0, 3, 6
    }

    #[test]
    fn every_ms_is_per_key() {
        let spec = SampleSpec { every_ms: Some(1000), ..Default::default() };
        let mut s = SampleStage::build(&spec, "body.signal.id").unwrap();
        assert_eq!(s.process(msg("a", 0)).len(), 1); // first for a
        assert_eq!(s.process(msg("a", 500)).len(), 0); // within window
        assert_eq!(s.process(msg("a", 1000)).len(), 1); // window elapsed
        assert_eq!(s.process(msg("b", 500)).len(), 1); // different key, first
    }
}
