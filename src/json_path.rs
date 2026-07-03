//! # Minimal JSON-path resolver over a ggcommons [`Message`]
//!
//! Used by the filter / aggregate / project stages and the stream partition-key extractor to pull
//! values out of a message by a dotted path. Path roots: `body.` (the default when no known root
//! prefix is present), `identity.`, `tags.`, `header.`. A `[]` suffix on a segment spreads across
//! an array (matching any element). Examples: `body.signal.id`, `body.samples[].quality`,
//! `identity.device`, `tags.site`.
//!
//! ## UNS note — `identity.` is the `tags.thing` replacement
//! Under the unified namespace the source device no longer lives in `tags.thing`; it is the last
//! hierarchy level of the top-level `identity` element. The `identity.` root exposes the *source
//! publisher's* UNS identity — `identity.device`, `identity.component`, `identity.instance`,
//! `identity.path`, and `identity.hier[].level` / `identity.hier[].value` — for keying, filtering,
//! or provenance on who produced the message. (A stray inbound `tags.thing` is now an ordinary tag.)

use ggcommons::messaging::message::Message;
use serde_json::{json, Value};

/// Build the JSON view of a message's [`ggcommons::messaging::message::MessageIdentity`] used by
/// both the `identity.` json-path root and the script `identity` binding: the wire fields
/// (`hier`/`path`/`component`/`instance`) plus the computed `device` (the last hierarchy value,
/// which is not a serialized wire field). `Value::Null` when the message carries no identity.
pub fn identity_view(msg: &Message) -> Value {
    match &msg.identity {
        Some(id) => json!({
            "device": id.device(),
            "component": id.component(),
            "instance": id.instance(),
            "path": id.path(),
            "hier": id
                .hier()
                .iter()
                .map(|h| json!({ "level": h.level, "value": h.value }))
                .collect::<Vec<_>>(),
        }),
        None => Value::Null,
    }
}

/// Resolve a dotted path against `msg`, returning **all** matched leaf values (a `[]` segment can
/// yield several). Empty when nothing matches.
pub fn resolve_values(msg: &Message, path: &str) -> Vec<Value> {
    let mut acc = Vec::new();
    if let Some(rest) = path.strip_prefix("body.") {
        walk(&msg.body, rest, &mut acc);
    } else if path == "body" {
        acc.push(msg.body.clone());
    } else if let Some(rest) = path.strip_prefix("identity.") {
        // The UNS source-publisher identity (the `tags.thing` replacement).
        walk(&identity_view(msg), rest, &mut acc);
    } else if path == "identity" {
        acc.push(identity_view(msg));
    } else if let Some(rest) = path.strip_prefix("tags.") {
        if let Ok(v) = serde_json::to_value(&msg.tags) {
            walk(&v, rest, &mut acc);
        }
    } else if let Some(rest) = path.strip_prefix("header.") {
        if let Ok(v) = serde_json::to_value(&msg.header) {
            walk(&v, rest, &mut acc);
        }
    } else {
        // Bare path defaults into the body (e.g. `signal.id`, `samples[].quality`).
        walk(&msg.body, path, &mut acc);
    }
    acc
}

/// The first matched value, if any.
pub fn resolve_first(msg: &Message, path: &str) -> Option<Value> {
    resolve_values(msg, path).into_iter().next()
}

/// The first matched value rendered as a string (JSON strings unquoted; other scalars stringified).
pub fn resolve_first_string(msg: &Message, path: &str) -> Option<String> {
    resolve_first(msg, path).map(|v| value_to_string(&v))
}

/// Render a JSON scalar as a plain string (string → its contents; number/bool → its literal; other
/// → compact JSON).
pub fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn walk(v: &Value, path: &str, acc: &mut Vec<Value>) {
    if path.is_empty() {
        acc.push(v.clone());
        return;
    }
    let (seg, rest) = match path.split_once('.') {
        Some((a, b)) => (a, b),
        None => (path, ""),
    };
    let (key, spread) = match seg.strip_suffix("[]") {
        Some(k) => (k, true),
        None => (seg, false),
    };
    let target = if key.is_empty() { Some(v) } else { v.get(key) };
    let Some(target) = target else { return };
    if spread {
        if let Some(arr) = target.as_array() {
            for el in arr {
                walk(el, rest, acc);
            }
        }
    } else {
        walk(target, rest, acc);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ggcommons::messaging::message::{HierEntry, MessageBuilder, MessageIdentity};
    use serde_json::json;

    fn sample_msg() -> Message {
        // The UNS source identity replaces the old `tags.thing`: device `thing-1`, component
        // `opcua-adapter`, instance `kep1`. An ordinary business tag (`site`) is set too.
        let identity = MessageIdentity::new(
            vec![HierEntry { level: "device".into(), value: "thing-1".into() }],
            "opcua-adapter",
            Some("kep1".into()),
        )
        .unwrap();
        MessageBuilder::new("SouthboundSignalUpdate", "1.0")
            .identity(identity)
            .tag("site", json!("factory-1"))
            .payload(json!({
                "device": { "adapter": "opcua", "instance": "inst1" },
                "signal": { "id": "ns=3;i=1001", "name": "Temp" },
                "samples": [
                    { "value": 21.5, "quality": "GOOD" },
                    { "value": 99.0, "quality": "BAD" }
                ]
            }))
            .build()
    }

    #[test]
    fn resolves_nested_body_path() {
        let m = sample_msg();
        assert_eq!(resolve_first_string(&m, "body.signal.id").as_deref(), Some("ns=3;i=1001"));
        assert_eq!(resolve_first_string(&m, "signal.name").as_deref(), Some("Temp"));
    }

    #[test]
    fn spreads_array_segment() {
        let m = sample_msg();
        let qualities = resolve_values(&m, "body.samples[].quality");
        assert_eq!(qualities.len(), 2);
        assert_eq!(qualities[0], json!("GOOD"));
        assert_eq!(qualities[1], json!("BAD"));
    }

    #[test]
    fn resolves_identity_root() {
        // The UNS replacement for `tags.thing`: the source publisher's device/component/instance.
        let m = sample_msg();
        assert_eq!(resolve_first_string(&m, "identity.device").as_deref(), Some("thing-1"));
        assert_eq!(resolve_first_string(&m, "identity.component").as_deref(), Some("opcua-adapter"));
        assert_eq!(resolve_first_string(&m, "identity.instance").as_deref(), Some("kep1"));
        assert_eq!(resolve_first_string(&m, "identity.path").as_deref(), Some("thing-1"));
        // The hierarchy is walkable too (last level's value is the device).
        assert_eq!(resolve_first_string(&m, "identity.hier[].level").as_deref(), Some("device"));
        assert_eq!(resolve_first_string(&m, "identity.hier[].value").as_deref(), Some("thing-1"));
    }

    #[test]
    fn resolves_tags_root() {
        // Ordinary business tags still resolve; `tags.thing` is gone (device is in `identity`).
        let m = sample_msg();
        assert_eq!(resolve_first_string(&m, "tags.site").as_deref(), Some("factory-1"));
        assert!(resolve_values(&m, "tags.thing").is_empty());
    }

    #[test]
    fn identity_root_empty_when_no_identity() {
        // A message built without a config-bound / explicit identity yields nothing under `identity.`.
        let m = MessageBuilder::new("X", "1.0").payload(json!({ "a": 1 })).build();
        assert!(resolve_values(&m, "identity.device").is_empty());
        assert_eq!(resolve_first(&m, "identity"), Some(Value::Null));
    }

    #[test]
    fn missing_path_yields_empty() {
        let m = sample_msg();
        assert!(resolve_values(&m, "body.nope.gone").is_empty());
    }
}
