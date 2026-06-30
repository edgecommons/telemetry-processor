//! # Minimal JSON-path resolver over a ggcommons [`Message`]
//!
//! Used by the filter / aggregate / project stages and the stream partition-key extractor to pull
//! values out of a message by a dotted path. Path roots: `body.` (the default when no known root
//! prefix is present), `tags.`, `header.`. A `[]` suffix on a segment spreads across an array
//! (matching any element). Examples: `body.tag.id`, `body.samples[].quality`, `tags.site`.

use ggcommons::messaging::message::Message;
use serde_json::Value;

/// Resolve a dotted path against `msg`, returning **all** matched leaf values (a `[]` segment can
/// yield several). Empty when nothing matches.
pub fn resolve_values(msg: &Message, path: &str) -> Vec<Value> {
    let mut acc = Vec::new();
    if let Some(rest) = path.strip_prefix("body.") {
        walk(&msg.body, rest, &mut acc);
    } else if path == "body" {
        acc.push(msg.body.clone());
    } else if let Some(rest) = path.strip_prefix("tags.") {
        if let Ok(v) = serde_json::to_value(&msg.tags) {
            walk(&v, rest, &mut acc);
        }
    } else if let Some(rest) = path.strip_prefix("header.") {
        if let Ok(v) = serde_json::to_value(&msg.header) {
            walk(&v, rest, &mut acc);
        }
    } else {
        // Bare path defaults into the body (e.g. `tag.id`, `samples[].quality`).
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
    use ggcommons::messaging::message::MessageBuilder;
    use serde_json::json;

    fn sample_msg() -> Message {
        MessageBuilder::new("SouthboundTagUpdate", "1.0")
            .thing_name("thing-1")
            .payload(json!({
                "device": { "adapter": "opcua", "instance": "inst1" },
                "tag": { "id": "ns=3;i=1001", "name": "Temp" },
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
        assert_eq!(resolve_first_string(&m, "body.tag.id").as_deref(), Some("ns=3;i=1001"));
        assert_eq!(resolve_first_string(&m, "tag.name").as_deref(), Some("Temp"));
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
    fn resolves_tags_root() {
        let m = sample_msg();
        assert_eq!(resolve_first_string(&m, "tags.thing").as_deref(), Some("thing-1"));
    }

    #[test]
    fn missing_path_yields_empty() {
        let m = sample_msg();
        assert!(resolve_values(&m, "body.nope.gone").is_empty());
    }
}
