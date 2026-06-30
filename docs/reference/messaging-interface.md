# Reference — Messaging Interface

The topic/message contract: what the processor subscribes to, what it publishes, and where. For
configuration of routes and targets see [configuration.md](configuration.md); for value typing see
[data-types.md](data-types.md); for the design rationale see [explanation.md](../explanation.md).

The processor is a one-way **transform-and-forward** stage: it subscribes to local telemetry, runs a
per-route pipeline, and forwards each result to one target. It has **no request/reply / command
surface** — that stays with the southbound adapters.

## Envelope

All messages use the ggcommons JSON envelope (`header` + `tags` + `body`) — the same envelope the
adapters publish. The processor does not require a specific `header.name`: any JSON message that
matches a route's `subscribe` filter flows through (filters and scripts can act on any body).

## Subscribes

Each route subscribes to its configured `subscribe` filters on the **local bus** (MQTT/IPC). MQTT
wildcards (`+`, `#`) are allowed; `{ThingName}` / `{ComponentName}` / `tags{}` template variables are
substituted at startup (see [configuration.md](configuration.md#template-variables)). A filter shared
by several routes is subscribed **once** and fanned out to each route.

The expected input is a `SouthboundTagUpdate` envelope (`docs/SOUTHBOUND.md` §2):

```jsonc
{
  "header": { "name": "SouthboundTagUpdate", "version": "1.0", "timestamp": "<ISO-8601>",
              "uuid": "…", "correlation_id": null },
  "tags":   { "thing": "<thingName>", "appId": "…", "site": "…", "shop": "…", "line": "…" },
  "body": {
    "device":  { "adapter": "opcua", "instance": "<instanceId>", "endpoint": "opc.tcp://host:4840" },
    "tag":     { "id": "<canonical stable id>", "name": "<human label>", "address": { /* protocol-native */ } },
    "samples": [
      { "value": <any>, "quality": "GOOD|BAD|UNCERTAIN", "qualityRaw": "<native code>",
        "sourceTs": "<ISO-8601 UTC>", "serverTs": "<ISO-8601 UTC>" }
    ]
  }
}
```

> **Non-southbound messages still flow through.** A `filter`/`sample`/`script` stage can act on any
> JSON body; only the operations that read southbound paths (the `quality` shorthand, `body.tag.id`
> keying, `body.samples[]` aggregation) assume the shape. **Rows-mode file archiving** also expects
> the southbound shape — a payload without `body.samples` lands in a `_unmapped` raw file (see
> [data-types.md](data-types.md#raw-schema)).

## Publishes

The output target is per route (`target`).

| `target` | Destination | Topic / key | Transport call |
|----------|-------------|-------------|----------------|
| `local` | local bus | `publish.topic` (default = the source topic) | `publish(topic, msg)` |
| `northbound` | AWS IoT Core / northbound MQTT | `publish.topic` (default = the source topic), QoS from `publish.qos` | `publish_to_iot_core(topic, msg, qos)` |
| `stream:<name>` | a ggcommons durable stream | partition key from `publish.partitionKey` (default = the route `key`) | `streams().stream(name).append(record)` |

- **`local`** republishes the processed message; topic templates are resolved at startup.
- **`northbound`** publishes to IoT Core via the mqttproxy with `qos` = `atLeastOnce` (default) or
  `atMostOnce`.
- **`stream:<name>`** appends the serialized message as one record (partition key resolved per
  message); the stream's configured sink (kinesis/kafka/file) delivers it asynchronously. Forwarding
  errors are logged, never propagated (telemetry is best-effort at the dispatch edge — use a durable
  `stream:` target for no-loss output).

## Aggregate output (`ProcessedTelemetry`)

An `aggregate` stage emits one message per `(key, window)` with `header.name = "ProcessedTelemetry"`
(other envelope fields inherited from the window's first message):

```jsonc
"body": {
  "tag": { "id": "<key>", … },              // the source tag identity, where present
  "samples": [ { "value": <primary>, "quality": "GOOD" } ],
  "agg": { "avg": 20.0, "max": 30.0, "min": 10.0, "count": 3, "last": 30.0 },
  "window": { "startMs": 1719446400000, "endMs": 1719446405000, "count": 3 }
}
```

| Field | Meaning |
|-------|---------|
| `samples[0].value` | the **primary** reducer = the **first-listed** `fn`. Carried in `samples[]` so rows-mode file archiving lands a value in the typed value column. |
| `samples[0].quality` | always `"GOOD"`. |
| `agg.<fn>` | the full reducer set (one entry per configured `fn`). See [data-types.md](data-types.md#aggregate-agg-types) for value types. |
| `window.startMs` / `window.endMs` | window bounds (Unix ms). For a count window both equal the close time. |
| `window.count` | number of messages folded into the window. |

> Numeric reducers (`avg`/`max`/`min`/`sum`) are emitted only when ≥1 sample in the window was
> numeric; otherwise that reducer is `null`.

## Topic-template variables

Resolved (once, at startup) into `subscribe` filters and `publish.topic`:

| Variable | Resolves to |
|----------|-------------|
| `{ThingName}` | the `-t/--thing` value (or platform identity) |
| `{ComponentName}` / `{ComponentFullName}` | the component's short / fully-qualified name |
| `{<key>}` | any key under top-level `tags` — `{site}`, `{appId}`, `{shop}`, `{line}`, `{tag}` (if defined) |

> `publish.partitionKey` is **not** a topic template — it is a [key path](configuration.md#key-paths)
> resolved against each message (default `body.tag.id`).

## Channel guidance

The processor reuses the ggcommons message envelope and routes each result to the right of three
channels (`docs/TELEMETRY_PROCESSOR.md`, `docs/platform/DESIGN-channels.md`):

- **High-rate / bulk telemetry → `stream:<name>`** (durable buffer → Kinesis/Kafka/file). This is the
  channel for the firehose.
- **Low-rate control/alarm data → `northbound`** (IoT Core) or **`local`** (re-publish on the bus for
  another local consumer).

Sizing a route's `maxQueue` generously and preferring a `stream:` target gives the strongest
delivery guarantee; `local`/`northbound` dispatch is fire-and-forget.
