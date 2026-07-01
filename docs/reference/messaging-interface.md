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

<a id="envelope-tags-vs-the-signal"></a>
### Envelope `tags` vs the *signal* — two different things

The word "tag" is overloaded in edge telemetry, so this contract uses two distinct names:

- **Envelope `tags`** — the ggcommons message-envelope metadata map (`header` + **`tags`** + `body`).
  It is an *open* set of key/values that ride on **every** message regardless of payload — e.g.
  `thing`, `appId`, `site`, `shop`, `line`, but a deployment may use any keys. The processor treats it
  as opaque metadata: it is exposed to scripts as the `tags` binding, usable in topic templates, and
  the file sink's default projection lands the whole object in one JSON column.
- **The *signal*** — one southbound **data point** (an OPC UA node, a Modbus register, …) carried in
  `body.signal` (`{ id, name, address }`) with its readings in `body.samples[]`. This is what was
  historically called a "tag" in the OPC UA / historian world; the ggcommons contract calls it a
  **signal** to free the word "tag" for the envelope metadata above.

So `signalId` (`body.signal.id`) is the data point; `tags` is the envelope metadata. They are
unrelated, and the rename (`SouthboundSignalUpdate`, `body.signal`, `signalId`) touches only the data
point — the envelope `tags` keep their name.

## Subscribes

Each route subscribes to its configured `subscribe` filters on the **local bus** (MQTT/IPC). MQTT
wildcards (`+`, `#`) are allowed; `{ThingName}` / `{ComponentName}` / `tags{}` template variables are
substituted at startup (see [configuration.md](configuration.md#template-variables)). A filter shared
by several routes is subscribed **once** and fanned out to each route.

A typical input is a `SouthboundSignalUpdate` envelope (`docs/SOUTHBOUND.md` §2) — but see the note
below: the processor does not mandate it.

```jsonc
{
  "header": { "name": "SouthboundSignalUpdate", "version": "1.0", "timestamp": "<ISO-8601>",
              "uuid": "…", "correlation_id": null },
  "tags":   { "thing": "<thingName>", "appId": "…", "site": "…", "shop": "…", "line": "…" },
  "body": {
    "device":  { "adapter": "opcua", "instance": "<instanceId>", "endpoint": "opc.tcp://host:4840" },
    "signal":  { "id": "<canonical stable id>", "name": "<human label>", "address": { /* protocol-native */ } },
    "samples": [
      { "value": <any>, "quality": "GOOD|BAD|UNCERTAIN", "qualityRaw": "<native code>",
        "sourceTs": "<ISO-8601 UTC>", "serverTs": "<ISO-8601 UTC>" }
    ]
  }
}
```

> **The processor is payload-agnostic — it does not mandate this schema.** Any JSON body that matches
> a route's `subscribe` filter flows through; the `SouthboundSignalUpdate` shape above is only a
> *convention* the built-in conveniences default to. Specifically, the shape is assumed **only** by:
> the `quality` filter shorthand, the default key `body.signal.id`, `body.samples[]` aggregation, and
> the file sink's **default** rows projection. Each of those is overridable for an arbitrary payload —
> point a filter `field`/`script` at your own paths, set the route `key` and the aggregate
> [`value`](configuration.md#aggregate-stage) path, and declare a
> [rows user projection](data-types.md#rows-user-projection). With the **default** rows projection, a
> payload that lacks `body.samples` is never dropped — it lands in a `_unmapped` raw file (see
> [data-types.md](data-types.md#raw-schema)).

## Publishes

The output target is per route (`target`).

| `target` | Destination | Topic / key | Transport call |
|----------|-------------|-------------|----------------|
| `local` | local bus | `publish.topic` (default = the source topic) | `publish(topic, msg)` |
| `northbound` | AWS IoT Core / northbound MQTT | `publish.topic` (default = the source topic), QoS from `publish.qos` | `publish_to_iot_core(topic, msg, qos)` |
| `stream:<name>` | a ggcommons durable stream | partition key from `publish.partitionKey` (default = the route `key`, i.e. `body.signal.id`) | `streams().stream(name).append(record)` |

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
  "signal": { "id": "<key>", … },           // the source signal identity, where present
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
| `{<key>}` | any key under top-level `tags` — `{site}`, `{appId}`, `{shop}`, `{line}`, or any custom key |

> `publish.partitionKey` is **not** a topic template — it is a [key path](configuration.md#key-paths)
> resolved against each message (default `body.signal.id`).

## Channel guidance

The processor reuses the ggcommons message envelope and routes each result to the right of three
channels (`docs/TELEMETRY_PROCESSOR.md`, `docs/platform/DESIGN-channels.md`):

- **High-rate / bulk telemetry → `stream:<name>`** (durable buffer → Kinesis/Kafka/file). This is the
  channel for the firehose.
- **Low-rate control/alarm data → `northbound`** (IoT Core) or **`local`** (re-publish on the bus for
  another local consumer).

Sizing a route's `maxQueue` generously and preferring a `stream:` target gives the strongest
delivery guarantee; `local`/`northbound` dispatch is fire-and-forget.
