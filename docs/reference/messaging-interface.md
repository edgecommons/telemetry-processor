# Reference — Messaging Interface

The topic/message contract: what the processor subscribes to, what it publishes, and where. For
configuration of routes and targets see [configuration.md](configuration.md); for value typing see
[data-types.md](data-types.md); for the design rationale see [explanation.md](../explanation.md).

The processor is a **transform-and-forward** stage on the ggcommons **Unified Namespace (UNS)**: it
subscribes to the fleet's telemetry (the `data` class), runs a per-route pipeline, and forwards each
result to one target. It is also — for free from the ggcommons library — a first-class **command**
citizen: it answers the built-in `cmd` verbs and its own custom verbs (see [Command
verbs](#command-verbs)), publishes `evt` health events, and emits a `metric` throughput metric.

## Unified Namespace (UNS) topic grammar

Every UNS topic is `ecv1/{device}/{component}/{instance}/{class}[/channel]` (rootless; a `site`
position appears between `ecv1` and `{device}` only under a multi-level `hierarchy` with
`topic.includeRoot`). The processor's own token is `telemetry-processor` (the sanitized short name
after the last `.` of `com.mbreissi.telemetry-processor`), so it lives at
`ecv1/{device}/telemetry-processor/{instance}/{class}[/channel]`.

The **eight classes**, and how the processor uses each:

| Class | Owner | Processor use |
|-------|-------|---------------|
| `data` | application | **Input** (subscribes the fleet's adapter data) **and output** (processed telemetry) |
| `evt` | application | **Output** — pipeline health events (see [Events](#events-evt)) and forwarded alarms |
| `cmd` | application | **Input** — the library command inbox (built-in + [custom verbs](#command-verbs)) |
| `app` | application | free-form; unused by default |
| `state` | **reserved** (library) | the automatic keepalive on `ecv1/{device}/telemetry-processor/main/state` |
| `metric` | **reserved** (library) | the processor's `metric/pipeline` throughput metric |
| `cfg` | **reserved** (library) | the effective-config publisher |
| `log` | **reserved** (library) | (library-owned) |

> **Reserved-class publish guard.** A component publish (`publish` / `publish_to_iot_core`) to a
> `state` / `metric` / `cfg` / `log` topic is **rejected** by the library — the reserved classes are
> written only through the library's own publishers. A route `publish.topic` must therefore target
> `data` / `evt` / `app`; the processor logs a WARN at startup if a resolved `publish.topic` lands on
> a reserved class (the publish would otherwise be silently dropped). Subscribing a reserved class to
> *read* it is always allowed.

## Envelope

All messages use the ggcommons UNS JSON envelope — `{ header, identity, tags, body }` — the same
envelope the adapters publish:

```jsonc
{
  "header":   { "name": "SouthboundSignalUpdate", "version": "1.0", "timestamp": "<ISO-8601>",
                "uuid": "…", "correlation_id": null },
  "identity": { "hier": [ { "level": "device", "value": "<device>" } ],
                "path": "<device>", "component": "opcua-adapter", "instance": "kep1" },
  "tags":     { "appId": "…", "site": "…", "shop": "…", "line": "…" },
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

The processor does not require a specific `header.name`: any JSON message that matches a route's
`subscribe` filter flows through (filters and scripts can act on any body).

<a id="envelope-tags-vs-the-signal"></a>
### Identity vs. envelope `tags` vs. the *signal* — three different things

- **The `identity` element** — the top-level UNS identity of the **publisher**: `hier` (the hierarchy
  levels, the last of which is the *device*), the precomputed `path`, the `component` token, and the
  per-message `instance`. **This is where the source device lives** — the old `tags.thing` is
  **removed**. Pipelines read it via the [`identity.` JSON path](#identity-json-path)
  (`identity.device` / `identity.component` / `identity.instance` / `identity.path`) and scripts read
  the `identity` binding. This is the correct key for "which device/adapter produced this reading".
- **Envelope `tags`** — the ggcommons message-envelope metadata map: an *open* set of key/values that
  ride on every message (`appId`, `site`, `shop`, `line`, or any custom key). Opaque business
  metadata: exposed to scripts as the `tags` binding, usable in topic templates, and landed by the
  file sink's default projection in one JSON column. (A stray inbound `thing` key is now just an
  ordinary tag — it is no longer special.)
- **The *signal*** — one southbound **data point** (an OPC UA node, a Modbus register, …) carried in
  `body.signal` (`{ id, name, address }`) with its readings in `body.samples[]`. Historically called
  a "tag" in the OPC UA / historian world; the ggcommons contract calls it a **signal**.

<a id="identity-json-path"></a>
### The `identity.` JSON path (the `tags.thing` replacement)

Filter `field`, aggregate/sample `by`, the route `key`, and stream `partitionKey` all take a dotted
[key path](configuration.md#key-paths). Alongside `body.` / `tags.` / `header.` there is now an
`identity.` root exposing the source publisher's UNS identity:

| Path | Value |
|------|-------|
| `identity.device` | the source device (last hierarchy value) — the `tags.thing` replacement |
| `identity.component` | the source component token (e.g. `opcua-adapter`) |
| `identity.instance` | the source per-message instance token |
| `identity.path` | the `/`-joined hierarchy values |
| `identity.hier[].level` / `identity.hier[].value` | the hierarchy levels (array spread) |

Scripts get the same view as an `identity` scope binding (`identity.device`, `identity.component`, …).

## Subscribes

Each route subscribes to its configured `subscribe` filters on the **local bus** (MQTT/IPC). MQTT
wildcards (`+`, `#`) are allowed; `{ThingName}` / `{ComponentName}` / `tags{}` template variables are
substituted at startup (see [configuration.md](configuration.md#template-variables)). A filter shared
by several routes is subscribed **once** and fanned out to each route.

The **fleet consumer** for all southbound telemetry is the single UNS wildcard:

```text
ecv1/+/+/+/data/#          # every device / component / instance / signal on the data class
```

Scope it to specific adapters when you don't want the whole fleet, e.g.
`ecv1/+/opcua-adapter/+/data/#`. A typical input is a `SouthboundSignalUpdate` envelope
(`docs/SOUTHBOUND.md` §7) published by an adapter on `ecv1/{device}/{adapter}/{inst}/data/{signalPath}`
— but the processor is payload-agnostic (below).

> **The processor is payload-agnostic — it does not mandate this schema.** Any JSON body that matches
> a route's `subscribe` filter flows through; the `SouthboundSignalUpdate` shape is only a *convention*
> the built-in conveniences default to. Specifically, the shape is assumed **only** by: the `quality`
> filter shorthand, the default key `body.signal.id`, `body.samples[]` aggregation, and the file
> sink's **default** rows projection. Each is overridable — point a filter `field`/`script` at your
> own paths, set the route `key` and the aggregate [`value`](configuration.md#aggregate-stage) path,
> and declare a [rows user projection](data-types.md#rows-user-projection).

### Self-echo guard (loop safety)

Because the processor **consumes** the `data` class it also **republishes** onto (for `local`
targets), a naive fleet subscription would re-consume its own output → an amplifying loop. Two
mechanisms prevent this:

1. **Restamp on `local`.** For a `local` target the dispatcher restamps the output envelope's
   `identity` with the processor's own identity (instance = the route id). The local output is the
   processor's product, and this makes step 2 effective. (`northbound` / `stream` targets leave the
   source identity intact — they never re-enter the local bus, so provenance is preserved.)
2. **Drop own echoes.** The subscribe fan-out drops any inbound message whose `identity` device **and**
   component equal the processor's own. A re-consumed `local` output (now carrying the processor's
   identity) is discarded. Cross-device chaining still works — a *different* device does not match.

## Publishes

The output target is per route (`target`). Route outputs must land on a non-reserved class
(`data` / `evt` / `app`).

| `target` | Destination | Topic / key | Transport call |
|----------|-------------|-------------|----------------|
| `local` | local bus | `publish.topic` (default = the source topic); identity restamped to the processor | `publish(topic, msg)` |
| `northbound` | AWS IoT Core / northbound MQTT | `publish.topic` (default = the source topic), QoS from `publish.qos` | `publish_to_iot_core(topic, msg, qos)` |
| `stream:<name>` | a ggcommons durable stream | partition key from `publish.partitionKey` (default = the route `key`, i.e. `body.signal.id`) | `streams().stream(name).append(record)` |

- Set an explicit `publish.topic` to a UNS `data`/`evt`/`app` topic template, e.g.
  `ecv1/{ThingName}/telemetry-processor/main/data/downsampled` or
  `ecv1/{ThingName}/telemetry-processor/main/evt/alarms`. Templates are resolved at startup.
- **`northbound`** publishes to IoT Core via the mqttproxy with `qos` = `atLeastOnce` (default) or
  `atMostOnce`.
- **`stream:<name>`** appends the serialized message as one record; the stream's configured sink
  (kinesis/kafka/file) delivers it asynchronously. Forwarding errors are logged and tallied (a
  `stream-unavailable` `evt` fires), never propagated — use a durable `stream:` target for no-loss
  output.

## Aggregate output (`ProcessedTelemetry`)

An `aggregate` stage emits one message per `(key, window)` with `header.name = "ProcessedTelemetry"`
(other envelope fields inherited from the window's first message — including the source `identity`,
except on a `local` target where it is restamped):

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

## Command verbs

The processor subscribes its own command inbox `ecv1/{device}/telemetry-processor/main/cmd/#` (wired
automatically by the library). A `cmd` request whose `header.reply_to` is set gets a structured reply
`{"ok": true, "result": …}` or `{"ok": false, "error": {"code", "message"}}`; a request without
`reply_to` is fire-and-forget.

**Built-in verbs** (library-provided, cannot be shadowed):

| Verb | Result |
|------|--------|
| `ping` | `{ "status": "RUNNING", "uptimeSecs": n }` — liveness/echo |
| `reload-config` | re-fetch + re-apply the config from the active source → `{ "reloaded": true }` |
| `get-configuration` | the current **redacted effective config** → `{ "config": … }` |

**Custom verbs** (registered by the processor):

| Verb | Body | Result |
|------|------|--------|
| `get-stats` | — | `{ "routes": [ { id, in, out, dropped, streamAppends, publishFailures, queueDepth, paused } ] }` — per-route counters |
| `flush` | — | force-close every route's open **time** windows now → `{ "flushed": n }` (messages emitted). Count windows keep their count semantics. |
| `pause` | `{ "route"? }` | stop enqueuing to a route (or all routes when omitted) → `{ "paused": [ids] }` |
| `resume` | `{ "route"? }` | the inverse of `pause` → `{ "resumed": [ids] }` |

> **Known limitation.** The built-in `reload-config` hot-swaps the config snapshot but the routes are
> wired once at startup, so a route topology change needs a component restart (a dynamic
> `reload-routes` rebuild is a documented follow-up). `pause`/`resume` and `flush` operate on the
> already-wired routes.

## Events (`evt`)

The processor publishes rate-limited health events on `ecv1/{device}/telemetry-processor/main/evt/<channel>`
(a non-reserved class; subscribe the fleet with `ecv1/+/+/+/evt/#`):

| Channel | When | Body |
|---------|------|------|
| `queue-overflow` | a route's worker queue was full and a message was dropped (backpressure) | `{ "route" }` |
| `route-error` | a `local`/`northbound` forward failed | `{ "route", "topic", "error" }` |
| `stream-unavailable` | a `stream:<name>` target is down / its append failed | `{ "route", "stream", "error" }` |

Each channel is coalesced (at most one event per channel per cooldown window) so a sustained fault
can't storm the bus. Routes 2 (`alarms-northbound` in the recipe) can also forward alarm-flagged
readings northbound as `evt/alarms`.

## Metrics (`metric`)

With `metricEmission.target: "messaging"` the processor emits a `pipeline` metric every 30 s on
`ecv1/{device}/telemetry-processor/main/metric/pipeline` (subscribe `ecv1/+/+/+/metric/#`), carrying
the summed per-interval deltas of the route counters: `messagesIn`, `messagesOut`, `messagesDropped`,
`streamAppends`, `publishFailures`. Per-route detail is available on demand via the `get-stats`
command. System measures (CPU/memory/…) are emitted automatically by the heartbeat as the `sys`
metric.

## Topic-template variables

Resolved (once, at startup) into `subscribe` filters and `publish.topic`:

| Variable | Resolves to |
|----------|-------------|
| `{ThingName}` | the `-t/--thing` value (or platform identity) — the device |
| `{ComponentName}` / `{ComponentFullName}` | the component's short (`telemetry-processor`) / fully-qualified name |
| `{<key>}` | any key under top-level `tags` — `{site}`, `{appId}`, `{shop}`, `{line}`, or any custom key |

> `publish.partitionKey` is **not** a topic template — it is a [key path](configuration.md#key-paths)
> resolved against each message (default `body.signal.id`).

## Channel guidance

Route each result to the right destination:

- **High-rate / bulk telemetry → `stream:<name>`** (durable buffer → Kinesis/Kafka/file). The channel
  for the firehose; the archive preserves the source `identity` for provenance.
- **Low-rate control/alarm data → `northbound`** (IoT Core, e.g. an `evt/alarms` topic) or **`local`**
  (re-publish on the bus as the `data` class for another local consumer).

Sizing a route's `maxQueue` generously and preferring a `stream:` target gives the strongest delivery
guarantee; `local`/`northbound` dispatch is fire-and-forget (failures are tallied + surfaced as
`evt`).
