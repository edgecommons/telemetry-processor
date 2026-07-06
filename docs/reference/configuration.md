# Reference — Configuration

Complete reference for every configuration option of the **telemetry-processor** component. For *why*
these settings exist and how a route is wired, read [explanation.md](../explanation.md); for worked
configs, see [sample-configurations.md](../sample-configurations.md). The topic/message contract is in
[messaging-interface.md](messaging-interface.md); value typing is in [data-types.md](data-types.md).

## Config source & CLI

The component reads one JSON document from the `-c/--config` source and parses the standard edgecommons
CLI contract; the source and transport default by platform.

| Flag | Values | Notes |
|------|--------|-------|
| `-c/--config` | `FILE <path>` \| `ENV` \| `GG_CONFIG` \| `SHADOW` \| `CONFIG_COMPONENT` \| `CONFIGMAP` | Default from the resolved platform (below). |
| `--platform` | `GREENGRASS` \| `HOST` \| `KUBERNETES` \| `auto` | Default `auto` (auto-detected, always overridable). |
| `--transport` | `IPC` \| `MQTT [messaging_config.json]` | Defaults from the platform; `IPC` is valid only on `GREENGRASS`. |
| `-t/--thing` | `<name>` | IoT Thing name; resolves `{ThingName}` in templates (takes the full string). |

| Platform | Default `-c` source | Default `--transport` |
|----------|---------------------|-----------------------|
| `GREENGRASS` | `GG_CONFIG` | `IPC` |
| `HOST` | `FILE` | `MQTT` (dual-broker) |
| `KUBERNETES` | `CONFIGMAP` | `MQTT` |

Route settings live under `component`; the sibling sections (`tags`, `messaging`, `streaming`,
`logging`, `heartbeat`, `metricEmission`) are standard edgecommons sections.

## Top-level sections

| Section | Required | Purpose |
|---------|----------|---------|
| `component` | yes | Routes (`instances[]`) and their cross-route defaults (`global.defaults`) — this document. |
| `tags` | recommended | Site/asset identity (`appId`/`site`/`shop`/`line`); attached to messages and usable as topic template variables. |
| `messaging` | HOST/KUBERNETES | MQTT broker connection (or supply via `--transport MQTT <file>`). On GREENGRASS the transport is IPC. |
| `streaming` | only for `stream:` targets | Named durable streams + their sinks (kinesis/kafka/**file**), below. |
| `logging`, `heartbeat`, `metricEmission` | optional | Standard edgecommons sections. |

## `component.global.defaults`

Cross-route defaults overlaid by each route (`global ⊕ instance`, instance wins per key).

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `key` | string | `body.signal.id` | Default aggregation / sample / stream-partition key path (a [dotted path](#key-paths)). |
| `target` | string | — | Default `target` for a route that omits one (`local` \| `northbound` \| `stream:<name>`). |
| `scriptsDir` | string (template) | the process working dir | Base directory for `script` **file** references (`{"file": "rules/x.rhai"}`). A relative script path resolves against it; an absolute path is used as-is. Template-resolved at startup. See [Use an external script file](../how-to-guides.md#use-an-external-script-file). |
| `scriptEngine` | enum | `rhai` | Default engine for `filter`/`script` stages: `rhai` (pure-Rust, always available) or `lua` (Lua 5.4 — needs the `scripting-lua` build). Per-route `scriptEngine` overrides. See [Scripting — choosing an engine](../scripting.mdx#choosing-an-engine). |

## `component.instances[]` (one route)

Each entry is one independent route: subscribe → pipeline → target.

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `id` | string | **required** | Route id (logs only). |
| `subscribe` | string[] | **required (non-empty)** | Topic filters on the local bus. MQTT `+`/`#` wildcards allowed; each filter is template-resolved (below) at startup. |
| `pipeline` | stage[] | `[]` | Ordered processing stages (below). Empty = pass-through. |
| `target` | string | `global.defaults.target` | `local` \| `northbound` \| `stream:<name>`. Required if no global default. |
| `publish` | object | — | Output topic / partition key / QoS (below). |
| `key` | string | `global.defaults.key` ▸ `body.signal.id` | Route default key path for `sample`/`aggregate`/stream partitioning. |
| `maxQueue` | number | `256` | Per-route internal queue depth. **Drop-on-full**: when the route's worker can't keep up, new messages are dropped (logged at debug, tallied in `get-stats` `dropped`, and surfaced as a rate-limited `evt/warning/queue-overflow`). |
| `scriptEngine` | enum | `global.defaults.scriptEngine` ▸ `rhai` | Engine for this route's `filter`/`script` stages (`rhai` \| `lua`). The script *dialect* follows the engine. |

> Numeric fields accept an integer **or** an integer-valued float (Greengrass delivers config numbers
> as doubles).

### `instances[].publish`

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `topic` | string (template) | the source topic | Output topic for `local`/`northbound`. Template-resolved at startup. |
| `partitionKey` | string (path) | the route `key` | Partition-key path for `stream:<name>` (resolved per message). |
| `qos` | string | `atLeastOnce` | `northbound` only: `atLeastOnce` or `atMostOnce`. |

## Pipeline stages

A stage is an externally-tagged object — `{"filter": {…}}`, `{"sample": {…}}`, `{"aggregate": {…}}`,
`{"project": {…}}`, or `{"script": "<rhai>"}` / `{"script": {"file": "<path>"}}`. Stages run in
order; each transforms 0..N messages.

### `filter` — keep/drop whole messages

Exactly one form applies, checked in this order: `script` → `quality` → `field`.

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `script` | string \| `{file}` | — | Rhai/Lua boolean predicate (per the route's `scriptEngine`) over the [message view](#rhai-scope); keep when it returns `true`. Inline source, or `{"file": "rules/keep.rhai"}` — see [`script`](#script-stage). |
| `quality` | string | — | Shorthand: keep only when **every** `body.samples[].quality` equals this (and ≥1 sample exists). |
| `field` | string (path) | — | Built-in predicate path (supports `[]` array spread → any-element match). |
| `op` | string | `eq` | `eq` \| `ne` \| `gt` \| `lt` \| `ge` \| `le` \| `exists` \| `contains` (symbolic aliases `==`/`!=`/`>`/…/`>=` also parse). |
| `value` | any | `null` | Right-hand value for the comparison. Numbers and numeric strings compare numerically. |

> A `filter` with none of `script`/`quality`/`field` fails the route at build time.

### `sample` — per-key downsampling

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `everyMs` | number | — | Keep at most one message per key per this many ms (clamped to ≥1). |
| `everyN` | number | — | Keep one in every N per key (clamped to ≥1). |
| `by` | string (path) | the route `key` | Per-key path. |

> Needs exactly one of `everyMs` / `everyN`.

<a id="aggregate-stage"></a>
### `aggregate` — tumbling windowed reduction

Emits one [`ProcessedTelemetry`](messaging-interface.md#aggregate-output-processedtelemetry) per
`(key, window)` on close.

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `window` | string | **required** | Time window `"10s"` / `"500ms"`, or a bare record **count** `"100"`. |
| `by` | string (path) | the route `key` | Per-key path. |
| `fn` | string[] | **required (non-empty)** | Reducers: `avg` \| `max` \| `min` \| `sum` \| `count` \| `first` \| `last`. The **first** listed is the *primary* (lands in `samples[0].value`). |
| `value` | string (path) | `body.samples[].value` ▸ whole body | Path to the value(s) to fold (supports `[]` to spread an array). Defaults to every `body.samples[].value`, falling back to the whole body for a payload with no `samples`. **Set this for a non-`SouthboundSignalUpdate` payload** — e.g. `"body.temperature"`. |

> Time windows close on the worker flush tick or when a message for a newer window arrives; **count**
> windows close in-line when N is reached. Numeric reducers (`avg`/`max`/`min`/`sum`) skip
> non-numeric samples; `count` counts all; `first`/`last` keep the raw value.

### `project` — reshape / whitelist the body

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `keep` | string[] | — | Body paths to retain. **Only the first path segment (top-level body key) is kept** — e.g. `"signal.id"` retains the whole `signal` object. |
| `set` | object | — | Literal fields overlaid onto the body. |

> With neither `keep` nor `set`, the body passes through unchanged.

<a id="script-stage"></a>
### `script` — a transform (Rhai or Lua)

A program run per message that returns a new body map/table, or `()` (Rhai) / `nil` (Lua) to **drop**
the message. It runs in the route's [`scriptEngine`](#componentglobaldefaults) — the script *dialect*
follows the engine. The source is given **inline** or from an **external file**:

| Form | Meaning |
|------|---------|
| `{"script": "<source>"}` | Inline source. Good for a one-liner. |
| `{"script": {"file": "rules/x.rhai"}}` | Read the program from a `.rhai`/`.lua` file at startup. The path resolves against [`global.defaults.scriptsDir`](#componentglobaldefaults) when relative, or is used as-is when absolute. Use this for anything beyond a one-liner — see [Use an external script file](../how-to-guides.md#use-an-external-script-file). |

Both forms are **compiled once at startup** (a bad path or a compile error fails fast, before any
message flows), sandboxed, and bounded (1,000,000 ops/eval) so a runaway script cannot wedge a worker.
For the full scripting model — engine selection, scope, state, return values, both languages, and a
cookbook of worked examples in **both engines** — see the dedicated **[Scripting guide](../scripting.mdx)**.

<a id="rhai-scope"></a>**Script scope (Rhai or Lua)** (available to both `filter` `script` and the `script` stage; identical in both engines) —
the per-message **message view** plus the constant **runtime context**:

| Binding | Type | Value |
|---------|------|-------|
| `topic` | string | the source topic |
| `header` | map | the envelope header — `name`, `version`, `timestamp`, `uuid`, `correlation_id`, `reply_to` |
| `body` | map | the message body |
| `tags` | map | the envelope `tags{}` (message metadata — *not* the signal) |
| `identity` | map | the **source publisher's** UNS identity — `identity.device` / `identity.component` / `identity.instance` / `identity.path`; `()` when the message carries none |
| `samples` | array | `body.samples` (or `[]`) |
| `value` | any | the first sample's `value` (scalar **or array**) |
| `quality` | string | the first sample's `quality` |
| `thingName` | string | the IoT Thing name (`{ThingName}`) |
| `componentName` | string | the short component name (`{ComponentName}`) |
| `componentFullName` | string | the fully-qualified component name (`{ComponentFullName}`) |
| `routeId` | string | the id of the route running the script |
| `recvMs` | integer | this message's broker receive time (Unix ms) |

<a id="key-paths"></a>
> **Key paths** are dotted paths over the message: roots `body.` (the default when no known root
> prefix is present), `identity.`, `tags.`, `header.`; a `[]` suffix on a segment spreads across an
> array. Examples: `body.signal.id`, `body.samples[].quality`, `identity.device`, `tags.site`.
> The `identity.` root (`identity.device` / `identity.component` / `identity.instance` /
> `identity.path`) exposes the **source publisher's** UNS identity, so
> a route can key/filter on which device or adapter produced a reading.

## Template variables

Substituted into `subscribe` filters and `publish.topic` (resolved once at startup against the active
config):

| Variable | Resolves to |
|----------|-------------|
| `{ThingName}` | the `-t/--thing` value (or platform identity) |
| `{ComponentName}` / `{ComponentFullName}` | the component's short / fully-qualified name |
| `{<key>}` | any key under top-level `tags` — e.g. `{site}`, `{appId}`, `{shop}`, `{line}`, or any custom key |

## The `streaming` section

Required when any route targets `stream:<name>`. Each stream pairs a **sink** with a durable
**buffer** and **batch**/**delivery** tuning; the route appends records, the export engine drains them
to the sink. `buffer`, `batch`, and `delivery` are the standard edgecommons streaming options (see the
[telemetry-streaming](../explanation.md) design) — summarized here:

| Block | Key fields (defaults) | Meaning |
|-------|-----------------------|---------|
| `buffer` | `type` (`disk`/`memory`), `path`, `segmentBytes` (67108864), `maxDiskBytes` (1073741824), `onFull` (`dropOldest`), `fsync` (`perBatch`) | Embedded durable (or in-memory) buffer. |
| `batch` | `maxRecords` (500), `maxBytes` (4194304), `maxLatencyMs` (1000) | How records are batched before a send. |
| `delivery` | `maxRetries` (-1 = forever), `backoffBaseMs` (50), `backoffMaxMs` (30000), `pollIntervalMs` (100) | Retry/poll behavior. |

The `kinesis` and `kafka` sinks are also standard streaming sinks:

| Sink `type` | Required | Optional |
|-------------|----------|----------|
| `kinesis` | `streamName` | `region`, `endpointUrl` |
| `kafka` | `bootstrapServers`, `topic` | `properties` (librdkafka map) |

### File sink (`sink.type: "file"`)

Rolling Parquet/Avro files for later bulk upload to a cloud data lake. Files land under
`<dir>/<partitionBy>/`, written to a `*.inprogress` temp file and atomically renamed on finalize.

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `type` | const | **required** | `"file"`. |
| `format` | enum | `parquet` | `parquet` (columnar) or `avro` (row-oriented, true value union). |
| `mode` | enum | `rows` | `rows` (normalized typed telemetry, one row per sample) or `raw` (one row per message, opaque payload). |
| `dir` | string (template) | **required** | Output directory root. Config templates resolved upstream by the library. |
| `partitionBy` | string | — | Hive-style partition sub-path with UTC time tokens `{yyyy}` / `{MM}` / `{dd}` / `{HH}` and the compound `{yyyy-MM-dd}`, e.g. `dt={yyyy-MM-dd}/hr={HH}`; resolved per file at roll time. |
| `maxFileBytes` | number | `134217728` (128 MiB) | Roll a new file once it would exceed this size. **Soft cap** — see note. |
| `maxFiles` | number | `0` (unbounded) | Keep at most this many finalized files under `dir`; over the cap, `onFull` applies. |
| `rollEverySecs` | number | `0` (disabled) | Roll the open file after this age, evaluated on the next send. |
| `onFull` | enum | `dropOldest` | At `maxFiles`: `dropOldest` (delete the oldest finalized file) or `stop` (refuse to open a new file → buffer applies backpressure). |
| `compression` | enum | `snappy` | `none` \| `snappy` \| `zstd` \| `gzip` (mapped to the format's native codec). |
| `rows` | object | — | **`rows` mode only.** A declared column projection (below). Absent → the built-in `SouthboundSignalUpdate` default projection. |

> **`maxFileBytes` is a soft cap.** Size is checked at **row-group (batch) granularity** after each
> batch's row group is flushed, and the measured size **excludes the not-yet-written footer**. A
> finalized file can therefore overshoot `maxFileBytes` by up to one batch's row group plus the
> footer. Keep `batch.maxBytes` comfortably **below** `maxFileBytes`.

#### `rows` projection — declared columns (payload-agnostic)

With `mode: "rows"` and **no `rows` block**, the sink uses its built-in default projection (decode a
`SouthboundSignalUpdate`, one row per `body.samples[]`, envelope `tags` as one JSON column — see
[data-types.md](data-types.md#rows-default-projection)). Supply a
`rows` block to declare your **own** columns from arbitrary paths — the file schema is fixed from your
list at open time and makes no assumption about a southbound shape.

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `columns` | object[] | **required** | Ordered column list. Each is `{ name, path, type? }`. |
| `columns[].name` | string | **required** | Output column name. |
| `columns[].path` | string (path) | **required** | Dotted JSON path into the message (`body.`/`tags.`/`header.`; `<explode>[]…` is element-relative). A missing/incompatible value → a **null** cell. |
| `columns[].type` | enum | `string` | `string` \| `long` \| `double` \| `bool` \| `json` (`json` serializes an object/array — e.g. the whole `tags`). See the [coercion table](data-types.md#rows-user-projection). |
| `explode` | string (path) | — | Path to an array; emit **one row per element**. Columns whose path begins with `<explode>[]` resolve against the current element; all others against the whole message. |

> A user projection is **never** routed to `_unmapped` — an unmatched path is a null cell. See
> [data-types.md](data-types.md#rows-user-projection) for a worked example.

> With the **default** projection, a `rows`-mode payload that is **not** a `SouthboundSignalUpdate`
> (not JSON, or no `body.samples`) is never dropped — it is written to a sibling `_unmapped` **raw**
> file. See [data-types.md](data-types.md#raw-schema).

### Required cargo features

The `stream:<name>` target needs the `streaming` feature; each sink needs its own feature on top:

| Sink | Features |
|------|----------|
| file (Parquet) | `streaming-file-parquet` |
| file (Avro) | `streaming-file-avro` |
| Kinesis | `streaming-kinesis` |
| Kafka | `streaming-kafka` |

`streaming` alone is buffer-only (records accumulate but never export). A `stream:` route built
without `streaming` logs a warning and drops its output.

## UNS observability & control

The processor is a first-class UNS/console citizen. Beyond the library-automatic `state` keepalive,
`cfg` publisher, and `cmd` inbox, it exposes (see
[messaging-interface.md](messaging-interface.md#command-verbs)):

- **`metric/pipeline`** — with `metricEmission.target: "messaging"`, a throughput metric
  (`messagesIn`/`messagesOut`/`messagesDropped`/`streamAppends`/`publishFailures`) every 30 s on the
  UNS `metric` class. (System CPU/memory come from the heartbeat's `sys` metric.)
- **`evt/warning/*`** — rate-limited `queue-overflow` / `route-error` / `stream-unavailable` health
  events, published through the library's `events()` facade.
- **Command verbs** — built-in `ping` / `reload-config` / `get-configuration`, plus the processor's
  custom `get-stats` / `flush` / `pause` / `resume`.

## Lifecycle

> Routes are read **once at startup**. Changing the route topology (`component.instances[]`) requires
> a component restart. The built-in `reload-config` verb hot-swaps the config snapshot but does not
> rebuild the already-wired routes; there is no dynamic route rebuild. Use
> `pause` / `resume` / `flush` to control the already-wired routes at runtime.

> The component handles **SIGTERM** (and the platform shutdown signal): it aborts the metric emitter,
> unsubscribes every filter, closes the route channels, and waits for each worker to drain — including
> a **final aggregate flush**, so in-flight windows are emitted before exit.
