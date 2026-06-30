# Reference — Configuration

Complete reference for every configuration option of the **telemetry-processor** component. For *why*
these settings exist and how a route is wired, read [explanation.md](../explanation.md); for worked
configs, see [sample-configurations.md](../sample-configurations.md). The topic/message contract is in
[messaging-interface.md](messaging-interface.md); value typing is in [data-types.md](data-types.md).

## Config source & CLI

The component reads one JSON document from the `-c/--config` source and parses the standard ggcommons
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
`logging`, `heartbeat`, `metricEmission`) are standard ggcommons sections.

## Top-level sections

| Section | Required | Purpose |
|---------|----------|---------|
| `component` | yes | Routes (`instances[]`) and their cross-route defaults (`global.defaults`) — this document. |
| `tags` | recommended | Site/asset identity (`appId`/`site`/`shop`/`line`); attached to messages and usable as topic template variables. |
| `messaging` | HOST/KUBERNETES | MQTT broker connection (or supply via `--transport MQTT <file>`). On GREENGRASS the transport is IPC. |
| `streaming` | only for `stream:` targets | Named durable streams + their sinks (kinesis/kafka/**file**), below. |
| `logging`, `heartbeat`, `metricEmission` | optional | Standard ggcommons sections. |

## `component.global.defaults`

Cross-route defaults overlaid by each route (`global ⊕ instance`, instance wins per key).

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `key` | string | `body.tag.id` | Default aggregation / sample / stream-partition key path (a [dotted path](#key-paths)). |
| `target` | string | — | Default `target` for a route that omits one (`local` \| `northbound` \| `stream:<name>`). |

## `component.instances[]` (one route)

Each entry is one independent route: subscribe → pipeline → target.

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `id` | string | **required** | Route id (logs only). |
| `subscribe` | string[] | **required (non-empty)** | Topic filters on the local bus. MQTT `+`/`#` wildcards allowed; each filter is template-resolved (below) at startup. |
| `pipeline` | stage[] | `[]` | Ordered processing stages (below). Empty = pass-through. |
| `target` | string | `global.defaults.target` | `local` \| `northbound` \| `stream:<name>`. Required if no global default. |
| `publish` | object | — | Output topic / partition key / QoS (below). |
| `key` | string | `global.defaults.key` ▸ `body.tag.id` | Route default key path for `sample`/`aggregate`/stream partitioning. |
| `maxQueue` | number | `256` | Per-route internal queue depth. **Drop-on-full**: when the route's worker can't keep up, new messages are dropped (logged at debug). |

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
`{"project": {…}}`, or `{"script": "<rhai>"}`. Stages run in order; each transforms 0..N messages.

### `filter` — keep/drop whole messages

Exactly one form applies, checked in this order: `script` → `quality` → `field`.

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `script` | string | — | Rhai boolean predicate over the [message view](#rhai-scope); keep when it returns `true`. |
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

### `aggregate` — tumbling windowed reduction

Emits one [`ProcessedTelemetry`](messaging-interface.md#aggregate-output-processedtelemetry) per
`(key, window)` on close.

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `window` | string | **required** | Time window `"10s"` / `"500ms"`, or a bare record **count** `"100"`. |
| `by` | string (path) | the route `key` | Per-key path. |
| `fn` | string[] | **required (non-empty)** | Reducers: `avg` \| `max` \| `min` \| `sum` \| `count` \| `first` \| `last`. The **first** listed is the *primary* (lands in `samples[0].value`). |

> Time windows close on the worker flush tick or when a message for a newer window arrives; **count**
> windows close in-line when N is reached. Numeric reducers (`avg`/`max`/`min`/`sum`) skip
> non-numeric samples; `count` counts all; `first`/`last` keep the raw sample value.

### `project` — reshape / whitelist the body

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `keep` | string[] | — | Body paths to retain. **Only the first path segment (top-level body key) is kept** — e.g. `"tag.id"` retains the whole `tag` object. |
| `set` | object | — | Literal fields overlaid onto the body. |

> With neither `keep` nor `set`, the body passes through unchanged.

### `script` — Rhai transform

`{"script": "<rhai source>"}` — a Rhai program run per message that returns a new body map, or `()`
to **drop** the message. Compiled once at startup; the shared engine is bounded (1,000,000 ops).

<a id="rhai-scope"></a>**Rhai scope** (available to both `filter` `script` and the `script` stage):

| Binding | Type | Value |
|---------|------|-------|
| `topic` | string | the source topic |
| `body` | map | the message body |
| `tags` | map | the envelope `tags{}` |
| `samples` | array | `body.samples` (or `[]`) |
| `value` | any | the first sample's `value` |
| `quality` | string | the first sample's `quality` |

<a id="key-paths"></a>
> **Key paths** are dotted paths over the message: roots `body.` (the default when no known root
> prefix is present), `tags.`, `header.`; a `[]` suffix on a segment spreads across an array. Examples:
> `body.tag.id`, `body.samples[].quality`, `tags.site`.

## Template variables

Substituted into `subscribe` filters and `publish.topic` (resolved once at startup against the active
config):

| Variable | Resolves to |
|----------|-------------|
| `{ThingName}` | the `-t/--thing` value (or platform identity) |
| `{ComponentName}` / `{ComponentFullName}` | the component's short / fully-qualified name |
| `{<key>}` | any key under top-level `tags` — e.g. `{site}`, `{appId}`, `{shop}`, `{line}`, `{tag}` if defined |

## The `streaming` section

Required when any route targets `stream:<name>`. Each stream pairs a **sink** with a durable
**buffer** and **batch**/**delivery** tuning; the route appends records, the export engine drains them
to the sink. `buffer`, `batch`, and `delivery` are the standard ggcommons streaming options (see the
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

> **`maxFileBytes` is a soft cap.** Size is checked at **row-group (batch) granularity** after each
> batch's row group is flushed, and the measured size **excludes the not-yet-written footer**. A
> finalized file can therefore overshoot `maxFileBytes` by up to one batch's row group plus the
> footer. Keep `batch.maxBytes` comfortably **below** `maxFileBytes`.

> A `rows`-mode payload that is **not** a `SouthboundTagUpdate` (not JSON, or no `body.samples`) is
> never dropped — it is written to a sibling `_unmapped` **raw** file. See
> [data-types.md](data-types.md#raw-schema).

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

## Lifecycle

> Routes are read **once at startup**. There is **no live route hot-reload** yet — changing
> `component.instances[]` requires a component restart.

> The component handles **SIGTERM** (and the platform shutdown signal): it unsubscribes every filter,
> closes the route channels, and waits for each worker to drain — including a **final aggregate
> flush**, so in-flight windows are emitted before exit.

## Accepted but not implemented

The current build parses but does not act on these (documented to prevent misplaced trust):

- **No route hot-reload.** Routes are read once at startup; changing `component.instances[]` takes
  effect only on a restart (a Greengrass deployment or a pod rollout). The ggcommons config source
  watches the file/ConfigMap, but the processor does not register a change listener, so it does not
  re-subscribe or rebuild route workers on a live change.
- **No `processor_health` metric.** The processor does not emit a per-route health metric. Heartbeat
  and `metricEmission` are the standard ggcommons sections only.
