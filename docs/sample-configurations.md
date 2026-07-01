# Sample Configurations

Complete, ready-to-adapt configurations for the **Telemetry Processor**
(`com.mbreissi.greengrass.TelemetryProcessor`), one per realistic deployment scenario. Each sample is
a valid config document; the prose after it explains **what every option does and how it changes
runtime behavior** — which topics a route consumes, how its pipeline filters/samples/aggregates the
stream, where the result is forwarded (local bus, northbound IoT Core, or a durable stream), and how
the file/stream sinks roll, batch, and survive restarts.

For the exhaustive option list see [reference/configuration.md](reference/configuration.md); for the
topic/message envelopes the routes consume and emit see
[reference/messaging-interface.md](reference/messaging-interface.md); for the reasoning behind the
pipeline, channel, and durability models see [explanation.md](explanation.md); and for task recipes
see [how-to-guides.md](how-to-guides.md).

> **How config reaches the component.** The processor reads one JSON document from the `-c/--config`
> source, which defaults by platform: `HOST` → `FILE`, `GREENGRASS` → `GG_CONFIG` (the deployment's
> `ComponentConfiguration`), `KUBERNETES` → `CONFIGMAP` (a mounted directory). Routes live under
> `component.instances[]` with cross-route defaults under `component.global.defaults`; the sibling
> sections (`tags`, `messaging`, `logging`, `heartbeat`, `metricEmission`, `streaming`) are standard
> ggcommons sections. Only the `streaming.streams[].sink` **`file`** variant is a canonical-schema
> addition; routes live in the permissive `component` subtree and need no schema change.

This page is organized as:

- **[The route and pipeline model](#the-route-and-pipeline-model-read-this-first)** — how a route is
  wired (`subscribe` → `pipeline` → `target`), the stage operators, key/template resolution, and the
  feature flags each target needs. Read this first; every example below relies on it.
- **[§1](#1-minimal-host-dev-filter-sample-local)** — the smallest HOST/dev pipeline: filter GOOD,
  downsample, republish to the local bus.
- **[§2](#2-windowed-aggregate-to-a-durable-parquet-archive)** and
  **[§3](#3-high-rate-raw-archival-with-size-driven-parquet-rotation)** — the durable **file sink**:
  windowed rows-mode Parquet partitioned by date/hour, and high-rate raw archival rolled by size.
- **[§4](#4-hot-path-aggregate-to-kinesis)** and **[§5](#5-alarms-northbound-to-iot-core)** — the two
  cloud channels: a Kinesis hot path and low-rate alarms northbound to IoT Core.
- **[§6](#6-greengrass-v2-deployment-ipc)** and **[§7](#7-kubernetes-configmap)** — the on-device
  Greengrass (IPC) and in-cluster Kubernetes (ConfigMap) shapes.
- **[§8](#8-fan-out-multiple-routes-sharing-one-subscribe-filter)**–**[§10](#10-avro-instead-of-parquet)**
  — fan-out across routes, the Rhai escape hatch, and Avro as a landing format.
- **[§11](#sample-payload-agnostic)** — payload-agnostic end to end: external `.rhai` script files +
  a declared file projection on a non-southbound body.

Closes with **[Where settings resolve from (precedence)](#where-settings-resolve-from-precedence)**.

---

## The route and pipeline model (read this first)

Two pieces of structure drive every example: how a **route** is wired, and how its **pipeline** of
stages transforms each message. Both are spelled out once here.

### A route is one `component.instances[]` entry

The processor enumerates routes from `component.instances[]`; each entry is one independent route with
its own subscription, pipeline, and target. A route may omit any field present in
`component.global.defaults`; the effective route is `global ⊕ instance` with the instance winning.

| Field | Meaning |
|-------|---------|
| `id` (required) | Route id — used in logs and as the metric dimension. |
| `subscribe` | `[string]` of MQTT topic filters (`+`/`#` wildcards allowed). Each filter is run through the ggcommons template resolver, so `{ThingName}` / `{ComponentName}` / `{site}` (and any `tags` key) expand against the active config. |
| `pipeline` | `[stage]` — an ordered list of transform stages (below). Order matters: stages run left to right. |
| `target` | `"local"` \| `"northbound"` \| `"stream:<name>"`. Falls back to `global.defaults.target`; a route with no target at all is skipped with an error. |
| `publish` | `{ topic, partitionKey, qos }` — the output address. `topic` (for `local`/`northbound`) is template-resolved at startup; `partitionKey` (for `stream:`) and `qos` are described per scenario. |
| `key` | Default aggregation/partition key **path** for the route (e.g. `body.signal.id`). Falls back to `global.defaults.key`, then the built-in `body.signal.id`. |
| `maxQueue` | Depth of the route's bounded inbound queue (also the broker-side subscribe queue depth). Default `256`; **drop-on-full at the edge** (a full queue logs and drops, it does not block the broker). |

> **`key`/`by`/`partitionKey` are JSON paths, not templates.** They address a field inside each
> message (`body.signal.id`, `body.samples[].value`, `tags.site`) via the dotted-path resolver — a `[]`
> suffix spreads across an array. Only `subscribe[]` and `publish.topic` go through `{…}` template
> substitution. Don't put `{ThingName}` in a `key`/`partitionKey`, and don't put a `body....` path in
> a topic.

### The pipeline stages (externally tagged, in order)

Each stage is a single-key object naming the operator. A stage emits 0..N messages; a `filter` drops
to 0 or 1, an `aggregate` accumulates and emits on window close, the rest pass 1.

| Stage | Form | Behavior |
|-------|------|----------|
| `filter` | `{ "filter": { "quality": "GOOD" } }` | Keep the message only when **every** `body.samples[].quality` equals the string (and at least one sample exists). |
| `filter` | `{ "filter": { "field": "body.samples[].value", "op": "gt", "value": 50 } }` | Keep when **any** value resolved at `field` satisfies `op` vs `value`. Ops: `eq`, `ne`, `gt`, `lt`, `ge`, `le`, `exists`, `contains`. `[]` spreads across an array (any-element match). Numbers compare numerically (strings that parse as numbers are coerced). |
| `filter` | `{ "filter": { "script": "samples.all(\|s\| s.quality == \"GOOD\")" } }` | A Rhai boolean predicate over a read-only view; keep when it returns `true`. An eval error drops the message (logged). |
| `sample` | `{ "sample": { "everyMs": 1000, "by": "body.signal.id" } }` or `{ "everyN": 100 }` | Per-key downsample: keep one message per key per `everyMs` window, or one in every `everyN`. `by` falls back to the route `key`. |
| `aggregate` | `{ "aggregate": { "window": "10s", "by": "body.signal.id", "fn": ["avg","max","min","sum","count","first","last"] } }` | Tumbling-window reduction per key. `window` is time (`"10s"` / `"500ms"`) or a bare record count (`"100"`). Emits one `ProcessedTelemetry` message per `(key, window)` on close (§2). |
| `project` | `{ "project": { "keep": ["signal","samples"], "set": { "origin": "processor" } } }` | `keep` whitelists **top-level body keys** (the first segment of each listed path); `set` overlays literal fields onto the body. With neither, the body passes through. |
| `script` | `{ "script": "#{ \"scaled\": value * 0.1 }" }` | A Rhai program that returns a new body map; `()` drops the message (§9). |

The Rhai engine is always compiled in. A `filter`/`script` scope exposes `topic` (string),
`body`/`tags` (maps), `samples` (array), and the convenience bindings `value`/`quality` (the first
sample's). The shared engine is bounded (`max_operations = 1_000_000`) to deter runaway scripts.

### Targets and the feature flags they need

| `target` | What it does | Build requirement |
|----------|--------------|-------------------|
| `local` | Republish the processed message on the local bus (`messaging.publish`) to `publish.topic`, or — if `topic` is omitted — back onto the **source topic**. | always available |
| `northbound` | Publish to IoT Core / a northbound MQTT broker (`publish_to_iot_core`) at `publish.qos`. | always available; needs a reachable cloud session (§5) |
| `stream:<name>` | Append the serialized message to the durable stream `<name>` (defined under `streaming.streams[]`), partitioned by `publish.partitionKey`. | the `streaming` feature **plus** the matching sink feature; without `streaming` the append is dropped with a warning |

Off-by-default Cargo features compose the binary: `standalone` (default), `greengrass` (IPC — Linux/WSL
only), `streaming`, and the sink features `streaming-kinesis`, `streaming-file-parquet`,
`streaming-file-avro`. A `stream:` route that names a file-Parquet sink needs
`streaming,streaming-file-parquet`; a Kinesis sink needs `streaming,streaming-kinesis`; Avro needs
`streaming,streaming-file-avro`.

---

## 1. Minimal HOST dev: filter, sample, local

The smallest useful pipeline. One route subscribes to the southbound bus, keeps only GOOD-quality
updates, downsamples each signal to 1 Hz, and republishes the result on the local bus under a `processed/…`
topic. This is the shape you run against a local broker and a southbound adapter (or a replay) while
developing.

On HOST the dual-MQTT transport needs broker details. You can supply them inline under `messaging`
(shown here) or as a separate file passed positionally as `--transport MQTT ./standalone-messaging.json`.

```jsonc
// config.json
{
  "logging": { "level": "INFO", "rust_format": "{timestamp} [{level}] {target} - {message}" },

  "messaging": {
    "local": { "host": "localhost", "port": 1883, "clientId": "telemetry-processor-local" }
  },

  "metricEmission": { "target": "log", "namespace": "ggcommons" },

  "tags": { "appId": "Demo", "site": "factory-1", "shop": "shopA", "line": "line1" },

  "component": {
    "global": { "defaults": { "key": "body.signal.id" } },
    "instances": [
      {
        "id": "downsample-local",
        "subscribe": [ "southbound/factory-1/+/+/+" ],
        "pipeline": [
          { "filter": { "quality": "GOOD" } },
          { "sample": { "everyMs": 1000, "by": "body.signal.id" } }
        ],
        "target": "local",
        "publish": { "topic": "processed/{ThingName}/downsampled" }
      }
    ]
  }
}
```

Run it:

```bash
# built binary (the standalone feature is the default build)
telemetry-processor --platform HOST --transport MQTT ./standalone-messaging.json \
  -c FILE ./config.json -t my-thing

# or from source
cargo run --features standalone -- --platform HOST --transport MQTT ./standalone-messaging.json \
  -c FILE ./config.json -t my-thing
```

| Option | Effect on runtime behavior |
|--------|----------------------------|
| `messaging.local` | The local MQTT broker the processor both **subscribes to** (the `southbound/…` source) and **publishes to** (the `processed/…` output). On HOST this is one half of the dual-MQTT transport; `clientId` must be unique per process or the broker drops the older session. Omit it here and pass the same block as the positional `--transport MQTT ./standalone-messaging.json` instead. |
| `metricEmission` | Standard ggcommons metric target (`log` / `messaging` / `cloudwatch` / `prometheus`). Observability only — it does not change processing. |
| `tags` | Site/asset identity attached to messages and usable as topic template variables (`{site}`, `{appId}`). Pure metadata. |
| `global.defaults.key` | Default key **path** every route inherits for `sample`/`aggregate`/`partitionKey` when it doesn't set its own. `body.signal.id` is the southbound contract's stable canonical id. |
| `global.defaults.target` | Default `target` a route inherits when it omits one. |
| `instances[].id` | Stable route id (**required**); appears in logs. |
| `subscribe` | The topic filter(s) this route consumes. `southbound/factory-1/+/+/+` matches every `{ComponentName}/{InstanceId}/{signalId}` published by adapters under `factory-1`. `+`/`#` wildcards are honored; the filter is template-resolved first. |
| `filter.quality: "GOOD"` | Drops any message unless **all** its `samples[].quality` are `GOOD` — the cheap way to shed BAD/UNCERTAIN readings before they cost downstream work. |
| `sample.everyMs` / `by` | Per-key downsample: at most one message **per signal** (`by: body.signal.id`) per 1000 ms. The first message for a key always passes; later ones within the window are dropped. Turns an adapter's high-rate feed into a steady 1 Hz stream. |
| `target: "local"` | Republishes each surviving message on the local bus. |
| `publish.topic` | Output topic template (resolved at startup). Omit it and the processed message is republished on its **source topic** — set it (as here) to land on a distinct `processed/…` topic so consumers don't see both raw and processed copies. |

---

## 2. Windowed aggregate to a durable Parquet archive

The archetypal edge-analytics route: collapse a high-rate per-signal feed into 10-second tumbling
windows (avg/max/min/count/last) and land the rollups as **columnar Parquet** under a date/hour
partition layout, ready for bulk upload to a cloud data lake (S3/Glue/Athena, ADLS, BigQuery). The
file destination is a normal stream sink, so the route forwards to `stream:archive` and the
`streaming.streams[]` entry named `archive` owns the file sink, durable buffer, and batching.

```jsonc
// config.json
{
  "logging": { "level": "INFO", "rust_format": "{timestamp} [{level}] {target} - {message}" },
  "messaging": { "local": { "host": "localhost", "port": 1883, "clientId": "telemetry-processor" } },
  "metricEmission": { "target": "log", "namespace": "ggcommons" },
  "tags": { "appId": "Demo", "site": "factory-1", "shop": "shopA", "line": "line1" },

  "streaming": {
    "streams": [
      {
        "name": "archive",
        "sink": {
          "type": "file",
          "format": "parquet",
          "mode": "rows",
          "dir": "./out/archive",
          "partitionBy": "dt={yyyy-MM-dd}/hr={HH}",
          "maxFileBytes": 134217728,
          "maxFiles": 64,
          "rollEverySecs": 300,
          "onFull": "dropOldest",
          "compression": "snappy"
        },
        "buffer": { "path": "./out/stream-archive", "segmentBytes": 16777216, "maxDiskBytes": 1073741824, "onFull": "dropOldest" },
        "batch": { "maxRecords": 5000, "maxBytes": 8388608, "maxLatencyMs": 5000 },
        "delivery": { "pollIntervalMs": 1000 }
      }
    ]
  },

  "component": {
    "global": { "defaults": { "key": "body.signal.id" } },
    "instances": [
      {
        "id": "archive-good",
        "subscribe": [ "southbound/factory-1/+/+/+" ],
        "pipeline": [
          { "filter": { "quality": "GOOD" } },
          { "aggregate": { "window": "10s", "by": "body.signal.id", "fn": ["avg", "max", "min", "count", "last"] } }
        ],
        "target": "stream:archive",
        "publish": { "partitionKey": "body.signal.id" }
      }
    ]
  }
}
```

```bash
telemetry-processor --platform HOST --transport MQTT ./standalone-messaging.json \
  -c FILE ./config.json -t my-thing
# build with the file-Parquet sink feature:
cargo run --features standalone,streaming,streaming-file-parquet -- --platform HOST ...
```

The `aggregate` stage emits one `ProcessedTelemetry` message per `(signal, window)` when the window
closes (on the worker's flush tick, or when a message for a newer window arrives). Its body carries
`samples[0].value` = the **first-listed** reducer (here `avg`, so the file sink's rows mode lands a
value), the full reducer set under `agg`, and a `window` block (`{ startMs, endMs, count }`):

```jsonc
// emitted ProcessedTelemetry body (per signal, per 10s window)
"body": {
  "signal": { "id": "ns=3;i=1001", "name": "Temp" },
  "samples": [ { "value": 21.4, "quality": "GOOD" } ],   // value = avg (first fn)
  "agg": { "avg": 21.4, "max": 23.1, "min": 19.8, "count": 412, "last": 22.0 },
  "window": { "startMs": 1719705600000, "endMs": 1719705610000, "count": 412 }
}
```

### File sink options

| Option | Effect on runtime behavior |
|--------|----------------------------|
| `sink.type: "file"` | Selects the rolling-file sink (vs `kinesis`/`kafka`). Built only when the binary includes `streaming-file-parquet` (or `-avro`); otherwise the stream buffers but never drains. |
| `sink.format: "parquet"` | Output encoding — `parquet` (default, columnar, query-ready, best compression + column pruning) or `avro` (§10). |
| `sink.mode: "rows"` | `rows` flattens each `SouthboundSignalUpdate` / `ProcessedTelemetry` sample into one **typed** row (sparse `valueDouble`/`valueLong`/`valueBool`/`valueString` columns + a `valueType` discriminator, plus `site`/`shop`/`line`/`adapter`/`signalId`/`signalName`/`quality`/`sourceTs`/`serverTs`). A payload that is **not** a southbound-shaped envelope is **never dropped** — it lands in a sibling `_unmapped` **raw** file. |
| `sink.dir` | Output directory root (template vars like `{ThingName}` resolved by the library). Finalized files are written under `<dir>/<partitionBy>/`. |
| `sink.partitionBy` | Hive-style partition sub-path appended to `dir`. UTC time tokens `{yyyy}` / `{MM}` / `{dd}` / `{HH}` and the compound `{yyyy-MM-dd}` are resolved **per file at roll time** → `dt=2026-06-30/hr=14/`. (Per-message-field partition directories are a deferral; `site`/`adapter` ride as columns today.) |
| `sink.maxFileBytes` | Roll a new file once the current one **would exceed** this many bytes (default `134217728` = 128 MiB — large enough to avoid the analytics "small files" problem). **Soft cap:** it is checked at row-group granularity, so a finalized Parquet file can exceed `maxFileBytes` by up to one row group plus the footer. For tight files, keep `batch.maxBytes` well **below** `maxFileBytes` so each appended batch is small relative to the roll threshold. |
| `sink.maxFiles` | Ring cap on finalized files under `dir` (`0` = unbounded). When exceeded, `onFull` applies. `64` here bounds the archive footprint to ~64 files. |
| `sink.rollEverySecs` | Force a roll after this many seconds (evaluated on the next send, not a wall-clock interrupt). `300` caps the open-file window at 5 min — which also bounds Parquet hard-crash loss (§ durability). `0` disables time-based rolling (size-only, see §3). |
| `sink.onFull` | When `maxFiles` is reached: `dropOldest` (default — delete the oldest finalized file) or `stop` (the sink reports a non-retryable failure so the durable buffer applies backpressure/retention instead of overwriting). |
| `sink.compression` | File codec: `none` / `snappy` (default) / `zstd` / `gzip`, mapped to the format's native codec. `snappy` is the conventional splittable analytics default. |

### Buffer / batch / delivery options

| Option | Effect on runtime behavior |
|--------|----------------------------|
| `buffer.path` | Directory for this stream's durable segment log + checkpoint. The route appends here first; the export engine drains it to the sink. Survives restarts (recovered on open). |
| `buffer.segmentBytes` | Roll a new buffer segment when an append would exceed this size (default `67108864`). |
| `buffer.maxDiskBytes` | Total on-disk budget for the buffer (default `1073741824`). When exceeded with undelivered data, `buffer.onFull` decides. Must be `≥ segmentBytes`. |
| `buffer.onFull` | Backpressure when the buffer is over budget: `dropOldest` (default — telemetry-friendly, never blocks the producer), `block` (lossless, blocks the route worker), or `rejectNew`. |
| `batch.maxRecords` / `maxBytes` / `maxLatencyMs` | The export engine assembles a send batch when it reaches `maxRecords` **or** `maxBytes`, or after `maxLatencyMs` even if partial (so low rates still drain). `maxBytes` is the **per-write size** into the file sink — keep it under `maxFileBytes` for predictable file sizes (see soft-cap note). |
| `delivery.pollIntervalMs` | How often the engine checks the buffer for new data when idle. |
| `publish.partitionKey` | Path used as the stream record's partition key (default = the route `key`). For a file sink it is metadata on the buffered record; for Kinesis/Kafka it is the shard/partition key. |

> **Durability (file sink).** A clean shutdown finalizes the open file on drop — **no loss**. A hard
> crash: **Parquet discards the unclosed footer-less `*.inprogress` file**, so loss is bounded by the
> open-file window (`rollEverySecs` / `maxFileBytes`); **Avro recovers to its last sync block** (§10).
> The pipeline is at-least-once, so a crash between sink-write and buffer-commit can re-deliver a
> batch — consumers de-duplicate on `(signalId, sourceTs)`.

---

## 3. High-rate raw archival with size-driven Parquet rotation

When you want a **forensic firehose** — every message archived verbatim, rolled purely by size — use
`mode: "raw"` with `rollEverySecs: 0` (time-roll off) and a small `maxFileBytes`. Raw mode writes one
row per message (minimal envelope columns `topic` / `recvTs` / `name` / `version` plus the opaque
payload), so it accepts **any** message shape, not just southbound envelopes. With time-rolling
disabled, files rotate only when they fill, giving uniform, size-bounded objects ideal for steady bulk
upload.

```jsonc
// config.json — streaming + component sections
{
  "streaming": {
    "streams": [
      {
        "name": "raw-archive",
        "sink": {
          "type": "file",
          "format": "parquet",
          "mode": "raw",
          "dir": "/data/raw-archive",
          "partitionBy": "dt={yyyy-MM-dd}/hr={HH}",
          "maxFileBytes": 8388608,
          "maxFiles": 256,
          "rollEverySecs": 0,
          "onFull": "dropOldest",
          "compression": "zstd"
        },
        "buffer": { "path": "/data/stream-raw", "segmentBytes": 4194304, "maxDiskBytes": 536870912, "onFull": "dropOldest" },
        "batch": { "maxRecords": 2000, "maxBytes": 524288, "maxLatencyMs": 2000 },
        "delivery": { "pollIntervalMs": 500 }
      }
    ]
  },
  "component": {
    "global": { "defaults": { "key": "body.signal.id" } },
    "instances": [
      {
        "id": "archive-all-raw",
        "subscribe": [ "southbound/factory-1/#" ],
        "pipeline": [],
        "target": "stream:raw-archive",
        "publish": { "partitionKey": "body.signal.id" },
        "maxQueue": 20000
      }
    ]
  }
}
```

| Option | Effect on runtime behavior |
|--------|----------------------------|
| `pipeline: []` | No transform — every matched message is forwarded verbatim. The route is pure transport from the local bus into the durable archive. |
| `subscribe: ["southbound/factory-1/#"]` | The multi-level `#` wildcard captures the **entire** southbound subtree under `factory-1` (all components/instances/signals, including nested `…/alarms/#`). |
| `mode: "raw"` | One row per message; payload kept opaque. Accepts non-southbound shapes (this is also the `_unmapped` fallback rows mode uses). Use for replay/forensics where you want bytes, not typed columns. |
| `maxFileBytes: 8388608` | Small (8 MiB) cap so files rotate frequently by size. Combined with `rollEverySecs: 0`, **size is the only trigger** — at a steady ingest rate you get a predictable cadence of ~8 MiB objects. Remember the soft cap: a file finalizes at the first row-group boundary past 8 MiB, so keep `batch.maxBytes` (512 KiB here) well below it. |
| `rollEverySecs: 0` | Disables time-based rolling. Without it, a slow period would still roll a half-empty file every N seconds; with it, files only roll when full — uniform sizes, fewer tiny objects. |
| `maxFiles: 256` | Ring of 256 finalized files (~2 GiB at 8 MiB each) before `dropOldest` reclaims the oldest. |
| `compression: "zstd"` | Higher-ratio codec — sensible for cold archival where read latency matters less than footprint. |
| `maxQueue: 20000` | A deep inbound queue absorbs bursts on this high-rate route so the drop-on-full edge rarely triggers. |

> Because a hard crash discards the open Parquet file, a **small `maxFileBytes`** is itself a
> loss-bounding lever in raw mode: the window of at-risk records is just the records written since the
> last roll. If you need none-lost-on-crash, choose Avro (§10).

---

## 4. Hot path: aggregate to Kinesis

Bulk process telemetry destined for cloud analytics goes on the **streaming channel** to Kinesis. The
route aggregates per signal, then forwards to `stream:hot`, whose sink is Kinesis. The durable buffer in
front of Kinesis means a WAN outage parks records on disk and drains them when connectivity returns.

```jsonc
// config.json — streaming + component sections
{
  "streaming": {
    "streams": [
      {
        "name": "hot",
        "sink": {
          "type": "kinesis",
          "streamName": "ggcommons-telemetry-hot",
          "region": "us-east-1"
        },
        "buffer": { "path": "/data/stream-hot", "segmentBytes": 4194304, "maxDiskBytes": 268435456, "onFull": "dropOldest" },
        "batch": { "maxRecords": 500, "maxBytes": 4194304, "maxLatencyMs": 1000 },
        "delivery": { "pollIntervalMs": 1000, "maxRetries": -1 }
      }
    ]
  },
  "component": {
    "global": { "defaults": { "key": "body.signal.id" } },
    "instances": [
      {
        "id": "hot-rollup",
        "subscribe": [ "southbound/factory-1/+/+/+" ],
        "pipeline": [
          { "filter": { "quality": "GOOD" } },
          { "aggregate": { "window": "5s", "by": "body.signal.id", "fn": ["avg", "max", "count"] } }
        ],
        "target": "stream:hot",
        "publish": { "partitionKey": "body.signal.id" }
      }
    ]
  }
}
```

```bash
cargo run --features standalone,streaming,streaming-kinesis -- --platform HOST \
  --transport MQTT ./standalone-messaging.json -c FILE ./config.json -t my-thing
```

| Option | Effect on runtime behavior |
|--------|----------------------------|
| `sink.type: "kinesis"` | Delivers batches to Amazon Kinesis Data Streams. Requires the `streaming-kinesis` feature. |
| `sink.streamName` | Target Kinesis stream (supports template vars). |
| `sink.region` | AWS region for the stream. Optional — falls back to the SDK's default region resolution. |
| `sink.endpointUrl` | (Not shown) override the Kinesis endpoint for LocalStack/floci/VPC-endpoint testing; the default credential/endpoint chain applies otherwise. |
| `publish.partitionKey` | Resolved per record as the **Kinesis partition key** (default = route `key` = `body.signal.id`), so a signal's records hash to a consistent shard and stay ordered. |
| `delivery.maxRetries: -1` | Retry a batch **forever** (the disconnected-edge case) with exponential backoff (`backoffBaseMs`→`backoffMaxMs`). Records sit safely in the durable buffer until accepted. |
| `buffer.maxDiskBytes` | Caps the on-disk parking lot (256 MiB). On a long outage, `onFull: dropOldest` sheds the oldest undelivered records to stay within budget. |

> **Credentials.** On `GREENGRASS`, the Kinesis sink resolves the device role via the
> **TokenExchangeService** — declare it as a component dependency so the Nucleus injects
> `AWS_CONTAINER_CREDENTIALS_FULL_URI` for the SDK default credential chain (see the recipe in §6). On
> `HOST`/`KUBERNETES`, supply credentials the usual way for the AWS SDK (env / profile / IRSA).

---

## 5. Alarms northbound to IoT Core

Low-rate, actionable data — alarms, state changes, a few values someone acts on — goes to the
**northbound** channel (IoT Core), not the bulk streaming channel. This route subscribes only to the
alarms subtree, keeps the messages whose quality is **not** GOOD (a fault/alarm condition), and
publishes them to IoT Core at a chosen QoS.

```jsonc
// config.json — messaging + component sections
{
  "messaging": {
    "local":   { "host": "localhost", "port": 1883, "clientId": "telemetry-processor" },
    "iotCore": {
      "endpoint": "a1b2c3d4e5f6g7-ats.iot.us-east-1.amazonaws.com",
      "port": 8883,
      "clientId": "telemetry-processor",
      "credentials": {
        "certPath": "/greengrass/v2/thingCert.crt",
        "keyPath":  "/greengrass/v2/privKey.key",
        "caPath":   "/greengrass/v2/rootCA.pem"
      }
    }
  },
  "component": {
    "global": { "defaults": { "key": "body.signal.id" } },
    "instances": [
      {
        "id": "alarms-northbound",
        "subscribe": [ "southbound/factory-1/+/+/alarms/#" ],
        "pipeline": [
          { "filter": { "field": "body.samples[].quality", "op": "ne", "value": "GOOD" } }
        ],
        "target": "northbound",
        "publish": { "topic": "telemetry/{ThingName}/alarms", "qos": "atLeastOnce" }
      }
    ]
  }
}
```

| Option | Effect on runtime behavior |
|--------|----------------------------|
| `messaging.iotCore` | The cloud half of the HOST dual-MQTT transport — the mTLS session `northbound` publishes through. Required on `HOST`/`KUBERNETES` for a `northbound` target; on `GREENGRASS` it is **not** needed (the publish routes through the Nucleus' IoT Core connection — see §6 `accessControl`). |
| `subscribe` | Narrowed to `…/+/+/alarms/#`, so only the adapters' alarm topics enter this route — IoT Core is priced per message, so keep northbound sparse. |
| `filter.field` / `op: "ne"` / `value: "GOOD"` | Keep a message when **any** `body.samples[].quality` is not `GOOD` — i.e. a fault/alarm/uncertain reading. (Equivalently a Rhai predicate like `samples.any(\|s\| s.quality != "GOOD")` — see §9.) |
| `target: "northbound"` | Publishes via `publish_to_iot_core` instead of the local bus. |
| `publish.topic` | The IoT Core topic (template-resolved). `telemetry/{ThingName}/alarms` namespaces alarms per device. |
| `publish.qos` | `atLeastOnce` (default) guarantees delivery with possible duplicates; `atMostOnce` is fire-and-forget (cheaper, may drop). Only these two are accepted; anything else falls back to `atLeastOnce`. |

---

## 6. Greengrass v2 deployment (IPC)

On `--platform GREENGRASS` there is **no `messaging` broker block and no config file**: messaging uses
Greengrass IPC (`--transport IPC`, the platform default) and config arrives from the deployment's
`ComponentConfiguration`. The sample below is the `ComponentConfig` block as it sits in `recipe.yaml`
(YAML, because the recipe is YAML), with a file `archive` stream **and** a Kinesis `hot` stream, plus
two routes. A cloud deployment overrides the same keys via `aws greengrassv2`.

```yaml
# recipe.yaml — ComponentConfiguration.DefaultConfiguration.ComponentConfig
ComponentConfiguration:
  DefaultConfiguration:
    ComponentConfig:
      logging:
        level: "INFO"
        rust_format: "{timestamp} [{level}] {target} - {message}"
      heartbeat:
        intervalSecs: 5
        targets:
          - type: "metric"
        measures: { cpu: true, memory: true }
      metricEmission:
        target: "log"
        namespace: "ggcommons"
        targetConfig:
          logFileName: "/greengrass/v2/work/{ComponentFullName}/metric.log"
      streaming:
        streams:
          - name: "archive"
            sink:
              type: "file"
              format: "parquet"
              mode: "rows"
              dir: "/greengrass/v2/work/{ComponentFullName}/archive"
              partitionBy: "dt={yyyy-MM-dd}/hr={HH}"
              maxFileBytes: 134217728
              maxFiles: 64
              rollEverySecs: 300
              onFull: "dropOldest"
              compression: "snappy"
            buffer:
              path: "/greengrass/v2/work/{ComponentFullName}/stream-archive"
              segmentBytes: 16777216
              maxDiskBytes: 1073741824
              onFull: "dropOldest"
            batch: { maxRecords: 5000, maxBytes: 8388608, maxLatencyMs: 5000 }
            delivery: { pollIntervalMs: 1000 }
          - name: "hot"
            sink: { type: "kinesis", streamName: "ggcommons-telemetry-hot", region: "us-east-1" }
            buffer:
              path: "/greengrass/v2/work/{ComponentFullName}/stream-hot"
              segmentBytes: 4194304
              maxDiskBytes: 268435456
              onFull: "dropOldest"
            delivery: { pollIntervalMs: 1000 }
      tags: { appId: "Demo", site: "Chantilly", shop: "test_shop", line: "test_line" }
      component:
        global:
          defaults: { key: "body.signal.id" }
        instances:
          # Route 1: downsample + window-aggregate GOOD telemetry → durable Parquet archive.
          - id: "archive-good"
            subscribe: [ "southbound/{site}/+/+/+" ]
            pipeline:
              - filter: { quality: "GOOD" }
              - aggregate: { window: "10s", by: "body.signal.id", "fn": ["avg", "max", "min", "count", "last"] }
            target: "stream:archive"
            publish: { partitionKey: "body.signal.id" }
          # Route 2: forward alarm-flagged updates northbound to IoT Core (low rate, control plane).
          - id: "alarms-northbound"
            subscribe: [ "southbound/{site}/+/+/alarms/#" ]
            pipeline:
              - filter: { field: "body.samples[].quality", op: "ne", value: "GOOD" }
            target: "northbound"
            publish: { topic: "telemetry/{ThingName}/alarms", qos: "atLeastOnce" }
```

The component also needs `ComponentDependencies` and `accessControl` (abbreviated here):

```yaml
ComponentDependencies:
  aws.greengrass.TokenExchangeService:        # injects the device-role creds for the Kinesis sink
    VersionRequirement: ">=0.0.0"
    DependencyType: HARD
# accessControl: grant aws.greengrass.ipc.pubsub (local bus) + aws.greengrass.ipc.mqttproxy
#   (PublishToIoTCore for the northbound target) on the resources the routes use.
Manifests:
  - Platform: { os: linux }
    Lifecycle:
      Run:
        Script: "{artifacts:path}/telemetry-processor --platform GREENGRASS -c GG_CONFIG"
```

| Difference from HOST | Effect on runtime behavior |
|----------------------|----------------------------|
| No `messaging` section; transport is IPC | Routes subscribe/publish through the Nucleus' local IPC pub/sub; `northbound` publishes through the Nucleus' IoT Core connection (mqttproxy) — no broker block, no `messaging.iotCore`. |
| `--platform GREENGRASS -c GG_CONFIG` | Config is the deployment's `ComponentConfig`; `-c GG_CONFIG` is the platform default. The binary must be built with the **`greengrass`** feature (Linux/WSL only) plus the sink features the streams use. |
| `TokenExchangeService` dependency | Makes the device role available to the AWS SDK default chain so the Kinesis sink can `PutRecords`. Without it the `hot` stream buffers but never delivers. |
| `accessControl` | Grants the IPC pub/sub (local bus) and mqttproxy (`PublishToIoTCore`, the northbound target) the routes need. |
| `dir`/`buffer.path` under `/greengrass/v2/work/{ComponentFullName}` | The Nucleus-managed component work directory — writable, per-component, and cleaned on removal. |
| `subscribe`/`pipeline`/`target` | **Identical semantics to HOST.** Only the transport and config source change; a route can be lifted verbatim between platforms. Cloud `create-deployment` merge config patches `tags`/`subscribe`/`streamName` per device/group as data, not code. |

---

## 7. Kubernetes (ConfigMap)

On `--platform KUBERNETES` the config source defaults to `CONFIGMAP`: a ConfigMap is mounted as a
directory (here `/config`) and the processor reads `config.json` from it at startup. The broker is an
in-cluster Service; identity comes from the Downward API (`POD_NAME`); the durable buffer and Parquet
files live on a mounted volume (a PVC) so they survive pod restarts.

```yaml
# k8s/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: telemetry-processor-config
  labels: { app: telemetry-processor }
data:
  config.json: |
    {
      "logging": { "level": "INFO", "rust_format": "{timestamp} [{level}] {target} - {message}" },
      "metricEmission": { "target": "log", "namespace": "ggcommons" },
      "messaging": { "local": { "host": "mqtt-broker", "port": 1883, "clientId": "telemetry-processor" } },
      "streaming": {
        "streams": [
          {
            "name": "archive",
            "sink": {
              "type": "file", "format": "parquet", "mode": "rows",
              "dir": "/data/archive", "partitionBy": "dt={yyyy-MM-dd}/hr={HH}",
              "maxFileBytes": 134217728, "maxFiles": 64, "rollEverySecs": 300,
              "onFull": "dropOldest", "compression": "snappy"
            },
            "buffer": { "path": "/data/stream-archive", "segmentBytes": 16777216, "maxDiskBytes": 1073741824, "onFull": "dropOldest" },
            "batch": { "maxRecords": 5000, "maxBytes": 8388608, "maxLatencyMs": 5000 },
            "delivery": { "pollIntervalMs": 1000 }
          }
        ]
      },
      "tags": { "appId": "Demo", "site": "factory-1", "shop": "shopA", "line": "line1" },
      "component": {
        "global": { "defaults": { "key": "body.signal.id" } },
        "instances": [
          {
            "id": "archive-good",
            "subscribe": [ "southbound/factory-1/+/+/+" ],
            "pipeline": [
              { "filter": { "quality": "GOOD" } },
              { "aggregate": { "window": "10s", "by": "body.signal.id", "fn": ["avg", "max", "min", "count", "last"] } }
            ],
            "target": "stream:archive",
            "publish": { "partitionKey": "body.signal.id" }
          }
        ]
      }
    }
```

The Deployment passes `-c CONFIGMAP /config`, wires `POD_NAME`, mounts the ConfigMap at `/config` and
a PVC at `/data`, and exposes the health port:

```yaml
# k8s/deployment.yaml (excerpt)
args: ["-c", "CONFIGMAP", "/config"]          # --platform KUBERNETES is the image entrypoint
env:
  - name: POD_NAME
    valueFrom: { fieldRef: { fieldPath: metadata.name } }
ports:
  - { name: health, containerPort: 8080 }
readinessProbe: { httpGet: { path: /readyz,  port: health }, initialDelaySeconds: 3 }
livenessProbe:  { httpGet: { path: /healthz, port: health }, initialDelaySeconds: 5 }
volumeMounts:
  - { name: config, mountPath: /config, readOnly: true }
  - { name: data,   mountPath: /data }
```

| Option | Effect on runtime behavior |
|--------|----------------------------|
| `-c CONFIGMAP /config` | Reads `config.json` from the mounted ConfigMap directory. Config is read at startup; to change routes, apply the new ConfigMap and roll the Deployment (`kubectl rollout restart`) so the pod re-reads it. |
| `messaging.local.host` = a Service name | The in-cluster MQTT broker reached via Kubernetes Service DNS (`mqtt-broker`). Point it at your broker Service. |
| `POD_NAME` (Downward API) | With no `-t/--thing`, identity resolves from `GGCOMMONS_THING_NAME` ▸ `POD_NAME`, so `{ThingName}` in topics is the pod name unless overridden. |
| `dir`/`buffer.path` on `/data` (a PVC) | The durable buffer and rolling Parquet files must live on a **persistent** volume so they survive pod restarts/rescheduling; the file sink needs a writable, durable directory. |
| `ports`/probes on `:8080` | The library serves HTTP health (`/readyz`, `/healthz`) for k8s readiness/liveness gating. |
| `metricEmission.target` | `log` here; switch to `prometheus` to expose metrics for in-cluster scraping (the idiomatic k8s path). |

---

## 8. Fan-out: multiple routes sharing one subscribe filter

Several routes can consume the **same** source feed and send it different places. When two routes
share an identical `subscribe` filter, the processor opens **one** broker subscription and fans each
arriving message out to every route's queue — so a signal update is delivered once over the wire but
processed independently by each route's pipeline and target.

```jsonc
// config.json — component section (with a `streaming.streams[].archive` file sink as in §2)
"component": {
  "global": { "defaults": { "key": "body.signal.id" } },
  "instances": [
    {
      "id": "downsample-local",
      "subscribe": [ "southbound/factory-1/+/+/+" ],
      "pipeline": [
        { "filter": { "quality": "GOOD" } },
        { "sample": { "everyMs": 1000, "by": "body.signal.id" } }
      ],
      "target": "local",
      "publish": { "topic": "processed/{ThingName}/downsampled" }
    },
    {
      "id": "archive-good",
      "subscribe": [ "southbound/factory-1/+/+/+" ],
      "pipeline": [
        { "filter": { "quality": "GOOD" } },
        { "aggregate": { "window": "10s", "by": "body.signal.id", "fn": ["avg", "max", "min", "count", "last"] } }
      ],
      "target": "stream:archive",
      "publish": { "partitionKey": "body.signal.id" }
    }
  ]
}
```

| Behavior | Detail |
|----------|--------|
| Shared filter → one subscription | Both routes list `southbound/factory-1/+/+/+`. Subscriptions are keyed by filter, so this is a **single** broker subscription; the handler clones each message to both routes' bounded queues. |
| Independent pipelines/targets | `downsample-local` republishes a 1 Hz copy on the local bus; `archive-good` lands 10 s rollups in the Parquet archive. The two never interfere — separate worker tasks, separate state. |
| Independent backpressure | Each route has its own `maxQueue`. If one route's queue fills (slow target), it drops at its own edge without affecting the other. |
| Distinct outputs | Give each route a distinct `publish.topic` (or a `stream:` target) so consumers can tell the streams apart. |

> This is the idiomatic way to split one feed into a low-latency operational view **and** a durable
> analytical archive without double-subscribing the broker.

---

## 9. Rhai filter and Rhai transform

When the built-in operators don't fit, drop to Rhai. A `filter` `script` is a boolean predicate; a
`script` stage returns a new body (or `()` to drop). Both see the same scope: `topic`, `body`, `tags`,
`samples`, and the convenience bindings `value`/`quality` (the first sample's).

```jsonc
// config.json — component section
"component": {
  "global": { "defaults": { "key": "body.signal.id" } },
  "instances": [
    {
      "id": "good-and-in-range",
      "subscribe": [ "southbound/factory-1/+/+/+" ],
      "pipeline": [
        { "filter": { "script": "samples.all(|s| s.quality == \"GOOD\" && s.value < 100.0)" } },
        { "script": "#{ \"signal\": body.signal, \"scaled\": value * 0.1, \"q\": quality, \"src\": topic }" }
      ],
      "target": "local",
      "publish": { "topic": "processed/{ThingName}/scaled" }
    }
  ]
}
```

| Stage | Effect on runtime behavior |
|-------|----------------------------|
| `filter.script` | An arbitrary Rhai boolean over the message view. Here it keeps a message only when **every** sample is GOOD **and** under 100.0 — a compound condition the `field`/`op`/`value` form can't express in one step. An eval error (or a non-boolean result) drops the message and is logged. |
| `script` (transform) | Replaces the body with the map the script returns (Rhai object syntax `#{ … }`). This one rescales the first value (`value * 0.1`) and reshapes the body, carrying `signal`, `quality`, and the source `topic` through. Returning `()` instead drops the message. A result that can't convert to JSON drops it (logged). |
| Scope bindings | `value`/`quality` are the **first** sample's; use `samples` (the full array) for multi-sample logic. `tags` exposes the envelope tags (`tags.site`, `tags.thing`). |
| Engine bound | The shared engine caps operations per evaluation (`max_operations = 1_000_000`) so a pathological script can't stall the route worker. |

> Built-in stages compile to a fixed closure once at startup (no per-message parsing); a Rhai stage
> evaluates its compiled AST per message. Prefer the built-ins on the hot path and reserve Rhai for
> logic they can't express.

---

## 10. Avro instead of Parquet

Switch `format` to `avro` when your landing target prefers row-oriented Avro or needs **true union
value typing** — most notably **BigQuery**, which loads Avro natively and preserves the polymorphic
sample value as a `union { double, long, boolean, string }` rather than the sparse typed columns
Parquet uses. Avro also has a crash-durability edge: it **recovers to its last sync block**, so a hard
crash loses only the records after that marker (Parquet discards the whole unclosed file). Everything
else about the sink — rolling, partitioning, `maxFiles`, the durable buffer — is identical.

```jsonc
// config.json — the streaming.streams[].archive sink, Avro variant
"sink": {
  "type": "file",
  "format": "avro",
  "mode": "rows",
  "dir": "/data/archive-avro",
  "partitionBy": "dt={yyyy-MM-dd}/hr={HH}",
  "maxFileBytes": 134217728,
  "maxFiles": 64,
  "rollEverySecs": 300,
  "onFull": "dropOldest",
  "compression": "snappy"
}
```

```bash
cargo run --features standalone,streaming,streaming-file-avro -- --platform HOST ...
```

| Aspect | Parquet (§2) | Avro (this section) |
|--------|--------------|---------------------|
| Layout | Columnar — best column pruning + compression for Athena/Synapse external tables | Row-oriented — append-friendly landing format |
| Polymorphic value | Sparse typed columns (`valueDouble`/`valueLong`/`valueBool`/`valueString` + `valueType`) | True `union { double, long, boolean, string }` — faithful for BigQuery loads |
| Hard-crash loss | Discards the unclosed `*.inprogress` file → bounded by the open-file window | Recovers to the last sync block → minimal loss |
| Build feature | `streaming-file-parquet` | `streaming-file-avro` |
| `compression` | `none`/`snappy`/`zstd`/`gzip` mapped to the Parquet codec | same set, mapped to the Avro codec |

> Choose **Avro** as the landing format when strict no-loss-on-crash matters or BigQuery is the
> destination; choose **Parquet** (the default) for S3/Glue/Athena and Synapse, where columnar
> pruning and compression dominate query cost. Both use the same `mode: rows` typed schema and the
> same `_unmapped` raw fallback for non-southbound payloads.

---

<a id="sample-payload-agnostic"></a>
## 11. Payload-agnostic: external script files + a custom file projection

Nothing about the processor requires the `SouthboundSignalUpdate` shape. This example ingests a
**non-southbound** sensor body, normalizes it with an **external `.rhai` script file**, aggregates a
custom `value` path, and archives the rollup through a **declared file projection** — so neither the
script nor the file schema assumes a signal shape. It exercises every "v2" lever at once: on-by-default
features, a payload-agnostic pipeline, scripts that live in version-controlled files, and a
caller-declared Parquet schema.

Incoming bus message (no `body.signal`, no `body.samples`):

```jsonc
// topic: sensors/plant-3/pump-7/vibration
{ "header": { "name": "SensorReading", "version": "1.0" },
  "tags":   { "site": "plant-3" },
  "body":   { "deviceId": "pump-7", "metric": "vibration", "raw": 3214, "ts": "2026-06-30T12:00:00Z" } }
```

```jsonc
// config.json
{
  "component": {
    "global": {
      "defaults": {
        "key": "body.deviceId",                       // not body.signal.id — this payload has no signal
        "scriptsDir": "{ComponentName}/scripts"        // where the .rhai files are shipped (see below)
      }
    },
    "instances": [
      {
        "id": "vibration-rollup",
        "subscribe": [ "sensors/+/+/vibration" ],
        "pipeline": [
          { "filter": { "script": { "file": "keep_active.rhai" } } },   // external predicate
          { "script": { "file": "normalize.rhai" } },                   // external transform
          { "aggregate": { "window": "30s", "by": "body.deviceId",
                           "value": "body.value", "fn": ["avg", "max", "count"] } }
        ],
        "target": "stream:archive"
      }
    ]
  },
  "tags": { "site": "plant-3" },
  "streaming": {
    "streams": [
      {
        "name": "archive",
        "sink": {
          "type": "file", "format": "parquet", "mode": "rows",
          "dir": "/data/vibration", "partitionBy": "dt={yyyy-MM-dd}",
          "rows": {
            "columns": [
              { "name": "deviceId",  "path": "body.signal.id" },        // aggregate sets signal.id = the key
              { "name": "site",      "path": "tags.site" },
              { "name": "avgMmS",    "path": "body.agg.avg",    "type": "double" },
              { "name": "maxMmS",    "path": "body.agg.max",    "type": "double" },
              { "name": "samples",   "path": "body.agg.count",  "type": "long" },
              { "name": "windowEnd", "path": "body.window.endMs", "type": "long" }
            ]
          }
        },
        "buffer": { "path": "/data/stream-archive", "onFull": "dropOldest" }
      }
    ]
  }
}
```

```rhai
// scripts/keep_active.rhai — a filter predicate (returns a bool)
body.raw != () && body.raw > 0          // drop missing or zero readings
```

```rhai
// scripts/normalize.rhai — a transform (returns the new body, or () to drop)
#{
  "deviceId": body.deviceId,
  "value":    body.raw * 0.001,         // raw counts → mm/s
  "unit":     "mm/s",
  "site":     tags.site                  // fold in envelope metadata
}
```

```bash
# all of these features are on by default, so the standard build covers it:
cargo run -- --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
  -c FILE ./config.json -t my-thing
```

What each lever does here:

- **`scriptsDir` + `{"file": …}`** — `keep_active.rhai` and `normalize.rhai` are read from
  `scriptsDir` (template-resolved) and **compiled once at startup**; a missing file or a typo fails the
  component immediately, not at the first message. Keeping them as files means real line breaks, no
  JSON escaping, and clean diffs. Ship them as Greengrass artifacts or a Kubernetes ConfigMap — see
  [Ship script files with a deployment](how-to-guides.md#ship-script-files-with-a-deployment).
- **`key` / aggregate `by` + `value`** — the pipeline keys by `body.deviceId` and folds `body.value`
  (the normalized field), so no part of it touches `body.signal` / `body.samples`.
- **`rows` projection** — the file schema is **declared**, not inferred: six typed columns pulled from
  the aggregate's `ProcessedTelemetry` body (`body.agg.*`, `body.window.endMs`, and `body.signal.id`,
  which the aggregate stage sets to the key). No `explode` → one row per rollup. A missing path would
  be a null cell, never an `_unmapped` file. See
  [data-types.md](reference/data-types.md#rows-user-projection).

> The same config works unchanged for a genuinely southbound payload — drop the two scripts, set
> `key` back to `body.signal.id`, and omit the `rows` block to fall back to the built-in projection.
> Payload-agnostic is the default posture; the southbound shape is just the most common case.

---

## Where settings resolve from (precedence)

Most route settings resolve from the most specific source that provides them:

```
route (instances[]) value  ▸  component.global.defaults  ▸  built-in default
```

| Setting | Resolution | Built-in default |
|---------|-----------|------------------|
| `key` (aggregation/partition key path) | route `key` ▸ `global.defaults.key` ▸ built-in | `body.signal.id` |
| `target` | route `target` ▸ `global.defaults.target` ▸ (none → route skipped) | — (required) |
| `by` (in `sample`/`aggregate`) | stage `by` ▸ the resolved route `key` | `body.signal.id` |
| `partitionKey` (for `stream:`) | `publish.partitionKey` ▸ the resolved route `key` | `body.signal.id` |
| `publish.topic` (for `local`/`northbound`) | `publish.topic` ▸ the message's **source topic** | source topic |
| `publish.qos` (for `northbound`) | `publish.qos` (`atLeastOnce`/`atMostOnce`) | `atLeastOnce` |
| `maxQueue` | route `maxQueue` | `256` |
| flush tick | the smallest **time** aggregate `window` in the route | no flush timer when no time-window stage |

**Templates vs paths.** `subscribe[]` and `publish.topic` are resolved through the ggcommons template
engine (`{ThingName}`, `{ComponentName}`, `{ComponentFullName}`, and any `tags` key). `key`, `by`,
`partitionKey` are **JSON paths** into each message and are never template-substituted.

**Sink defaults** (when a `streaming.streams[].sink`/`buffer`/`batch`/`delivery` field is omitted):

| Field | Default |
|-------|---------|
| file `format` / `mode` | `parquet` / `rows` |
| file `maxFileBytes` | `134217728` (128 MiB; **soft cap** at row-group granularity) |
| file `maxFiles` | `0` (unbounded) |
| file `rollEverySecs` | `0` (time-roll disabled) |
| file `onFull` / `compression` | `dropOldest` / `snappy` |
| `buffer.segmentBytes` / `maxDiskBytes` | `67108864` / `1073741824` |
| `buffer.onFull` / `fsync` | `dropOldest` / `perBatch` |
| `batch.maxRecords` / `maxBytes` / `maxLatencyMs` | `500` / `4194304` / `1000` |
| `delivery.maxRetries` / `pollIntervalMs` | `-1` (retry forever) / `100` |

For the full option matrix and message envelopes, see
[reference/configuration.md](reference/configuration.md) and
[reference/messaging-interface.md](reference/messaging-interface.md).
