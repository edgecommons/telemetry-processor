# How-to Guides

Recipes for specific tasks. Each assumes the processor builds and runs (see the [README](../README.md)).
For ready-to-copy whole configs see [sample-configurations.md](sample-configurations.md); for concepts
see [explanation.md](explanation.md); for every field see [reference/configuration.md](reference/configuration.md).

A **route** is one `component.instances[]` entry — `{ id, subscribe[], pipeline[], target, publish }`.
Cross-route defaults live in `component.global.defaults` (`{ key, target }`), overlaid per route
(`global ⊕ instance`). The `pipeline` is an ordered list of stages; each drops, reshapes, or emits
messages, and survivors go to the route's `target`.

---

## Filter out bad-quality samples / select tags by value

**Goal:** drop messages you don't want before they cost any downstream work.

```jsonc
"pipeline": [
  { "filter": { "quality": "GOOD" } },                                   // keep only all-GOOD messages
  { "filter": { "field": "body.samples[].value", "op": "gt", "value": 50 } }  // …and at least one sample > 50
]
```

A `filter` stage takes exactly one form, checked in this order:

- `quality: "GOOD"` — keep only when **every** `body.samples[].quality` equals the value (≥1 sample).
- `field` / `op` / `value` — a predicate over a dotted path; `[]` spreads an array, matching when **any**
  element satisfies it. Ops: `eq`, `ne`, `gt`, `lt`, `ge`, `le`, `exists` (value omitted), `contains`
  (substring). Numeric strings compare as numbers.
- `script` — a Rhai boolean predicate (see [Rhai filter](#use-a-rhai-filter-and-a-rhai-transform)).

---

## Downsample a high-rate tag

**Goal:** thin a 1 kHz tag down to 1 Hz (or 1-in-N) without aggregating.

```jsonc
"pipeline": [
  { "filter": { "quality": "GOOD" } },
  { "sample": { "everyMs": 1000, "by": "body.tag.id" } }   // ≤ 1 message/sec, per tag
]
```

- `everyMs` keeps the first message in each time window and drops the rest; `everyN` keeps one in every
  N. Set one, not both.
- `by` is the per-key path — a separate budget per distinct value (here, per tag). It defaults to the
  route `key` (`body.tag.id`), so you can usually omit it. Sampling state is per-key and lock-free.

---

## Window-aggregate per tag

**Goal:** emit one rolled-up record per tag per time (or count) window.

```jsonc
"pipeline": [
  { "filter": { "quality": "GOOD" } },
  { "aggregate": { "window": "10s", "by": "body.tag.id", "fn": ["avg", "max", "min", "count", "last"] } }
]
```

- `window`: a duration (`"10s"`, `"500ms"`) for a tumbling **time** window, or a bare number (`"100"`)
  for a **count** window. Time windows close on the flush tick or when a newer-window message arrives;
  count windows close at N.
- `fn` reducers: `avg`, `max`, `min`, `sum`, `count`, `first`, `last` (numeric reducers skip non-numbers).
- The emitted message is renamed `ProcessedTelemetry`: the **first-listed `fn`** lands in
  `samples[0].value` (so a file sink's `rows` mode gets a value), the **full reducer set** under `agg`,
  and a `window` block `{ startMs, endMs, count }`. The source `tag` identity is preserved.

> A time-windowed route derives its flush tick from the smallest aggregate window automatically; a
> route with no time-windowed aggregate runs no flush timer.

---

## Reshape a message

**Goal:** keep a whitelist of fields and/or stamp in literals.

```jsonc
"pipeline": [
  { "project": { "keep": ["tag", "samples"], "set": { "origin": "processor" } } }
]
```

- `keep` retains the named **top-level body keys** (the first segment of each listed path) and discards
  the rest; with no `keep`, the body passes through.
- `set` overlays literal fields onto the body (applied after `keep`).

---

## Use a Rhai filter and a Rhai transform

**Goal:** express logic the built-ins don't cover.

```jsonc
"pipeline": [
  { "filter": { "script": "samples.all(|s| s.quality == \"GOOD\" && s.value < 100.0)" } },
  { "script": "#{ \"tag\": body.tag, \"scaled\": value * 0.1, \"src\": topic }" }
]
```

- A **`filter` `script`** returns a boolean — `true` keeps the message.
- A **`script`** stage returns the **new body** map, or `()` to **drop** the message.
- Scope exposed to both: `topic` (string), `body` and `tags` (maps), `samples` (array), and the
  convenience bindings `value` / `quality` (the first sample's). An eval error or a non-JSON result
  drops the message (logged at WARN).

---

## Archive to rolling Parquet files, and control rotation

**Goal:** land processed telemetry as bounded, query-ready files for later bulk upload.

Point a route at `stream:archive` (`"target": "stream:archive"`), then define an `archive` stream whose
sink is `file` (see [sample-configurations.md](sample-configurations.md) for the buffer/batch context):

```jsonc
"sink": {
  "type": "file", "format": "parquet", "mode": "rows",
  "dir": "/data/archive", "partitionBy": "dt={yyyy-MM-dd}/hr={HH}",
  "maxFileBytes": 134217728, "maxFiles": 64, "rollEverySecs": 300,
  "onFull": "dropOldest", "compression": "snappy"
},
"batch": { "maxRecords": 5000, "maxBytes": 8388608, "maxLatencyMs": 5000 }
```

Three independent rotation levers:

- **Size** — `maxFileBytes` (default 128 MiB). The file rolls when the **next** batch would exceed it,
  evaluated on send. A batch writes atomically, so keep `batch.maxBytes` well below `maxFileBytes` and
  the file overshoots the cap by at most one batch.
- **Time** — `rollEverySecs` rolls the open file after N seconds (`0` disables). Checked on the next
  send, not a wall-clock interrupt, so an idle stream holds its file open until traffic resumes.
- **Ring** — `maxFiles` caps finalized files under `dir` (`0` = unbounded). When full, `onFull` is
  `dropOldest` (delete the oldest) or `stop` (non-retryable failure → the durable buffer applies
  backpressure / retention instead of losing data).

> **Durability:** clean shutdown finalizes the open file (no loss). On a hard crash Parquet discards
> the unclosed `*.inprogress` file — loss bounded by the open-file window (`rollEverySecs` /
> `maxFileBytes`) — while Avro recovers to its last sync block. At-least-once; de-dup downstream on
> `(tagId, sourceTs)`.

---

## Choose Parquet vs Avro, and rows vs raw

**Goal:** pick the landing encoding and row shape for your lake.

```jsonc
"sink": { "type": "file", "format": "avro", "mode": "raw", "dir": "/data/raw" }
```

- `format`: `parquet` (default) — columnar, best compression + column pruning for Athena / BigQuery /
  Synapse; or `avro` — row-oriented, true union value typing, recover-to-last-sync-block durability
  (good for BigQuery loads and strict no-loss). Build the matching feature.
- `mode`: `rows` (default) flattens a `SouthboundTagUpdate`-shaped message into one typed row per
  sample (sparse `valueDouble|valueLong|valueBool|valueString` + `valueType`). Aggregated
  `ProcessedTelemetry` keeps that shape, so it lands as rows too; a payload that **isn't**
  `SouthboundTagUpdate`-shaped is never dropped — it spills to a sibling `_unmapped` raw file.
  `mode: "raw"` writes one opaque row per message (`topic`, `recvTs`, `name`, `version`, `payload`).

---

## Stream to Kinesis

**Goal:** export aggregates to a Kinesis data stream.

```jsonc
"streaming": { "streams": [
  { "name": "hot",
    "sink": { "type": "kinesis", "streamName": "ggcommons-telemetry-hot", "region": "us-east-1" },
    "buffer": { "path": "/data/stream-hot", "segmentBytes": 4194304, "maxDiskBytes": 268435456, "onFull": "dropOldest" } }
] }
```

Route to it with `"target": "stream:hot"` and build `--features streaming-kinesis`. On Greengrass,
`recipe.yaml` depends on `aws.greengrass.TokenExchangeService`, which injects
`AWS_CONTAINER_CREDENTIALS_FULL_URI` so the SDK default chain resolves the device role; on HOST, supply
credentials the SDK chain can find (env / profile / instance role).

---

## Forward alarms northbound to IoT Core

**Goal:** send low-rate control/alarm data straight to the cloud MQTT topic.

```jsonc
{ "id": "alarms-northbound",
  "subscribe": ["southbound/factory-1/+/+/alarms/#"],
  "pipeline": [ { "filter": { "field": "body.samples[].quality", "op": "ne", "value": "GOOD" } } ],
  "target": "northbound",
  "publish": { "topic": "telemetry/{ThingName}/alarms", "qos": "atLeastOnce" } }
```

- `target: "northbound"` publishes via IoT Core / the northbound MQTT broker.
- `publish.topic` is the destination (template vars like `{ThingName}` are resolved at startup);
  omitting it reuses the source topic.
- `publish.qos`: `atLeastOnce` (default) or `atMostOnce`.

---

## Route one topic to several destinations

**Goal:** fan one stream of telemetry out to multiple sinks.

Define multiple routes that **share a `subscribe` filter**. The processor subscribes each unique filter
once and fans every message out to every route that registered it, so the routes run independently:

```jsonc
"instances": [
  { "id": "downsample-local", "subscribe": ["southbound/factory-1/+/+/+"],
    "pipeline": [ { "filter": { "quality": "GOOD" } }, { "sample": { "everyMs": 1000 } } ],
    "target": "local", "publish": { "topic": "processed/{ThingName}/downsampled" } },
  { "id": "archive", "subscribe": ["southbound/factory-1/+/+/+"],
    "pipeline": [ { "aggregate": { "window": "10s", "by": "body.tag.id", "fn": ["avg", "max"] } } ],
    "target": "stream:archive" }
]
```

---

## Choose the partition key for a stream

**Goal:** control how stream records shard.

```jsonc
"target": "stream:hot",
"publish": { "partitionKey": "body.device.adapter" }
```

`publish.partitionKey` is a dotted path resolved per message. It defaults to the route `key`, which
defaults to `body.tag.id` — the stable canonical tag id. Override it to co-locate records by a
different dimension (e.g. device or site).

---

## Deploy to a platform

**Goal:** run on Greengrass (IPC), HOST (Docker / binary), or Kubernetes.

**HOST (Docker / bare host)** — dual-MQTT, config from a file:

```bash
cargo build --release --features standalone,streaming,streaming-file-parquet
./target/release/telemetry-processor --platform HOST \
  --transport MQTT ./test-configs/standalone-messaging.json \
  -c FILE ./test-configs/config.json -t my-thing
```

**Greengrass (on-device)** — config from the deployment; transport is IPC. Build the Linux artifact,
then deploy with the recipe:

```bash
GGCOMMONS_FEATURES="greengrass,streaming-kinesis,streaming-file-parquet" ./build.sh
greengrass-cli deployment create --recipeDir . --artifactDir ./artifacts \
  --merge "com.mbreissi.greengrass.TelemetryProcessor=1.0.0"
# recipe Run: telemetry-processor --platform GREENGRASS -c GG_CONFIG
```

`recipe.yaml` carries the route/stream config as `ComponentConfig` and depends on
`aws.greengrass.TokenExchangeService` (needed for the Kinesis sink).

**Kubernetes** — config from a mounted ConfigMap, identity from the Downward API, a `/data` volume for
the durable buffer + rolling files:

```bash
kubectl apply -f k8s/configmap.yaml -f k8s/deployment.yaml
# image entrypoint: telemetry-processor --platform KUBERNETES
# pod args:         -c CONFIGMAP /config        (POD_NAME → Thing name when -t is absent)
```

---

## Run it / shut it down cleanly

**Goal:** stop without leaking subscriptions or losing in-flight windows.

The process runs until SIGTERM (or Ctrl-C). On the signal it **unsubscribes** every filter, closes the
route channels, and waits for each worker to drain — which performs a **final aggregate flush**, so open
time-windows are emitted, not dropped. Greengrass and Kubernetes send SIGTERM on stop; just allow a
moment to drain before the container is killed.
