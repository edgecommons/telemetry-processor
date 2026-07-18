# How-to Guides

Recipes for specific tasks. Each assumes the processor builds and runs (see the [README](../README.md)).
For ready-to-copy whole configs see [sample-configurations.md](sample-configurations.md); for concepts
see [explanation.md](explanation.md); for every field see [reference/configuration.md](reference/configuration.md).

A **route** is one `component.instances[]` entry — `{ id, subscribe[], pipeline[], target, publish }`.
Cross-route defaults live in `component.global.defaults` (`{ key, target }`), overlaid per route
(`global ⊕ instance`). The `pipeline` is an ordered list of stages; each drops, reshapes, or emits
messages, and survivors go to the route's `target`.

---

## Filter out bad-quality samples / select signals by value

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
- `script` — a Rhai/Lua boolean predicate (see [script filter](#use-a-rhai-filter-and-a-rhai-transform)).

---

## Downsample a high-rate signal

**Goal:** thin a 1 kHz signal down to 1 Hz (or 1-in-N) without aggregating.

```jsonc
"pipeline": [
  { "filter": { "quality": "GOOD" } },
  { "sample": { "everyMs": 1000, "by": "body.signal.id" } }   // ≤ 1 message/sec, per signal
]
```

- `everyMs` keeps the first message in each time window and drops the rest; `everyN` keeps one in every
  N. Set one, not both.
- `by` is the per-key path — a separate budget per distinct value (here, per signal). It defaults to
  the route `key` (`body.signal.id`), so you can usually omit it. Sampling state is per-key and
  lock-free.

---

## Window-aggregate per signal

**Goal:** emit one rolled-up record per signal per time (or count) window.

```jsonc
"pipeline": [
  { "filter": { "quality": "GOOD" } },
  { "aggregate": { "window": "10s", "by": "body.signal.id", "fn": ["avg", "max", "min", "count", "last"] } }
]
```

- `window`: a duration (`"10s"`, `"500ms"`) for a tumbling **time** window, or a bare number (`"100"`)
  for a **count** window. Time windows close on the flush tick or when a newer-window message arrives;
  count windows close at N.
- `fn` reducers: `avg`, `max`, `min`, `sum`, `count`, `first`, `last` (numeric reducers skip non-numbers).
- `by` keys the windows (defaults to the route `key`, `body.signal.id`); `value` is the path folded
  (defaults to `body.samples[].value`). Set `value` for a non-southbound payload — see
  [Aggregate a non-southbound payload](#aggregate-a-non-southbound-payload).
- The emitted message is renamed `ProcessedTelemetry`: the **first-listed `fn`** lands in
  `samples[0].value` (so a file sink's `rows` mode gets a value), the **full reducer set** under `agg`,
  and a `window` block `{ startMs, endMs, count }`. The source `signal` identity is preserved.

> A time-windowed route derives its flush tick from the smallest aggregate window automatically; a
> route with no time-windowed aggregate runs no flush timer.

---

<a id="handle-array-valued-signals"></a>
## Handle array-valued signals

**Goal:** process a signal whose sample `value` is an **array** (an OPC UA array node, a batched
register read, a vector) — the OPC UA adapter and the wire format both carry these.

Array values are first-class across the stages; pick the tool for the job:

- **Aggregate across the elements.** The `aggregate` stage folds an array value **element-wise**, so
  the numeric reducers span every element of every sample in the window — no special syntax:

  ```jsonc
  { "aggregate": { "window": "10s", "by": "body.signal.id", "fn": ["avg", "min", "max", "count"] } }
  // value [10,20,30] then [40,50] in a window → avg 30, min 10, max 50, count 5
  ```

- **Filter on the elements.** A trailing `[]` flattens the array so an **any-element** predicate works:

  ```jsonc
  { "filter": { "field": "body.samples[].value[]", "op": "gt", "value": 100 } }  // keep if any element > 100
  ```

- **Compute over the array in a script.** An array `value` arrives as a Rhai array — `map`/`filter`/
  `reduce`/`for` over it to emit mean, peak, RMS, counts, etc. See the
  [Scripting cookbook](scripting.mdx#2-array-node-mean-peak-and-rms).

- **Archive the array.** The file sink's default `rows` projection stores an array as JSON in the
  `valueString` column; to spread it into one row per element, declare a
  [`rows` projection](reference/data-types.md#rows-user-projection) with `explode`.

---

## Reshape a message

**Goal:** keep a whitelist of fields and/or stamp in literals.

```jsonc
"pipeline": [
  { "project": { "keep": ["signal", "samples"], "set": { "origin": "processor" } } }
]
```

- `keep` retains the named **top-level body keys** (the first segment of each listed path) and discards
  the rest; with no `keep`, the body passes through.
- `set` overlays literal fields onto the body (applied after `keep`).

---

<a id="use-a-rhai-filter-and-a-rhai-transform"></a>
## Use a filter and transform (Rhai or Lua)

**Goal:** express logic the built-ins don't cover.

> Both the `filter` `script` and the `script` stage run in the route's
> [`scriptEngine`](reference/configuration.md#componentglobaldefaults) — **Rhai** (default) or **Lua**
> (the `scripting-lua` build). The script *dialect* follows the engine; the scope and return contract
> are identical in both. The examples below are Rhai — see the [Scripting guide](scripting.mdx) for the
> same recipes in both engines.

```jsonc
"pipeline": [
  { "filter": { "script": "samples.all(|s| s.quality == \"GOOD\" && s.value < 100.0)" } },
  { "script": "#{ \"signal\": body.signal, \"scaled\": value * 0.1, \"src\": topic }" }
]
```

- A **`filter` `script`** returns a boolean — `true` keeps the message.
- A **`script`** stage returns the **new body** map, or `()` to **drop** the message.
- Scope exposed to both: the message view — `topic`, the `header` / `body` / `tags` maps (`tags` is
  envelope metadata, not the signal), `identity` (the source publisher's UNS identity:
  `identity.device` / `identity.component` / `identity.instance` / `identity.path`),
  `samples`, and the conveniences `value` / `quality` (the first sample's) — **plus the runtime context**
  `thingName` / `componentName` / `componentFullName` / `routeId` / `recvMs`. An eval error or a non-JSON
  result drops the message (logged at WARN).

For the full scripting model — every scope binding (incl. the runtime context `thingName` /
`componentName` / `routeId` / `recvMs`), return/error semantics, a Rhai language primer, array
handling, and a cookbook of worked examples — see the dedicated **[Scripting guide](scripting.mdx)**.

---

<a id="use-an-external-script-file"></a>
## Use an external script file

**Goal:** keep a non-trivial script out of the JSON config — version-controlled, un-escaped, shippable.

Inline source is fine for a one-liner, but anything longer is painful to embed (every `"` escaped, no
line breaks, no diffing). Reference a `.rhai` **file** instead — give `script` an object `{ "file":
"<path>" }` in either a `filter` or a `script` stage:

```jsonc
"global": { "defaults": { "scriptsDir": "{ComponentName}/scripts" } },
"instances": [
  { "id": "derive", "subscribe": ["ecv1/+/+/+/data/#"],
    "pipeline": [
      { "filter": { "script": { "file": "keep_in_range.rhai" } } },
      { "script": { "file": "rules/derive.rhai" } }
    ],
    "target": "local" }
]
```

```rhai
// rules/derive.rhai — runs per message; returns the new body, or () to drop
let celsius = body.temperature;
if celsius == () { return (); }            // no reading → drop
#{
  "signalId": body.signal.id,
  "tempF":    celsius * 1.8 + 32.0,
  "site":     tags.site,                    // envelope metadata
  "src":      topic
}
```

- A relative path resolves against `global.defaults.scriptsDir`; an absolute path is used verbatim.
  `scriptsDir` is template-resolved (`{ComponentName}`, `{ThingName}`, `tags{}`).
- Files are **read and compiled once at startup**. A missing file or a syntax error stops the
  component immediately with a clear error — it never starts in a half-broken state.
- See [Ship script files with a deployment](#ship-script-files-with-a-deployment) for getting the
  `.rhai` files onto a Greengrass device or into a Kubernetes pod.

---

<a id="compute-a-derived-signal-from-several-inputs"></a>
## Compute a derived signal from several inputs (multi-signal script)

**Goal:** calculate a value whose operands arrive as **independent signals** — a KPI like OEE, a
ratio of two counters, an interlock across states — and publish it as a **new signal** on its own
topic.

Give a `script` stage named `inputs` (one selector per operand) and an `output.topic`. The stage
caches the latest value of every input, runs the script whenever one of them **changes**, and
publishes each result as a fresh envelope. Here both operands are marked `"required": true`, so the
stage waits until both exist before running — which lets the script stay a clean one-liner:

```jsonc
"instances": [
  { "id": "fill-ratio", "subscribe": ["ecv1/gw-fill-01/opcua-adapter/+/data/#"],
    "pipeline": [
      { "script": {
          "source": "return { ratio = inputs.good.value / inputs.total.value, by = trigger.name }",
          "inputs": {
            "good":  { "device": "gw-fill-01", "signalId": "GoodBottleCount", "required": true },
            "total": { "device": "gw-fill-01", "signalId": "TotalBottleCount", "required": true }
          },
          "output": { "topic": "ecv1/gw-fill-01/telemetry-processor/fill-ratio/data/current" }
      } }
    ],
    "scriptEngine": "lua",
    "target": "local" }
]
```

- The script sees `inputs.<name>.value` / `.quality` / `.timestamp` / `.recvMs` / `.topic` for every
  operand, and `trigger` for the one whose change fired the evaluation.
- **Completeness is the script's call.** By default the stage does not wait for missing inputs — it
  runs the script on the first change and the script guards itself (`if inputs.total == nil then
  return nil end`). Marking an input `"required": true` (as above) opts into stage-level waiting so
  the script doesn't have to check; with no `required` input the stage never waits.
- Input state is isolated **per source device**, so two lines with the same signal ids never mix.
- The published result is a new envelope — producer = the processor (instance = the route id),
  `correlation_id` = the triggering message's `uuid`. The output topic must not fall under the
  route's own `subscribe` filters (startup error: feedback loop).
- Repeated identical values don't re-evaluate; a quality flip does, and the script decides
  (`if inputs.total.quality ~= "GOOD" then return nil end` holds the last output).

See [Scripting — multi-signal inputs](scripting.mdx#multi-signal-inputs) for the full semantics, the
[configuration reference](reference/configuration.md#script-inputs) for every selector field, and
[sample §12](sample-configurations.md#12-multi-signal-oee-from-named-inputs-lua) for a complete OEE
route.

---

<a id="aggregate-a-non-southbound-payload"></a>
## Aggregate a non-southbound payload

**Goal:** window-reduce a body that isn't `SouthboundSignalUpdate`-shaped (no `body.samples`).

The processor doesn't mandate the southbound schema — point the stages at your own paths. Set the
aggregate `value` (the field to fold) and `by` (the per-key path); a `script` or `field` filter can
gate on any path too:

```jsonc
// incoming body: { "deviceId": "pump-7", "temperature": 41.9, "rpm": 1180 }
"instances": [
  { "id": "temp-rollup", "subscribe": ["sensors/+/temperature"],
    "key": "body.deviceId",
    "pipeline": [
      { "filter": { "field": "body.temperature", "op": "gt", "value": 0 } },
      { "aggregate": { "window": "30s", "by": "body.deviceId",
                       "value": "body.temperature", "fn": ["avg", "max", "count"] } }
    ],
    "target": "stream:archive" }
]
```

- `value` defaults to `body.samples[].value`, falling back to the whole body; set it explicitly
  (`body.temperature`) for any non-sample shape.
- Set the route `key` / aggregate `by` to your own identity path (`body.deviceId`) instead of the
  southbound `body.signal.id`.
- To archive this to files, declare a [rows projection](#project-custom-file-columns) — the default
  projection assumes the southbound shape.

---

<a id="project-custom-file-columns"></a>
## Project custom file columns (payload-agnostic archiving)

**Goal:** land your own typed columns in the file sink, from any payload shape.

With no `rows` block the file sink uses its built-in `SouthboundSignalUpdate` projection. Supply a
`rows` block to declare columns from arbitrary paths — the schema is fixed from your list and a
missing/incompatible value becomes a null cell (never `_unmapped`):

```jsonc
"sink": {
  "type": "file", "format": "parquet", "mode": "rows", "dir": "/data/archive",
  "rows": {
    "explode": "body.samples",
    "columns": [
      { "name": "deviceId", "path": "body.deviceId" },
      { "name": "site",     "path": "tags.site" },
      { "name": "value",    "path": "body.samples[].value", "type": "double" },
      { "name": "quality",  "path": "body.samples[].quality" },
      { "name": "ts",       "path": "body.samples[].sourceTs" }
    ]
  }
}
```

- `explode` fans an array out to one row per element; a column path starting `<explode>[]` resolves
  against the current element, all others against the whole message. Omit `explode` for one row per
  message.
- `type` is `string` (default) \| `long` \| `double` \| `bool` \| `json` (use `json` to land an
  object/array such as the whole `tags` in one column). See
  [data-types.md](reference/data-types.md#rows-user-projection).

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
> `(signalId, sourceTs)`.

---

## Choose Parquet vs Avro, and rows vs raw

**Goal:** pick the landing encoding and row shape for your lake.

```jsonc
"sink": { "type": "file", "format": "avro", "mode": "raw", "dir": "/data/raw" }
```

- `format`: `parquet` (default) — columnar, best compression + column pruning for Athena / BigQuery /
  Synapse; or `avro` — row-oriented, true union value typing, recover-to-last-sync-block durability
  (good for BigQuery loads and strict no-loss). Build the matching feature.
- `mode`: `rows` (default) flattens telemetry into typed rows. Its built-in projection decodes a
  `SouthboundSignalUpdate` into one row per sample (sparse `valueDouble|valueLong|valueBool|valueString`
  + `valueType`); aggregated `ProcessedTelemetry` keeps that shape, so it lands as rows too; a payload
  that **isn't** `SouthboundSignalUpdate`-shaped is never dropped — it spills to a sibling `_unmapped`
  raw file. Add a [`rows` block](#project-custom-file-columns) to declare your own columns instead.
  `mode: "raw"` writes one opaque row per message (`offset`, `partitionKey` — the resolved
  `publish.partitionKey` — `tsMs` the stream record time, and `payload` the lossy-UTF-8 message bytes).

---

## Stream to Kinesis

**Goal:** export aggregates to a Kinesis data stream.

```jsonc
"streaming": { "streams": [
  { "name": "hot",
    "sink": { "type": "kinesis", "streamName": "edgecommons-telemetry-hot", "region": "us-east-1" },
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
  "subscribe": ["ecv1/+/+/+/data/#"],
  "pipeline": [ { "filter": { "field": "body.samples[].quality", "op": "ne", "value": "GOOD" } } ],
  "target": "northbound",
  "publish": { "topic": "ecv1/{ThingName}/telemetry-processor/evt/alarms", "qos": "atLeastOnce" } }
```

- `target: "northbound"` publishes via IoT Core / the northbound MQTT broker.
- `publish.topic` is the destination (template vars like `{ThingName}` are resolved at startup);
  omitting it reuses the source topic. Alarms are events, so this targets the UNS **`evt`** class.
  Keep route outputs on `data` / `evt` / `app` — a publish to a reserved class
  (`state`/`metric`/`cfg`/`log`) is rejected by the guard (the processor WARNs at startup if a
  resolved `publish.topic` lands on one).
- `publish.qos`: `atLeastOnce` (default) or `atMostOnce`.

---

## Route one topic to several destinations

**Goal:** fan one stream of telemetry out to multiple sinks.

Define multiple routes that **share a `subscribe` filter**. The processor subscribes each unique filter
once and fans every message out to every route that registered it, so the routes run independently:

```jsonc
"instances": [
  { "id": "downsample-local", "subscribe": ["ecv1/+/+/+/data/#"],
    "pipeline": [ { "filter": { "quality": "GOOD" } }, { "sample": { "everyMs": 1000 } } ],
    "target": "local", "publish": { "topic": "ecv1/{ThingName}/telemetry-processor/data/downsampled" } },
  { "id": "archive", "subscribe": ["ecv1/+/+/+/data/#"],
    "pipeline": [ { "aggregate": { "window": "10s", "by": "body.signal.id", "fn": ["avg", "max"] } } ],
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
defaults to `body.signal.id` — the stable canonical signal id. Override it to co-locate records by a
different dimension (e.g. device or site).

---

## Route by source device / adapter (the `identity.` path)

**Goal:** key, filter, or partition on *who* published a reading.

The source device/component/instance travel in the message's top-level `identity` element, read
via the `identity.` key path (and the `identity` script binding):

```jsonc
"pipeline": [
  // keep only readings from OPC UA adapters
  { "filter": { "field": "identity.component", "op": "eq", "value": "opcua-adapter" } }
],
"key": "identity.device",                       // aggregate / partition per source device
"publish": { "partitionKey": "identity.device" }
```

Available paths: `identity.device` (the last hierarchy value), `identity.component`,
`identity.instance`, `identity.path`, and `identity.hier[].level` / `identity.hier[].value`. A script
reads the same via the `identity` binding, e.g. `identity.device == "gw-01"`. Scope the *subscribe*
filter to specific adapters instead when you don't need the whole fleet, e.g.
`ecv1/+/opcua-adapter/+/data/#`.

---

## Operate the processor over UNS commands

**Goal:** inspect and control a running processor from the console / any MQTT client.

The processor answers its command inbox at `ecv1/{device}/telemetry-processor/cmd/<verb>`. Send a
`cmd` envelope (`header.name` = the verb) with `header.reply_to` set to get a structured reply.

| Verb | What it does |
|------|--------------|
| `ping` | liveness — `{status:"RUNNING", uptimeSecs}` (library built-in) |
| `reload-config` | re-fetch + re-apply the config (library built-in) |
| `get-configuration` | the redacted effective config (library built-in) |
| `get-stats` | per-route counters `{routes:[{id,in,out,dropped,streamAppends,publishFailures,queueDepth,paused}]}` |
| `flush` | force-close every route's open **time** windows now → `{flushed:n}` |
| `pause` / `resume` | stop / restart enqueuing to a route (`{route}`) or all routes (body omitted) |

The processor also publishes, without any request: its `state` keepalive
(`ecv1/{device}/telemetry-processor/state`), a `metric/pipeline` throughput metric (when
`metricEmission.target: "messaging"`), and `evt/warning/{queue-overflow,route-error,stream-unavailable}`
health events (via the library's `events()` facade). Subscribe the fleet with
`ecv1/+/+/+/{state,metric,evt}/#`.

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
EDGECOMMONS_FEATURES="greengrass,streaming-kinesis,streaming-file-parquet" ./build.sh
greengrass-cli deployment create --recipeDir . --artifactDir ./artifacts \
  --merge "com.mbreissi.edgecommons.TelemetryProcessor=1.0.0"
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

<a id="ship-script-files-with-a-deployment"></a>
## Ship script files with a deployment

**Goal:** get your `.rhai` files onto the device/pod so `{"script": {"file": "…"}}` can load them.

A script file must exist on disk where the process can read it, at the path `scriptsDir` +
the relative `file`. How you deliver it depends on the platform:

**Greengrass** — ship the `.rhai` files as **component artifacts** and point `scriptsDir` at the
artifact directory. Greengrass unpacks artifacts under a per-component path exposed as
`{artifacts:decompressedPath}/…` (or `{artifacts:path}`), so set `scriptsDir` from the recipe:

```yaml
# recipe.yaml (excerpt)
Manifests:
  - Platform: { os: linux }
    Artifacts:
      - URI: "s3://.../telemetry-processor.zip"          # contains scripts/derive.rhai
    Lifecycle:
      Run: >
        telemetry-processor --platform GREENGRASS -c GG_CONFIG
ComponentConfiguration:
  DefaultConfiguration:
    component:
      global:
        defaults:
          scriptsDir: "{artifacts:decompressedPath}/telemetry-processor/scripts"
```

For a local `greengrass-cli` deployment, place the files under your `--artifactDir` and use the same
`scriptsDir`. Bump the component version (or `--remove` then `--merge`) when the scripts change —
artifacts are immutable per version.

**Kubernetes** — mount the scripts from a **ConfigMap** (or a volume) and point `scriptsDir` at the
mount path:

```bash
kubectl create configmap tp-scripts --from-file=scripts/   # derive.rhai, keep_in_range.rhai, …
```

```yaml
# deployment.yaml (excerpt)
spec:
  containers:
    - name: telemetry-processor
      volumeMounts:
        - { name: scripts, mountPath: /etc/tp/scripts, readOnly: true }
  volumes:
    - name: scripts
      configMap: { name: tp-scripts }
# …and in the component ConfigMap: component.global.defaults.scriptsDir = "/etc/tp/scripts"
```

**HOST / standalone** — just place the files next to the binary (or anywhere) and set `scriptsDir` to
that directory (an absolute `file` path also works without `scriptsDir`).

> Scripts are read **once at startup**, like the rest of the config — changing a `.rhai` file needs a
> component restart (a new deployment / pod rollout), not a live reload.

---

## Run it / shut it down cleanly

**Goal:** stop without leaking subscriptions or losing in-flight windows.

The process runs until SIGTERM (or Ctrl-C). On the signal it **unsubscribes** every filter, closes the
route channels, and waits for each worker to drain — which performs a **final aggregate flush**, so open
time-windows are emitted, not dropped. Greengrass and Kubernetes send SIGTERM on stop; just allow a
moment to drain before the container is killed.
