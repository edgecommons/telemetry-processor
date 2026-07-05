# Tutorial — Your First Telemetry Processor

In this tutorial you bring the `telemetry-processor` up on your laptop, feed it synthetic
`SouthboundSignalUpdate` telemetry over a local MQTT broker, and watch it do two things at once:
**downsample** a signal stream onto a new topic, and **archive** windowed aggregates as real
**Parquet** files. No Greengrass, no cloud, no hardware — just Docker, a Rust build, and a few lines
of Python.

By the end you will have seen a downsampled message land on the Unified-Namespace `data` topic
`ecv1/my-thing/telemetry-processor/main/data/downsampled`, and a rolling `.parquet` file appear under
`./out/archive/dt=…/hr=…/`.

> This is a guided first run — it makes the choices for you and keeps the explanation short. For the
> *why*, read the [explanation](explanation.md); for variations, the [how-to guides](how-to-guides.md);
> for every knob, the [configuration reference](reference/configuration.md).

## Prerequisites

- A **Rust toolchain** (stable) to build and run the component.
- **Docker**, to start the local EMQX broker.
- **Python 3.9+** with `pip install paho-mqtt` — a tiny publisher and subscriber.
- Optional: `pip install pyarrow pandas` if you want to read the Parquet output.

Run everything from the repository root.

## 1. Start a local MQTT broker

The HOST platform talks to a plain MQTT broker instead of Greengrass IPC. Start the bundled EMQX:

```bash
docker compose -f ../ggcommons/test-infra/compose.yaml up -d
```

This gives you a broker on `localhost:1883` (plaintext) — exactly the address in
`test-configs/standalone-messaging.json`, so no edits are needed.

## 2. Run the processor

In its own terminal, build and run with the streaming + Parquet features on:

```bash
cargo run --features standalone,streaming,streaming-file-parquet -- \
  --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
  -c FILE ./test-configs/config.json \
  -t my-thing
```

The flags are the standard ggcommons CLI contract: `--platform HOST` (laptop, not Greengrass),
`--transport MQTT <messaging>` (the broker connection), `-c FILE <config>` (the component config),
and `-t my-thing` (the Thing name, which fills the `{ThingName}` template).

`test-configs/config.json` defines two **routes** (each a `component.instances[]` entry), both
subscribed to the fleet's UNS `data` class `ecv1/+/+/+/data/#`:

- **`downsample-local`** — drops any update that isn't all-`GOOD` quality, then keeps **at most one
  message per signal per second** (`sample everyMs:1000`), and republishes the survivors on
  `ecv1/my-thing/telemetry-processor/main/data/downsampled`. Target `local` (straight back onto the
  bus; the output's `identity` is restamped to the processor so it can't self-echo through the fleet
  filter).
- **`archive`** — also drops non-`GOOD`, then rolls each signal's values into **5-second tumbling
  windows** computing `avg/max/min/count/last`, and appends each window result to the durable
  `archive` stream. Target `stream:archive` — whose **file sink** writes rolling **Parquet** under
  `./out/archive/`.

Wait for the `telemetry-processor started` log line, then leave it running.

## 3. Watch the downsampled output

In a second terminal, subscribe to everything the processor republishes:

```bash
python - <<'PY'
import paho.mqtt.client as mqtt, json
c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
# Only the processor's own data output (component = telemetry-processor), so we don't also
# print the raw input we publish in Step 4.
c.on_connect = lambda c,u,f,rc,p=None: c.subscribe("ecv1/+/telemetry-processor/+/data/#")
def on_msg(c,u,m):
    b = json.loads(m.payload)["body"]; s = b["samples"][0]
    print(f'{m.topic}  {b["signal"]["id"]:12} = {s["value"]:>8}  [{s["quality"]}]')
c.on_message = on_msg
c.connect("localhost", 1883); c.loop_forever()
PY
```

(MQTTX subscribed to `ecv1/+/telemetry-processor/+/data/#` works just as well.) Leave it running —
MQTT messages aren't retained, so the subscriber must be up before you publish.

## 4. Feed it synthetic telemetry

In a third terminal, publish a burst of `SouthboundSignalUpdate` envelopes for two signals — about four
per second for eight seconds — and slip in **one BAD-quality sample** so you can watch the filter
drop it:

```bash
python - <<'PY'
import paho.mqtt.client as mqtt, json, time
from datetime import datetime, timezone
c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
c.connect("localhost", 1883); c.loop_start()

def update(signal_id, signal_name, value, quality="GOOD"):
    now = datetime.now(timezone.utc).isoformat()
    env = {
        "header": {"name": "SouthboundSignalUpdate", "version": "1.0"},
        # The UNS identity element: the SOURCE publisher (a simulated adapter on device gw-01).
        # The device lives in this identity element.
        "identity": {"hier": [{"level": "device", "value": "gw-01"}],
                     "path": "gw-01", "component": "sim-adapter", "instance": "inst1"},
        "tags":   {"site": "factory-1"},
        "body": {
            "device":  {"adapter": "sim", "instance": "inst1"},
            "signal":     {"id": signal_id, "name": signal_name},
            "samples": [{"value": value, "quality": quality,
                         "sourceTs": now, "serverTs": now}],
        },
    }
    # An adapter's UNS data topic: ecv1/{device}/{component}/{instance}/data/{signalPath}
    c.publish(f"ecv1/gw-01/sim-adapter/inst1/data/{signal_name}", json.dumps(env))

for i in range(32):
    update("ns=3;i=1001", "Temp",     round(20 + i * 0.1, 2))
    update("ns=3;i=1002", "Pressure", round(1.0 + i * 0.01, 3))
    if i == 12:
        update("ns=3;i=1001", "Temp", -999.0, quality="BAD")  # dropped by the GOOD filter
    time.sleep(0.25)

time.sleep(1); c.loop_stop()
print("published ~64 GOOD updates + 1 BAD")
PY
```

You published ~4 updates/sec per signal, but the subscriber from Step 3 prints only about **one per signal
per second** — that's the `sample` stage downsampling. And the `-999.0 [BAD]` reading **never
appears**: the `filter { quality: GOOD }` stage dropped it before sampling. You'll see something like
(exact values and cadence depend on arrival timing):

```
ecv1/my-thing/telemetry-processor/main/data/downsampled  ns=3;i=1001  =    20.0  [GOOD]
ecv1/my-thing/telemetry-processor/main/data/downsampled  ns=3;i=1002  =     1.0  [GOOD]
ecv1/my-thing/telemetry-processor/main/data/downsampled  ns=3;i=1001  =    20.8  [GOOD]
ecv1/my-thing/telemetry-processor/main/data/downsampled  ns=3;i=1002  =    1.08  [GOOD]
```

## 5. Find the Parquet archive

Meanwhile the `archive` route has been folding the same telemetry into 5-second windows and handing
each result to the file sink (buffered durably under `./out/stream-archive/`). Files roll every 30
seconds (`rollEverySecs`) or at 1 MiB (`maxFileBytes`); until a file rolls it sits as a
partially-written, in-progress file. To force a clean finalize now, **stop the processor** (Ctrl-C in
its terminal) — on shutdown the sink writes the Parquet footer and renames the open file to its final
path. Then list the output:

```bash
find ./out/archive -name '*.parquet'
# ./out/archive/dt=2026-06-30/hr=14/part-1782846690324-0.parquet
```

The directories are Hive-style partitions (`dt={yyyy-MM-dd}/hr={HH}`, UTC). Each file is a **real
Parquet file** (it begins and ends with the `PAR1` magic) that pandas, pyarrow, or Athena read
directly — **one row per aggregated sample**, with typed columns:

```bash
python - <<'PY'
import pyarrow.parquet as pq, glob
f = sorted(glob.glob("./out/archive/dt=*/hr=*/*.parquet"))[-1]
print(pq.read_table(f).to_pandas()[["signalId", "valueDouble", "valueType", "quality", "tags"]])
PY
```

You'll see `signalId` (e.g. `ns=3;i=1001`), the window **average** in `valueDouble` with
`valueType="double"`, `quality="GOOD"`, and the whole envelope `tags` as one JSON column
(`{"site":"factory-1"}`) — alongside `signalName`, `sourceTs`, `serverTs`, `adapter`, and `instance`.
(The source **device** is in the message's `identity` element, not in the archive columns; the default
file projection lands the business `tags` as JSON.) The value is written as a sparse typed column
(`valueDouble`/`valueLong`/`valueBool`/`valueString`) chosen by `valueType`, which is what lets a
lakehouse crawl and column-prune it cleanly.

## What you just saw

One processor, one telemetry source, two routes — each a `filter → … → target` pipeline:

- **`filter` → `sample` → `local`** turned a high-rate firehose into a steady ~1 Hz/signal stream on the
  bus, dropping bad-quality readings along the way. That is the **low-latency, lossy** path.
- **`filter` → `aggregate` → `stream:archive`** turned the *same* firehose into windowed rollups
  written as query-ready Parquet through a durable buffer. That is the **bulk, durable** path —
  ready for later upload to a data lake.

The two routes never touched each other; they just subscribed to the same topic and forwarded their
results to different channels. That is the whole idea of the processor.

While it ran, the processor was also a full **Unified-Namespace citizen**: subscribe
`ecv1/+/+/+/state` to see its automatic keepalive, `ecv1/+/+/+/metric/#` for its `pipeline` throughput
metric, and `ecv1/+/+/+/evt/#` for health events — and you can address its command inbox at
`ecv1/my-thing/telemetry-processor/main/cmd/get-stats` (or `flush` / `pause` / `resume`, plus the
library built-ins `ping` / `reload-config` / `get-configuration`) to read the per-route counters. See
the [messaging-interface reference](reference/messaging-interface.md#command-verbs).

## Next steps

- Shape your own routes: [how-to guides](how-to-guides.md).
- Copy a working config: [sample configurations](sample-configurations.md).
- Understand the pipeline and durability model: [explanation](explanation.md).
- Every field, every default: [configuration reference](reference/configuration.md).

To tear down the broker when you're done: `docker compose -f ../ggcommons/test-infra/compose.yaml down`.
