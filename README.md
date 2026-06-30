# telemetry-processor

The **reference Rust processing component** for the ggcommons / edgecommons ecosystem. It is the
high-throughput northbound seam between southbound protocol adapters (which publish high-rate
`SouthboundTagUpdate` telemetry on the local bus) and the cloud.

It **subscribes** to configured local topics (MQTT wildcards + `{ThingName}`/`{site}` template
substitution), runs a declarative per-route **pipeline** — `filter` / `sample` / `aggregate` /
`project` / `script` (Rhai) — and **forwards** the result to a configured target:

- `local` — republish on the local bus,
- `northbound` — publish to IoT Core / a northbound MQTT broker,
- `stream:<name>` — append to a durable stream that exports to **Kinesis / Kafka / rolling
  Parquet-AVRO files** (the file sink lands query-ready data for later bulk upload to a data lake).

Each **route is one `component.instances[]` entry** (`{ id, subscribe[], pipeline[], target,
publish }`); cross-route defaults live in `component.global`. See `docs/TELEMETRY_PROCESSOR.md` in the
ggcommons monorepo for the full design.

## Run locally (HOST platform, MQTT transport)

```bash
docker compose -f ../ggcommons-monorepo/test-infra/compose.yaml up -d   # local EMQX broker

cargo run --features standalone,streaming,streaming-file-parquet -- \
  --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
  -c FILE ./test-configs/config.json \
  -t my-thing
```

Publish synthetic `SouthboundTagUpdate` messages to `southbound/factory-1/<comp>/<inst>/<tag>` and
watch: downsampled messages on `processed/my-thing/downsampled` (MQTTX), and rolling Parquet files
under `./out/archive/dt=…/`.

## Build the device artifact (Greengrass, Linux)

```bash
GGCOMMONS_FEATURES="greengrass,streaming-kinesis,streaming-file-parquet" ./build.sh
```

## CLI contract (provided by the ggcommons library)

`-c/--config <FILE|ENV|GG_CONFIG|CONFIGMAP|…>` · `--platform <GREENGRASS|HOST|KUBERNETES|auto>` ·
`--transport <IPC|MQTT [path]>` · `-t/--thing <name>`.
