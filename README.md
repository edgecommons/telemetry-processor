# telemetry-processor

The **reference Rust processing component** for the edgecommons / edgecommons ecosystem. It is the
high-throughput northbound seam between southbound protocol adapters (which publish high-rate
`SouthboundSignalUpdate` telemetry on the local bus) and the cloud.

It **subscribes** to configured local topics (MQTT wildcards + `{ThingName}` template substitution),
runs a declarative per-route **pipeline** — `filter` / `sample` / `aggregate` / `project` / `script`
(Rhai / Lua) — and **forwards** the result to a configured target:

- `local` — republish on the local bus,
- `northbound` — publish to IoT Core / a northbound MQTT broker,
- `stream:<name>` — append to a durable stream that exports to **Kinesis / Kafka / rolling
  Parquet-AVRO files** (the file sink lands query-ready data for later bulk upload to a data lake).

Each **route is one `component.instances[]` entry** (`{ id, subscribe[], pipeline[], target,
publish }`); cross-route defaults live in `component.global`. See `docs/TELEMETRY_PROCESSOR.md` in the
edgecommons monorepo for the full design.

## Unified Namespace (UNS)

The processor speaks the edgecommons **Unified Namespace** — topics are
`ecv1/{device}/{component}/[{instance}/]{class}[/channel]` (the instance segment is optional), and
because it is a single instance the processor appears on the bus at **component scope**:
`ecv1/{device}/telemetry-processor/{class}[/channel]` — the component token (the short name after the
last `.`) is followed directly by the class, with no instance segment. What this means for the processor:

- **Ingest** the fleet's southbound telemetry (the `data` class) with a single wildcard:
  `ecv1/+/+/+/data/#` (or scope it, e.g. `ecv1/+/opcua-adapter/+/data/#`).
- **Output** processed telemetry on the `data` class and events on `evt`; `state`/`metric`/`cfg`/`log`
  are **reserved** (library-owned) — a direct publish to them is rejected, so route outputs must
  target `data` / `evt` / `app`.
- **Source identity** travels in the message's top-level `identity` element (`hier`/`path`/`component`/
  `instance`), **not** in `tags.thing` (removed). Pipelines key/filter on it via the `identity.` JSON
  path (`identity.device`, `identity.component`, `identity.instance`) and scripts read the `identity`
  binding.
- **Self-echo safe:** a `local` republish onto the consumed `data` class is loop-guarded — the
  dispatcher restamps `local` output with the processor's own identity and the fan-out drops any
  re-consumed message carrying the processor's own device+component.
- **First-class console citizen for free** (from the library): the automatic `state` keepalive, the
  `cfg` effective-config publisher, and the `cmd` inbox with built-in `ping` / `reload-config` /
  `get-configuration`. The processor adds custom verbs `flush` / `get-stats` / `pause` / `resume`
  and emits its own `evt` health events + a `metric/pipeline` throughput metric. See
  `docs/reference/messaging-interface.md`.

## Run locally (HOST platform, MQTT transport)

```bash
docker compose -f ../core/test-infra/compose.yaml up -d   # local EMQX broker

cargo run -- \
  --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
  -c FILE ./test-configs/config.json \
  -t my-thing
```

The default build is **batteries-included** — standalone + streaming + Kinesis + the Parquet/AVRO
file sinks + CloudWatch are all on by default, so the command above needs no `--features`.

Publish synthetic `SouthboundSignalUpdate` messages (envelopes with a top-level `identity`) to an
adapter's UNS data topic, e.g. `ecv1/gw-01/opcua-adapter/kep1/data/<signal>`, and watch: downsampled
messages on `ecv1/my-thing/telemetry-processor/data/downsampled` (MQTTX), and rolling Parquet
files under `./out/archive/dt=…/`. Subscribe `ecv1/+/+/+/state` to see the processor's automatic
keepalive, and address `ecv1/my-thing/telemetry-processor/cmd/get-stats` to read its counters.

## Build the device artifact (Greengrass, Linux)

```bash
EDGECOMMONS_FEATURES="greengrass,streaming-kinesis,streaming-file-parquet" ./build.sh
```

## Cargo features

Batteries-included by default: `default = [standalone, streaming, streaming-kinesis,
streaming-file-parquet, streaming-file-avro, cloudwatch]`. Two features stay **off** by default
because they need a platform-specific native toolchain — enable them explicitly when you need them:

- **`greengrass`** — the Greengrass IPC transport (Linux-only C-FFI SDK; built via `./build.sh` for
  the device artifact).
- **`streaming-kafka`** — the Kafka sink (pulls `librdkafka` / `cmake`).

Slim the build with `--no-default-features` plus the subset you want (e.g.
`--no-default-features --features standalone,streaming,streaming-file-parquet`).

## CLI contract (provided by the edgecommons library)

`-c/--config <FILE|ENV|GG_CONFIG|CONFIGMAP|…>` · `--platform <GREENGRASS|HOST|KUBERNETES|auto>` ·
`--transport <IPC|MQTT [path]>` · `-t/--thing <name>`.
