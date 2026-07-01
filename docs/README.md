# Telemetry Processor — Documentation

`com.mbreissi.greengrass.TelemetryProcessor` subscribes to local telemetry topics, runs a declarative
per-route pipeline — **filter, sample, aggregate, project, Rhai script** — and forwards the result to a
target: republish on the local bus, send northbound to MQTT / AWS IoT Core, or append to a durable
stream that lands in **Kinesis, Kafka, or rolling Parquet/AVRO files**. Built on the `ggcommons`
library, it is the high-throughput seam between southbound protocol adapters and the cloud, and runs
wherever you deploy it — as a Greengrass v2 component, a standalone process, or a Kubernetes pod.

| Doc | Start here when you want to… |
|-----|------------------------------|
| **[Tutorial](tutorial.md)** | learn by doing — bring the processor up against a local broker and watch it downsample and archive telemetry, end to end |
| **[How-to guides](how-to-guides.md)** | accomplish a specific task — filter, downsample, window-aggregate, archive to Parquet, forward alarms northbound, deploy |
| **[Reference](reference/)** | look up an exact option, topic, payload, or column type |
| **[Explanation](explanation.md)** | understand how it works and why — the route/worker model, the processing-and-timing pipeline, targets and the file sink |

## Quick routing

- **"I'm new here."** → [Tutorial](tutorial.md).
- **"What does this config option do?"** → [Reference — Configuration](reference/configuration.md).
- **"What message do I subscribe to / publish, and on which topic?"** → [Reference — Messaging Interface](reference/messaging-interface.md).
- **"How are values stored in the Parquet / AVRO files?"** → [Reference — Data Types](reference/data-types.md).
- **"How do I downsample a fast signal or aggregate per signal?"** → [How-to guides](how-to-guides.md).
- **"How do I control file rotation — by size, time, or count?"** → [How-to guides](how-to-guides.md).
- **"Why is my data too fast / slow / laggy, and how does windowing work?"** → [Explanation — the processing-and-timing pipeline](explanation.md).
- **"How does one route share a topic with another?"** → [Explanation — one route, one worker](explanation.md).

## Audience

These docs are for **integrators and operators** — people who deploy the processor and write the
adapters or clients that produce and consume its messages. They do not cover modifying the
processor's own source.
