# Reference - Metrics

The telemetry-processor emits pipeline throughput metrics through the EdgeCommons metric service. With
`metricEmission.target: messaging`, metrics are published on the reserved UNS `metric` class:

```text
ecv1/{device}/telemetry-processor/main/metric/pipeline
```

The processor does not publish directly to reserved `metric` topics. It defines and emits the metric
through `gg.metrics()`, so the same metric name, measures, units, and dimensions are used by messaging,
CloudWatch, and Prometheus targets.

## Dimension model

The `pipeline` metric is intentionally fleet-level for the processor instance. It uses runtime-injected
component dimensions and does not include route ids as CloudWatch dimensions. Per-route details are
available through the `get-stats` command.

This keeps the CloudWatch dimension set stable even when routes are added, removed, or renamed.

## Emission model

The processor emits `pipeline` every 30 seconds. Measures are interval deltas summed across all routes,
not lifetime totals. A restarted process never emits negative deltas; counters saturate at zero when a
previous snapshot is greater than the current snapshot.

System measures such as CPU and memory are emitted separately by the EdgeCommons runtime as the `sys`
metric. This page describes only the processor's custom non-system metric.

## `pipeline`

Fleet-level route throughput and dispatch health.

Dimensions: runtime-injected component dimensions only.

| Measure | Unit | Purpose |
|---|---:|---|
| `messagesIn` | Count | Messages accepted into route worker queues during the interval. Helps measure inbound telemetry load. |
| `messagesOut` | Count | Messages successfully forwarded or appended during the interval. Helps measure successful pipeline output. |
| `messagesDropped` | Count | Messages dropped because route queues were full. Helps detect backpressure and undersized `maxQueue` settings. |
| `streamAppends` | Count | Records appended to durable stream targets. Helps confirm stream-backed routes are producing output. |
| `publishFailures` | Count | Local/northbound publish failures and stream append failures. Helps detect downstream broker, IoT Core, or stream sink problems. |

## Related command data

Use the `get-stats` command for per-route counters:

```text
ecv1/{device}/telemetry-processor/main/cmd/get-stats
```

The command returns route ids, cumulative in/out/drop counts, stream appends, publish failures,
queue depth, and paused state. Use it when you need route-level diagnosis that would be too
high-cardinality for CloudWatch metric dimensions.
