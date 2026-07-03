# Reference — Data Types

How telemetry values are represented once a route forwards them to a **file** sink (`stream:<name>`
whose stream has a `file` sink). The file sink lives in the shared `ggstreamlog` core; it writes one
of two schemas per [`mode`](configuration.md#file-sink-sinktype-file), and `rows` mode has two
**projections** — a built-in default and a config-declared one. For the on-wire envelope see
[messaging-interface.md](messaging-interface.md); for config see [configuration.md](configuration.md).

The same JSON value typing applies whether the record came straight from an adapter or from the
processor's own [`ProcessedTelemetry`](messaging-interface.md#aggregate-output-processedtelemetry)
(the primary reducer rides in `samples[0].value`, so it lands in the typed value column like any
sample).

<a id="rows-default-projection"></a>
## `rows` schema — default projection (normalized signal telemetry)

With `mode: "rows"` and **no `rows` config block**, the sink uses its built-in projection: it decodes
each record as a `SouthboundSignalUpdate` and writes **one row per `body.samples[]` element**. The
polymorphic sample `value` lands in **sparse typed columns** — exactly one of `valueDouble` /
`valueLong` / `valueBool` / `valueString` is set, and `valueType` names which (the EAV / historian
pattern; crawls cleanly in Glue/BigQuery/Synapse).

The columns, **in file order**:

| Column | Type (Parquet) | Source |
|--------|----------------|--------|
| `tags` | string (nullable) | the **whole envelope `tags` object** as compact JSON (null when the message has no `tags` object) — business metadata; the source **device** lives in the top-level `identity` element, not here |
| `signalId` | string (nullable) | `body.signal.id` |
| `signalName` | string (nullable) | `body.signal.name` |
| `adapter` | string (nullable) | `body.device.adapter` |
| `instance` | string (nullable) | `body.device.instance` |
| `valueDouble` | double (nullable) | `samples[].value` when fractional/non-integral number |
| `valueLong` | int64 (nullable) | `samples[].value` when an integral number |
| `valueBool` | boolean (nullable) | `samples[].value` when boolean |
| `valueString` | string (nullable) | `samples[].value` when string (or stringified array/object) |
| `valueType` | string (**non-null**) | discriminator: `double` \| `long` \| `boolean` \| `string` \| `null` |
| `quality` | string (nullable) | `samples[].quality` |
| `qualityRaw` | string (nullable) | `samples[].qualityRaw` |
| `sourceTs` | string (nullable) | `samples[].sourceTs` (ISO-8601 UTC) |
| `serverTs` | string (nullable) | `samples[].serverTs` (ISO-8601 UTC) |
| `tsMs` | int64 (**non-null**) | stream record timestamp (Unix ms) — the processor's receive time |
| `offset` | int64 (**non-null**) | the durable-buffer record offset |

> **The envelope `tags` are one JSON column, not fixed columns.** The ggcommons message-envelope
> `tags` is an *open* metadata map — a deployment may carry `site`/`shop`/`line`, or an entirely
> different set (the source device is **not** here — it travels in the top-level `identity` element,
> the `tags.thing` replacement). Rather than freezing those into named columns, the default projection lands
> the entire object in one `tags` column as compact JSON, so a lake query reads any key
> (`json_extract(tags, '$.site')` in Athena, `tags:site` in Snowflake, `JSON_VALUE` in BigQuery).
> If you want a specific tag as its own typed column, declare a [user projection](#rows-user-projection).
> The `tags` column is the message-envelope metadata — **not** the southbound *signal* (which is the
> data point in `signalId`/`signalName`); see the [terminology note](messaging-interface.md#envelope-tags-vs-the-signal).

> **Parquet vs Avro.** In **Parquet** the value is the four sparse typed columns above + `valueType`.
> In **Avro** the same row uses a **true union** `value: ["null","double","long","boolean","string"]`
> (one `value` field, plus `valueType`) for faithful BigQuery load typing. The metadata columns are
> identical; in Avro the string columns are non-null with an empty-string default rather than nullable.

<a id="rows-user-projection"></a>
## `rows` schema — user projection (declared columns)

When `mode: "rows"` carries a [`rows` config block](configuration.md#file-sink-sinktype-file), the
file's schema is **fixed from your column list at open time** — the projection is payload-agnostic and
makes no assumption about a southbound shape. Each column is a `name`, a dotted JSON `path` into the
message, and a target `type`; the resolver walks `body.`/`tags.`/`header.` roots and a missing or
type-incompatible value becomes a **null cell** (a user projection is *never* routed to `_unmapped`).

| `type` | Parquet column | Coercion |
|--------|----------------|----------|
| `string` (default) | string (nullable) | strings as-is; numbers/bools stringified; objects/arrays compact-JSON |
| `long` | int64 (nullable) | integral as-is; a fractional number is **truncated**; non-numbers → null |
| `double` | double (nullable) | any number; non-numbers → null |
| `bool` | boolean (nullable) | JSON booleans; non-booleans → null |
| `json` | string (nullable) | the resolved value serialized as compact JSON (use for an object/array such as the whole `tags`) |

**`explode`** turns an array into one row per element: set `explode` to the array's path, then any
column whose `path` begins with `<explode>[]` resolves against the *current element* while every other
column resolves against the whole message. With no `explode`, a projection emits exactly one row per
message. To reproduce the default per-sample fan-out for a southbound payload you would
`explode: "body.samples"` and reference `body.samples[].value`, `body.samples[].quality`, etc.

```jsonc
"sink": {
  "type": "file", "format": "parquet", "mode": "rows", "dir": "/data/archive",
  "rows": {
    "explode": "body.samples",
    "columns": [
      { "name": "signalId", "path": "body.signal.id" },
      { "name": "site",     "path": "tags.site" },
      { "name": "value",    "path": "body.samples[].value", "type": "double" },
      { "name": "quality",  "path": "body.samples[].quality" },
      { "name": "sourceTs", "path": "body.samples[].sourceTs" },
      { "name": "tags",     "path": "tags", "type": "json" }
    ]
  }
}
```

## `raw` schema

`mode: "raw"` writes **one row per message** with the payload kept opaque. It is also the
`_unmapped` fallback for a **default-projection** `rows`-mode payload that is **not** a
`SouthboundSignalUpdate` (not JSON, or no `body.samples`) — such a payload is **never dropped**, it
lands in a sibling `*_unmapped.<ext>` raw file. (A [user projection](#rows-user-projection) has no
`_unmapped` fallback — unmatched paths become null cells instead.)

| Column | Type | Source |
|--------|------|--------|
| `offset` | int64 | the durable-buffer record offset |
| `partitionKey` | string | the resolved partition key (`publish.partitionKey`, default `body.signal.id`) |
| `tsMs` | int64 | stream record timestamp (Unix ms) |
| `payload` | string | the full message bytes (lossy-UTF-8) |

## JSON value → typed column

A sample `value` is narrowed to one typed column:

| JSON `value` | `valueType` | Column set |
|--------------|-------------|------------|
| integral number (fits int64) | `long` | `valueLong` |
| non-integral / fractional number | `double` | `valueDouble` |
| boolean | `boolean` | `valueBool` |
| string | `string` | `valueString` |
| `null` (or absent) | `null` | *(all value columns null)* |
| array / object | `string` | `valueString` (compact-JSON stringified) |

> Integers use the int64 range; a consumer whose JSON parser uses IEEE-754 doubles (e.g. JavaScript)
> may lose precision for `|value| > 2^53`. An unsigned value above `2^63` is cast into the signed
> int64 column.

> **Array values** land in `valueString` as compact JSON (`valueType = "string"`) under the default
> projection. To spread an array into one row per element, or to fold it in `aggregate`, filter, or a
> script instead, see [Handle array-valued signals](../how-to-guides.md#handle-array-valued-signals).

## Quality & timestamps

- **`quality`** is the normalized, protocol-independent verdict — `GOOD` \| `BAD` \| `UNCERTAIN`
  (`docs/SOUTHBOUND.md` §3) — passed through verbatim into the `quality` column.
  `ProcessedTelemetry` (aggregate output) sets `quality = "GOOD"`.
- **`qualityRaw`** preserves the native status code (e.g. an OPC UA `StatusCode`, a Modbus exception)
  for diagnostics.
- **`sourceTs`** (device/field) and **`serverTs`** (protocol server) are ISO-8601 UTC **strings**,
  carried verbatim; either may be absent (→ null / empty). `tsMs` is distinct: the integer Unix-ms
  time the **processor** received the record, set by the stream, not by the device.

<a id="aggregate-agg-types"></a>
## Aggregate `agg` value types

The `agg` map in a `ProcessedTelemetry` body carries one entry per configured reducer:

| Reducer | JSON type | Notes |
|---------|-----------|-------|
| `avg` | number (double) | numeric samples only; `null` if the window had no numeric sample |
| `sum` | number (double) | numeric samples only; `null` if none numeric |
| `min` / `max` | number (double) | numeric samples only; `null` if none numeric |
| `count` | integer | count of **all** messages folded into the window (numeric or not) |
| `first` / `last` | the sample value's JSON type | the raw first / last sample value (any JSON type), or `null` if the window was empty |

> `samples[0].value` repeats the **primary** reducer (the first-listed `fn`), so file archiving and
> any consumer reading `samples[].value` see the headline number; the full reducer set is under `agg`.

## Duplicates & de-duplication

The file sink is part of the at-least-once streaming pipeline: a record re-delivered after a crash
between sink-write and buffer-commit can appear twice. De-duplicate downstream on
**`(signalId, sourceTs)`**.
