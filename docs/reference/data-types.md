# Reference — Data Types

How telemetry values are represented once a route forwards them to a **file** sink (`stream:<name>`
whose stream has a `file` sink). The file sink lives in the shared `ggstreamlog` core; it decodes each
record and writes one of two schemas per [`mode`](configuration.md#file-sink-sinktype-file). For the
on-wire envelope see [messaging-interface.md](messaging-interface.md); for config see
[configuration.md](configuration.md).

The same JSON value typing applies whether the record came straight from an adapter or from the
processor's own [`ProcessedTelemetry`](messaging-interface.md#aggregate-output-processedtelemetry)
(the primary reducer rides in `samples[0].value`, so it lands in the typed value column like any
sample).

## `rows` schema — normalized typed telemetry

`mode: "rows"` decodes each record as a `SouthboundTagUpdate` and writes **one row per
`body.samples[]` element**. The polymorphic sample `value` lands in **sparse typed columns** — exactly
one of `valueDouble` / `valueLong` / `valueBool` / `valueString` is set, and `valueType` names which
(the EAV / historian pattern; crawls cleanly in Glue/BigQuery/Synapse).

| Column | Type (Parquet) | Source |
|--------|----------------|--------|
| `thing` | string (nullable) | `tags.thing` |
| `appId` | string (nullable) | `tags.appId` |
| `site` | string (nullable) | `tags.site` |
| `shop` | string (nullable) | `tags.shop` |
| `line` | string (nullable) | `tags.line` |
| `adapter` | string (nullable) | `body.device.adapter` |
| `instance` | string (nullable) | `body.device.instance` |
| `tagId` | string (nullable) | `body.tag.id` |
| `tagName` | string (nullable) | `body.tag.name` |
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

> **Parquet vs Avro.** In **Parquet** the value is the four sparse typed columns above + `valueType`.
> In **Avro** the same row uses a **true union** `value: ["null","double","long","boolean","string"]`
> (one `value` field, plus `valueType`) for faithful BigQuery load typing. The metadata columns are
> identical; in Avro the string columns are non-null with an empty-string default rather than nullable.

## `raw` schema

`mode: "raw"` writes **one row per message** with the payload kept opaque. It is also the
`_unmapped` fallback for a `rows`-mode payload that is **not** a `SouthboundTagUpdate` (not JSON, or no
`body.samples`) — such a payload is **never dropped**, it lands in a sibling `*_unmapped.<ext>` raw
file.

| Column | Type | Source |
|--------|------|--------|
| `offset` | int64 | the durable-buffer record offset |
| `partitionKey` | string | the resolved partition key (`publish.partitionKey`, default `body.tag.id`) |
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
**`(tagId, sourceTs)`**.
