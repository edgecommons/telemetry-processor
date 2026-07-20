# DESIGN — telemetry-processor

> Treat this document as the **design-fidelity contract** for this component: before changing
> behavior, update the relevant section here in the same change, and review new work against what
> is written here — not against a summary of it.

## What it is

`com.mbreissi.edgecommons.TelemetryProcessor` is the reference Rust **processing** component: it
subscribes to configured local topics, runs a declarative per-route pipeline of stages
(`filter` / `sample` / `aggregate` / `project` / `script`), and forwards each result to `local`,
`northbound`, or a durable `stream:<name>` (Kinesis / Kafka / rolling Parquet-AVRO files). It is the
high-throughput northbound seam between southbound protocol adapters and the cloud.

## Decisions

Numbered `D-TP-<n>` so later sessions can cite them.

- **D-TP-1. Payload-agnostic core.** The processor does not require the `SouthboundSignalUpdate`
  shape. Any JSON body matching a route's `subscribe` filter flows through; the southbound shape is
  only a *convention* the built-in conveniences default to (the `quality` filter shorthand, the
  default key `body.signal.id`, `body.samples[]` aggregation, the file sink's default rows
  projection). Each is overridable per route. This is why the dispatcher uses raw
  `messaging()`/`streams()` rather than the `data()` facade, which mints its own topic under its own
  bound identity and forces a `SouthboundSignalUpdate` shape — wrong for a processor that republishes
  an already-built message that may not be southbound-shaped at all (see
  `docs/reference/messaging-interface.md`'s "Why this dispatch uses `messaging()`/`streams()`").
- **D-TP-2. External script files + the multi-signal `script` stage.** A `script`/`filter.script`
  stage source is inline or `{"file": "path"}` (resolved against `global.defaults.scriptsDir`),
  compiled once at startup. The object form additionally supports named, stateful `inputs` (a
  selector per input; the stage caches the latest observation of each and evaluates on any matched
  input's value/quality change, binding the full snapshot as `inputs` and the firing input as
  `trigger`) and an explicit `output` (publishes a new envelope on its own topic instead of mutating
  the triggering message in place, correlated back via `header.correlation_id`). This shipped to let
  a single script combine several southbound signals (e.g. an OEE calculation) without a bespoke
  join stage. Cached input state is partitioned by source device so two devices publishing the same
  signal ids never mix into one snapshot.
- **D-TP-3. Envelope `tags` as a JSON column, not exploded.** The default `rows`-mode file
  projection lands the envelope's `tags{}` map as one JSON column rather than one column per tag key
  — tags are an open, per-deployment set, so a fixed schema cannot enumerate them, and a JSON column
  keeps the Parquet/Avro schema stable across configs. A declared `rows` user projection can still
  pull an individual tag value out via its own `path`.
- **D-TP-4. Dispatch bypasses the `data()` facade (see D-TP-1).** `data()` always mints the topic
  from its own bound instance identity (breaking the documented `local`/`northbound` "default = the
  source topic" bridge), forces the `SouthboundSignalUpdate` header/body shape (which would break
  `ProcessedTelemetry`/`project`-reshaped bodies), and always stamps its own identity — right for
  `local` (which the self-echo guard depends on) but wrong for `northbound`/`stream`, which must
  preserve the source identity for provenance.
- **D-TP-5. Self-echo guard is two mechanisms, not one.** (a) A `local` republish restamps the output
  envelope's identity to the processor's own (instance = the route id); (b) the fan-out handler drops
  any inbound message whose identity device+component match the processor's own. Removing either
  breaks the guard: without (a) a re-consumed `local` output would still carry the *source's*
  identity and slip past (b); without (b) restamping alone does nothing to stop the loop.
- **D-TP-6 (this remediation, `#11`/`#9`). `Cargo.lock` is committed (org policy SD-B).** The
  `/Cargo.lock` `.gitignore` entry and its now-stale rationale ("excluded until the pin is bumped to
  the UNS-core rev") were removed; the lockfile was regenerated with the local `.cargo/config.toml`
  `[patch]` override **inactive**, so it records the pinned git dependency
  (`edgecommons` rev `36a70c48…`) rather than a local path — valid on a fresh clone and in CI. See
  `CLAUDE.md` for the local-dev override that must stay inactive when regenerating this file.
  License metadata was also reconciled: `Cargo.toml`'s `license` field was `Apache-2.0` (a stale
  leftover) against the actual `BUSL-1.1` `LICENSE` file; fixed to `license = "BUSL-1.1"` (`#9`).
- **D-TP-7 (this remediation, revised). `app.rs` vs. `route_build.rs`/`dispatch.rs` is a testability
  boundary, kept deliberately thin, not a broad "the whole wiring file" carve-out.** The first cut of
  this split (excluding all of `app.rs`) was too broad — `build_route` still contained testable
  decision branches (target/filter/publish-topic resolution, script-output-topic validation, the
  restamp policy), all of which need only a plain `Config` (publicly constructible via
  `Config::from_value`), not a live `EdgeCommons`. Those moved to a new `src/route_build.rs`
  (`resolve_global_wiring`, `resolve_target`, `resolve_filters`, `resolve_publish_topic`,
  `resolve_script_output_topics`, `compute_restamp`), unit-tested directly against
  `Config::from_value`-built configs — no fakes needed, since `Config` itself has a public test
  constructor. What remains in `src/app.rs` is only the code that must obtain a live
  `Arc<dyn MessagingService>` (`gg.messaging()`), `EventsFacade` (`gg.events()`),
  `Arc<CommandInbox>` (`gg.commands()`), `Arc<dyn MetricService>` (`gg.metrics()`),
  `Arc<dyn StreamService>` (`gg.streams()`, stream targets only — `build_route`'s one remaining
  live dependency), or `gg.shutdown_signal()` — none of those types has a public or test constructor
  outside the `edgecommons` crate (`EventsFacade::new` and `CommandInbox::new` are both `pub(crate)`
  in the library), so `app.rs` genuinely cannot be unit-tested without a live runtime. The CI
  coverage job excludes it (with `main.rs`) for exactly this reason, mirroring the "supervisor.rs"
  seam pattern the protocol-adapter templates use for a genuinely different reason (a live device
  driver). The self-echo guard + fan-out handler, the `get-stats`/`flush`/`pause`/`resume` command
  verbs, and the two console panels live in `src/dispatch.rs`, unit-tested against a downstream
  `MessagingService` fake (`src/test_support.rs`, since the library's own
  `testutil::RecordingMessaging` is `pub(crate)` and unreachable from this crate) and a test-only
  recording `EvtEmitter` (`EvtEmitter::recording`, added to `src/observe.rs` for the same reason —
  `EventsFacade` has no public constructor — mirroring `file-replicator`'s `Events::recording_events`
  precedent for the identical constraint). `src/test_support.rs` itself is excluded from the coverage
  denominator (test-only support code, not product logic — the same treatment `file-replicator` gives
  `testutil.rs`).
- **D-TP-8 (this remediation). Console panels are an enhancement, implemented.** Issue `#11`'s P2-6
  flagged panels as optional for processors (not a baseline requirement). Two `scope: "component"`
  panels (`overview`: fleet totals + flush/pause/resume; `routes`: per-route counters via
  `get-stats`) were straightforward to add via the library's existing `commands.register_panel` (used
  identically by the protocol-adapter templates) and ride the command surface that already shipped —
  so they were implemented rather than deferred. `scope: "component"`, not `"instance"`: the
  processor has no console-facing UNS instance dimension (a route is internal wiring, addressed by an
  optional `body.route` field on the command verbs, not a topic segment), unlike a southbound
  adapter's per-device instances.

## Config

`config.schema.json` is the source of truth for `component.global`'s shape (checked by
`edgecommons component validate`); its `$defs.route`/`$defs.stage` describe each
`component.instances[]` entry for documentation and forward compatibility (the CLI tool does not yet
validate `instances[]` against a component schema — see `ec-validate`'s `schema.rs` — so these
`$defs` are not yet enforced, only documented). `docs/reference/configuration.md` is the authoritative
field-by-field prose it was derived from; treat that page, not this one, as the shape reference.

## Command surface

Beyond the library's automatic `ping` / `reload-config` / `get-configuration`:

| Verb | Body | Result |
|------|------|--------|
| `get-stats` | — | Per-route counters (`in`/`out`/`dropped`/`streamAppends`/`publishFailures`/`queueDepth`/`paused`). |
| `flush` | — | Force-closes every route's open **time** windows now; `{flushed: n}`. Count windows are unaffected. |
| `pause` | `{route?}` | Stops enqueuing to a route (or all routes when omitted); `{paused: [ids]}`. |
| `resume` | `{route?}` | The inverse of `pause`; `{resumed: [ids]}`. |

Two edge-console panels (`overview`, `routes`; see D-TP-8) bind to these verbs. Full wire contract in
`docs/reference/messaging-interface.md`.

## Metrics

`metric/pipeline` (`messagesIn`/`messagesOut`/`messagesDropped`/`streamAppends`/`publishFailures`),
summed across routes and emitted as interval deltas every 30s via `gg.metrics()`. Per-route detail is
`get-stats`, not a metric dimension. See `docs/reference/metrics.md`.

## Validation

- `cargo test` (107 tests as of this remediation) — pipeline mechanics, route config parsing,
  route-build decisions (target/filter/publish/script-output-topic resolution, the restamp policy —
  `src/route_build.rs`), the fan-out handler + command/panel registration, the route dispatcher
  (local/northbound/stream targets, restamp, failure→evt), the metric/event surface. No broker
  required.
- `cargo llvm-cov --fail-under-lines 90` — the coverage gate; see D-TP-7 for exactly what is excluded
  and why. Measured at 92.95% lines on this remediation's changeset, identical on native Windows and
  WSL/Linux (CI runs on `ubuntu-latest`).
- `cargo clippy --all-targets -- -D warnings` — clean.
- Live-infra validation (HOST/dual-MQTT, GG-lab, Kubernetes) per the org validation matrix is not
  re-run by this remediation — it is a hygiene/CI-shaped change (lockfile, coverage gate, schema,
  governance docs, license, a testability-only source split, and two additive console panels), not a
  behavioral or wire-contract change. The `test-configs/` samples validate cleanly against
  `config.schema.json` (see the PR description for the validation transcript).

## Open items

- `edgecommons component validate` currently checks only `component.global` against a component's
  schema, not `component.instances[]`/`$defs.route` (see `ec-validate`'s `schema.rs` module doc) —
  this is a CLI-side gap, not specific to this repo; the `$defs` in `config.schema.json` are ready for
  when that lands.
