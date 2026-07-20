# telemetry-processor — component notes

EdgeCommons **processing component** (Rust). Full name `com.mbreissi.edgecommons.TelemetryProcessor`,
crate/binary `telemetry-processor`. Depends on the `edgecommons` Rust library. If this repo lives
inside the EdgeCommons org umbrella workspace, read its root `AGENTS.md` first (org repo map,
design-fidelity contract, validation matrix, platform/transport model); everything below is this
component's own detail.

## What it is

The high-throughput northbound seam between southbound protocol adapters (which publish
`SouthboundSignalUpdate` telemetry on the local bus) and the cloud. It **subscribes** to configured
local topics, runs a declarative per-route **pipeline** — `filter` / `sample` / `aggregate` /
`project` / `script` (Rhai or Lua) — and **forwards** the result to `local`, `northbound`, or a
durable `stream:<name>` (Kinesis / Kafka / rolling Parquet-AVRO files). Runs on `GREENGRASS` / `HOST`
/ `KUBERNETES` via `edgecommons` — no platform branching in this component's own code.

## The seam

`src/proc/mod.rs`'s `Processor` trait is the one place stage logic lives: `process` handles an
inbound message and returns zero or more; `on_tick` lets a stateful stage (an aggregate window) emit
on a timer instead of on arrival. Everything above it — `src/app.rs` (the thin composition root:
obtains live handles from `EdgeCommons`, spawns workers), `src/route_build.rs` (route target/filter/
publish/script-output-topic resolution, the restamp policy, cross-route defaulting), `src/dispatch.rs`
(the self-echo guard + fan-out handler, command-verb + console-panel registration), and
`src/proc/route.rs` (the per-route worker + target dispatch) — is written against the trait and does
not change when a new stage is added. `src/proc/script.rs` implements the `script`/`filter.script`
stage over either engine (Rhai always compiled in; Lua behind the `scripting-lua` feature) and its
stateful multi-signal variant (`src/proc/multi.rs`). `src/app.rs` vs. `src/route_build.rs`/
`src/dispatch.rs` split along a testability line, not a logical one — see
[Validation expectations](#validation-expectations).

## Config location

This component's own settings live under `component.global` (cross-route defaults) /
`component.instances[]` (one **route** per instance) in the EdgeCommons config document —
`config.schema.json` is the contract `edgecommons component validate` checks `component.global`
against, and its `$defs.route`/`$defs.stage` describe each instance for documentation and forward
compatibility. The sibling sections (`tags`, `messaging`, `streaming`, `logging`, `heartbeat`,
`metricEmission`) are the standard `edgecommons` envelope, owned by the canonical schema and not
redeclared here. `test-configs/` carries two runnable examples (`config.json`,
`standalone-messaging.json`). See `docs/reference/configuration.md` for the full field-by-field
reference.

## Validation expectations

- `cargo test` covers the pipeline stages (`src/proc/*.rs`), route config (`src/config.rs`), route
  build decisions (`src/route_build.rs`), the route dispatcher (`src/proc/route.rs`), the fan-out
  handler + command/panel registration (`src/dispatch.rs`), and the metric/event surface
  (`src/observe.rs`) directly — no broker required.
- `cargo llvm-cov --fail-under-lines 90` is the coverage gate (`.github/workflows/ci.yml`'s
  `coverage` job) — the org rule is 90% line coverage per language. This repo has no live-infra
  *driver* seam of its own (the Kinesis/Kafka/file-sink clients live in the
  `edgecommons`/`edgestreamlog` library, not here); the seam it does have is narrower and different
  in kind — the **EdgeCommons composition root** — and it is kept **thin on purpose**: only the code
  that must obtain a live `Arc<dyn MessagingService>` (`gg.messaging()`), `EventsFacade`
  (`gg.events()`), `Arc<CommandInbox>` (`gg.commands()`), `Arc<dyn MetricService>` (`gg.metrics()`),
  `Arc<dyn StreamService>` (`gg.streams()`, stream targets only), or `gg.shutdown_signal()` stays in
  `src/app.rs` — none of those types has a public or test constructor outside the `edgecommons`
  crate, so there is no way to fabricate one. Every *decision* that does not itself need a live
  `EdgeCommons` was pulled out so it stays in the coverage denominator:
  - `src/route_build.rs` — cross-route defaulting (`resolve_global_wiring`), a route's target/
    filter/publish-topic resolution, script-output-topic validation, and the `local`-target restamp
    policy. All of it needs only a plain `Config`, which a test builds directly via
    `Config::from_value` (no live `EdgeCommons` required) — see its own module doc for the one
    exception (`gg.streams()`, which stays in `app.rs`'s `build_route`).
  - `src/dispatch.rs` — the self-echo guard + fan-out handler, the command verbs, the two console
    panels — unit-tested against a downstream `MessagingService` fake (`src/test_support.rs`, this
    crate's own analog of the library's crate-private `testutil::RecordingMessaging`) and a test-only
    recording `EvtEmitter` (`EvtEmitter::recording`, mirroring `file-replicator`'s
    `Events::recording_events` — `EventsFacade` likewise has no public constructor).

  The coverage job excludes exactly three files: `main.rs` (the runtime bootstrap shim), `app.rs`
  (the thin composition root above), and `test_support.rs` (test-only support code, not production
  logic — the same treatment `file-replicator` gives its own `testutil.rs`). Every other line —
  pipeline mechanics, route/config parsing, route-build decisions, the fan-out handler, dispatch/
  restamping, the command surface, the metric/event emitters — stays in the denominator and is
  unit-tested. Do not lower the gate or exclude testable code to pass it — add tests. If you add a
  branch to `app.rs` and it doesn't need a live `EdgeCommons` type to run, it almost certainly
  belongs in `route_build.rs` or `dispatch.rs` instead.
- The `scripting-lua` feature (Lua 5.4 via vendored `mlua`) is built and tested in CI alongside the
  default Rhai-only build, so both script engines are exercised.
- `edgecommons component validate` checks this repo's config against `config.schema.json` and warns
  if `Cargo.lock` is not committed.

## Org conventions this scaffold inherits

- A processor is **payload-agnostic**: it uses raw `messaging()`/`streams()`, never the `data()`
  facade (which mints its own topic from a signal id under its own bound identity — the wrong tool
  for republishing an already-built, possibly non-southbound-shaped message). `evt` health events do
  use the library's `events()` facade, where its identity/topic/body ownership is the right fit.
- Self-echo guard + identity restamp are load-bearing, not optional style: because the processor
  **consumes** the `data` class it also republishes onto (for `local` targets), the dispatcher
  restamps `local` output with the processor's own identity and the subscribe fan-out drops any
  inbound message whose identity matches that — without both halves, a `local` route would loop.
- A full route queue drops and counts; it never blocks the transport's dispatch task.
- Four-way parity: if this repo's Java/Python/TypeScript siblings exist, observable behavior should
  match — same config shape, same metric names, same command verbs.
- Builders/facades are the construction path (`messaging()`, `streams()`, `events()`, `commands()`,
  `MetricBuilder`) — never hand-built topics or envelopes.
- Runtime artifacts (vaults, parameter caches, generated streams, TLS certs, logs, build output,
  local broker state) stay out of Git.
- `Cargo.lock` is committed (SD-B, org-level lockfile-commit policy): regenerate it with the local
  `.cargo/config.toml` `[patch]` override **inactive** so it records the pinned git dependency, not a
  local path — a lock recorded against the path override does not resolve on a fresh clone or in CI.
