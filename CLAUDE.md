# telemetry-processor (Claude Code)

EdgeCommons processing component (Rust), `com.mbreissi.edgecommons.TelemetryProcessor`. The full
picture — what this component is, the stage seam, config location, and the org conventions it
inherits — lives in `AGENTS.md` and is shared with every agent tool. It is imported here in full:

@AGENTS.md

## Local-dev notes

- **Default (committed pin):** `Cargo.toml`'s `edgecommons` dependency is a git `rev` pin — currently
  the `rust-lib/v0.5.0` release tag — and `Cargo.lock` is committed against that pin.
  `edgecommons/edgecommons` is public, so a plain `cargo build`/`clone` fetches it anonymously, in CI
  and locally.
- **Building against an unpushed sibling change:** add a gitignored `.cargo/config.toml` next to this
  file with a `[patch]` block pointing the git URL at your local `../core/libs/rust` checkout (see the
  comment above the `edgecommons` dependency in `Cargo.toml` for the exact form). This is local-dev
  only — CI never sees it, and it does not touch the committed `Cargo.lock`/pin. **Do not commit a
  `Cargo.lock` regenerated while that override is active** — it would record a local path, not the
  git pin, and would not resolve on a fresh clone or in CI.
