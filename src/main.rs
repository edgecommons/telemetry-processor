//! # telemetry-processor — entry point
//!
//! The reference Rust **processing** component for the edgecommons / edgecommons ecosystem. It
//! subscribes to configured local telemetry topics, runs a declarative per-route pipeline
//! (`filter` / `sample` / `aggregate` / `project` / `script`), and forwards the result to a target
//! (`local` / `northbound` / `stream:<name>` → Kinesis / Kafka / rolling Parquet-AVRO files).
//!
//! Routes are `component.instances[]` entries; cross-route defaults live in `component.global`. The
//! standard edgecommons CLI contract (`-c` / `--platform` / `--transport` / `-t`) is parsed by the
//! library. See `docs/TELEMETRY_PROCESSOR.md` in the edgecommons monorepo.
//!
//! ## Run locally (HOST platform, MQTT transport)
//! ```bash
//! cargo run --features standalone,streaming,streaming-file-parquet -- \
//!   --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
//!   -c FILE ./test-configs/config.json -t my-thing
//! ```

mod app;
mod config;
mod json_path;
mod observe;
mod proc;

use edgecommons::prelude::*;

/// The component's full name (matches `recipe.yaml` / `gdk-config.json`). Its sanitized UNS
/// component token is the segment after the last `.` — `telemetry-processor` (D-U18) — so the
/// processor appears on the bus as `ecv1/{device}/telemetry-processor/{instance}/{class}[/channel]`,
/// matching the repo/registry/console name (the `uns-bridge` naming precedent: `com.mbreissi.edgecommons.UnsBridge`).
const COMPONENT_NAME: &str = "com.mbreissi.edgecommons.TelemetryProcessor";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let gg = EdgeCommonsBuilder::new(COMPONENT_NAME).args(std::env::args_os()).build().await?;

    tracing::info!(
        component = gg.component_name(),
        thing = %gg.config().thing_name,
        "telemetry-processor starting"
    );

    let app = app::ProcessorApp::start(&gg).await?;
    app.run(&gg).await?;

    tracing::info!("telemetry-processor stopped");
    Ok(())
}
