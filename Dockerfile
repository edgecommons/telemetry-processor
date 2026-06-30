# Multi-stage build for the telemetry-processor component (HOST / KUBERNETES platforms).
#
# NOTE: this component depends on the ggcommons Rust library via a path dependency during local
# development, so the Docker build context must contain BOTH this repo and the ggcommons monorepo as
# siblings. Build from the parent `source/` directory:
#
#   docker build -f telemetry-processor/Dockerfile \
#     -t telemetry-processor:dev \
#     --build-arg FEATURES=standalone,streaming,streaming-file-parquet .
#
# A published build instead uses the registry/git ggcommons dependency and a normal single-repo
# context.
ARG RUST_VERSION=1.85
FROM rust:${RUST_VERSION}-bookworm AS build
ARG FEATURES=standalone,streaming,streaming-file-parquet
WORKDIR /src
# Bring in the workspace (telemetry-processor + sibling ggcommons-monorepo for the path dep).
COPY ggcommons-monorepo /src/ggcommons-monorepo
COPY telemetry-processor /src/telemetry-processor
WORKDIR /src/telemetry-processor
RUN cargo build --release --no-default-features --features "${FEATURES}"

FROM debian:bookworm-slim AS runtime
RUN useradd -r -u 10001 appuser && mkdir -p /data && chown appuser /data
COPY --from=build /src/telemetry-processor/target/release/telemetry-processor /usr/local/bin/telemetry-processor
USER appuser
WORKDIR /data
# The ConfigMap is mounted at /config; identity comes from the Downward API (POD_NAME) by default.
ENTRYPOINT ["telemetry-processor", "--platform", "KUBERNETES"]
