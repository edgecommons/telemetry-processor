# syntax=docker/dockerfile:1
#
# Build the telemetry-processor from source. Its `edgecommons` dependency is a git dependency on the
# private edgecommons/edgecommons repo, so the build needs SSH access to GitHub. Build with BuildKit and
# forward your SSH agent (or a CI deploy key):
#
#   DOCKER_BUILDKIT=1 docker build --ssh default \
#     --build-arg FEATURES=standalone,streaming,streaming-file-parquet \
#     -t telemetry-processor:dev .
#
# This builds from THIS repo only — no sibling checkout required (cargo fetches edgecommons over SSH).
ARG RUST_VERSION=1.85
FROM rust:${RUST_VERSION}-bookworm AS build
ARG FEATURES=standalone,streaming,streaming-file-parquet
RUN apt-get update \
 && apt-get install -y --no-install-recommends openssh-client git \
 && rm -rf /var/lib/apt/lists/*
# Trust github.com for the SSH fetch. The local-dev `.cargo/config.toml` (which carried the
# git-fetch-with-cli setting AND the sibling `[patch]`) is gitignored + dockerignored, so the
# in-container build resolves the pinned edgecommons git rev — force the git CLI transport here so it
# uses the forwarded SSH agent (libgit2 cannot).
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null
WORKDIR /src
COPY . .
RUN --mount=type=ssh cargo build --release --no-default-features --features "${FEATURES}"

FROM debian:bookworm-slim AS runtime
RUN useradd -r -u 10001 appuser && mkdir -p /data && chown appuser /data
COPY --from=build /src/target/release/telemetry-processor /usr/local/bin/telemetry-processor
USER appuser
WORKDIR /data
# The ConfigMap is mounted at /config; identity comes from the Downward API (POD_NAME) by default.
ENTRYPOINT ["telemetry-processor", "--platform", "KUBERNETES"]
