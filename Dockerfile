# Copyright 2026 MonoTS Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Multi-stage image for MonoTS (monots-server + monots CLI).
# Build:  docker build -t monots:latest .
#         make docker-build
# Run:    docker run --rm -p 50051:50051 -v monots-data:/opt/monots/data monots:latest
#         make docker-run
#         docker compose up -d

ARG RUST_VERSION=1.94.1
ARG DEBIAN_VERSION=bookworm

# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------
FROM rust:${RUST_VERSION}-${DEBIAN_VERSION} AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        pkg-config \
        libssl-dev \
        libsasl2-dev \
        libcurl4-openssl-dev \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

ENV RUSTFLAGS="-C opt-level=z -C strip=symbols"
RUN cargo build --release --locked -p server -p cli \
    && strip target/release/monots-server target/release/monots

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
FROM debian:${DEBIAN_VERSION}-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        libsasl2-2 \
        libcurl4 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 monots \
    && useradd --system --uid 10001 --gid monots --home-dir /opt/monots --shell /usr/sbin/nologin monots

WORKDIR /opt/monots

COPY --from=builder /build/target/release/monots-server /opt/monots/bin/monots-server
COPY --from=builder /build/target/release/monots /opt/monots/bin/monots
COPY conf/config.yaml /opt/monots/conf/config.yaml
COPY LICENSE NOTICE README.md /opt/monots/

RUN mkdir -p /opt/monots/data /opt/monots/logs \
    && chown -R monots:monots /opt/monots

ENV MONOTS_HOME=/opt/monots \
    MONOTS_CONF=/opt/monots/conf/config.yaml \
    MONOTS_DATA_DIR=/opt/monots/data \
    MONOTS_LOG_DIR=/opt/monots/logs \
    PATH="/opt/monots/bin:${PATH}"

USER monots
EXPOSE 50051
VOLUME ["/opt/monots/data", "/opt/monots/logs"]

ENTRYPOINT ["monots-server"]
CMD ["--config", "/opt/monots/conf/config.yaml"]
