#!/usr/bin/env bash
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

# Package MonoTS for a Docker/OCI platform using Buildx + QEMU.
# The build runs *inside* the target architecture (emulated), so native
# deps (cmake / librdkafka / openssl) compile without a cross toolchain.
#
# Usage:
#   ./scripts/package-docker-platform.sh linux/arm/v7 armv7-unknown-linux-gnueabihf
#   ./scripts/package-docker-platform.sh linux/riscv64 riscv64gc-unknown-linux-gnu
#
# Outputs:
#   dist/monots-<version>-<triple>.{tar.gz,zip}

set -euo pipefail

PLATFORM="${1:?platform required, e.g. linux/arm/v7}"
TRIPLE="${2:?rust triple required, e.g. armv7-unknown-linux-gnueabihf}"
DOCKERFILE="${3:-}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BUILD_ARGS=(--build-arg "RUST_VERSION=1.94.1")

if [[ -z "${DOCKERFILE}" ]]; then
  case "${PLATFORM}" in
    linux/riscv64|linux/ppc64le|linux/s390x)
      # Official debian:bookworm has no riscv64; trixie does.
      DOCKERFILE="${ROOT}/Dockerfile.package.debian"
      BUILD_ARGS+=(--build-arg "DEBIAN_VERSION=trixie")
      ;;
    *)
      DOCKERFILE="${ROOT}/Dockerfile.package"
      ;;
  esac
fi

VERSION="$(grep '^version' Cargo.toml | head -1 | awk -F '"' '{print $2}')"
NAME="monots-${VERSION}-${TRIPLE}"
OUT_DIR="${ROOT}/dist/${NAME}"
STAGE="$(mktemp -d "${TMPDIR:-/tmp}/monots-pkg.XXXXXX")"
trap 'rm -rf "${STAGE}"' EXIT

echo "[-] Docker package: platform=${PLATFORM} triple=${TRIPLE}"
echo "[-] Dockerfile: ${DOCKERFILE}"

docker buildx version >/dev/null
docker buildx build \
  --platform "${PLATFORM}" \
  --file "${DOCKERFILE}" \
  "${BUILD_ARGS[@]}" \
  --target export \
  --output "type=local,dest=${STAGE}" \
  "${ROOT}"

mkdir -p "${OUT_DIR}/bin" "${OUT_DIR}/conf" "${OUT_DIR}/data" "${OUT_DIR}/logs"
cp "${STAGE}/monots-server" "${STAGE}/monots" "${OUT_DIR}/bin/"
cp "${ROOT}/scripts/start-server.sh" "${ROOT}/scripts/start-cli.sh" "${OUT_DIR}/bin/"
chmod +x "${OUT_DIR}/bin/"*
cp "${ROOT}/conf/config.yaml" "${OUT_DIR}/conf/config.yaml"
cp "${ROOT}/README.md" "${ROOT}/LICENSE" "${ROOT}/NOTICE" "${OUT_DIR}/"
{
  echo "Name: monots"
  echo "Version: ${VERSION}"
  echo "Triple: ${TRIPLE}"
  echo "Platform: ${PLATFORM}"
  echo "Date: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
} > "${OUT_DIR}/manifest.txt"

mkdir -p "${ROOT}/dist"
(
  cd "${ROOT}/dist"
  tar -czf "${NAME}.tar.gz" "${NAME}"
  zip -rq "${NAME}.zip" "${NAME}"
)

echo "[✔] Ready: dist/${NAME}.tar.gz"
