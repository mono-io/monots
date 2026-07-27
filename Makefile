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

APP_NAME     := monots
SERVER_CRATE := server
CLI_CRATE    := cli
SERVER_BIN   := monots-server
CLI_BIN      := monots
DOCKER_IMAGE ?= monots:latest
DOCKER_PORT  ?= 50051

# Version from root `[workspace.package]` (single source of truth).
VERSION      := $(shell grep '^version' Cargo.toml | head -1 | awk -F '"' '{print $$2}')
DATE         := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

# Auto-detect system environment & normalize architecture
RAW_ARCH := $(shell uname -m)
ifeq ($(RAW_ARCH), arm64)
    ARCH := aarch64
else ifeq ($(RAW_ARCH), amd64)
    ARCH := x86_64
else
    ARCH := $(RAW_ARCH)
endif

OS          := $(shell uname -s | tr '[:upper:]' '[:lower:]')
OS_NAME     := $(shell uname -s)

DIST_ROOT   := dist
ifeq ($(OS_NAME), Linux)
    TRIPLE := $(ARCH)-unknown-linux-gnu
    STATIC_FLAGS :=
else ifeq ($(OS_NAME), Darwin)
    TRIPLE := $(ARCH)-apple-darwin
    STATIC_FLAGS :=
else ifneq (,$(findstring MINGW,$(OS_NAME))$(findstring MSYS,$(OS_NAME)))
    TRIPLE := $(ARCH)-pc-windows-msvc
    STATIC_FLAGS := -C target-feature=+crt-static
else
    TRIPLE := $(ARCH)-unknown-linux-gnu
    STATIC_FLAGS :=
endif

OPTIMIZE_FLAGS := -C opt-level=z -C strip=symbols $(STATIC_FLAGS)

TARGET_DIR  := target/$(TRIPLE)/release
HOST_BIN_DIR := target/release

FULL_NAME   := $(APP_NAME)-$(VERSION)
FULL_PATH   := $(DIST_ROOT)/$(FULL_NAME)

C_R := \033[0;31m
C_G := \033[0;32m
C_B := \033[0;34m
C_Y := \033[0;33m
C_0 := \033[0m

log = @printf "$(C_B)[-]$(C_0) %-15s %s\n" "$(1)" "$(2)"
success = @printf "$(C_G)[✔]$(C_0) %s\n" "$(1)"

.PHONY: all help build build-host dev test integration-test clean dist run-server run-cli \
	docker-build docker-run docker-up docker-down fmt fmt-check check check-license \
	.check-env .ensure-target

all: build-host

help:
	@echo "Usage: make [TARGET]"
	@echo ""
	@echo "  build       Build release binaries (cross-target: $(TRIPLE))"
	@echo "  build-host  Build release binaries for current host (faster local dev)"
	@echo "  dev         Debug build (current host)"
	@echo "  test                Run workspace unit tests (excludes integration)"
	@echo "  integration-test    Run Rust integration tests (tests/integration)"
	@echo "  dist        Package release layout to dist/$(FULL_NAME).{tar.gz,zip}"
	@echo "  check       License header check + cargo check"
	@echo "  check-license  Verify Apache-2.0 copyright headers on source files"
	@echo "  fmt         cargo fmt"
	@echo "  fmt-check   cargo fmt --check (CI)"
	@echo "  run-server  Start server via scripts/start-server.sh (host build)"
	@echo "  run-cli     Start interactive CLI"
	@echo "  docker-build  Build Docker image ($(DOCKER_IMAGE))"
	@echo "  docker-run    Run container (port $(DOCKER_PORT), named volume for data)"
	@echo "  docker-up     docker compose up -d --build"
	@echo "  docker-down   docker compose down"
	@echo "  clean       Remove build artifacts, dist, data, logs"
	@echo ""
	@echo "  Version: $(VERSION) | Arch: $(ARCH) | OS: $(OS)"
	@echo ""
	@echo "Project layout (FunctionStream robot-branch style):"
	@echo "  src/{common,catalog,storage,query,core,server}/  — Rust crates"
	@echo "  sdk/  — gRPC client SDK"
	@echo "  proto/ cli/cli/ tests/integration/ benchmark/        — workspace members"
	@echo "  conf/ scripts/ dist/ data/ logs/"

.check-env:
	@command -v cargo >/dev/null 2>&1 || { printf "$(C_R)[X] Cargo not found$(C_0)\n"; exit 1; }

check-license:
	$(call log,LICENSE,scripts/check-license-headers.sh)
	@bash scripts/check-license-headers.sh

.ensure-target:
	@rustup target list --installed | grep -q "$(TRIPLE)" || \
	(printf "$(C_Y)[!] Auto-installing target toolchain: $(TRIPLE)$(C_0)\n" && \
	 rustup target add $(TRIPLE))

build: .check-env check-license .ensure-target
	$(call log,BUILD,Server + CLI [$(OS_NAME) / $(TRIPLE)])
	@RUSTFLAGS="$(OPTIMIZE_FLAGS)" \
	cargo build --release \
		--target $(TRIPLE) \
		-p $(SERVER_CRATE) -p $(CLI_CRATE) \
		--quiet
	$(call success,Binaries: $(TARGET_DIR)/$(SERVER_BIN) $(TARGET_DIR)/$(CLI_BIN))

build-host: .check-env check-license
	$(call log,BUILD,Server + CLI [host / release])
	@RUSTFLAGS="$(OPTIMIZE_FLAGS)" \
	cargo build --release -p $(SERVER_CRATE) -p $(CLI_CRATE) --quiet
	$(call success,Binaries: $(HOST_BIN_DIR)/$(SERVER_BIN) $(HOST_BIN_DIR)/$(CLI_BIN))

dev: .check-env check-license
	$(call log,BUILD,Debug build)
	@cargo build -p $(SERVER_CRATE) -p $(CLI_CRATE) --quiet
	$(call success,Binaries: target/debug/$(SERVER_BIN) target/debug/$(CLI_BIN))

test: .check-env check-license
	$(call log,TEST,cargo test --workspace \(exclude IT\))
	@cargo test --workspace --exclude monots-integration-tests --quiet

integration-test: check-license
	@$(MAKE) -C tests/integration test CARGO_TEST_ARGS="$(CARGO_TEST_ARGS)"

check: .check-env check-license
	$(call log,CHECK,cargo check)
	@cargo check --workspace --quiet

fmt:
	$(call log,FMT,cargo fmt)
	@cargo fmt --all

fmt-check:
	$(call log,FMT,cargo fmt --check)
	@cargo fmt --all -- --check

dist: build
	$(call log,DIST,Layout: $(FULL_PATH))
	@rm -rf "$(FULL_PATH)"
	@mkdir -p "$(FULL_PATH)/bin" "$(FULL_PATH)/conf" "$(FULL_PATH)/data" "$(FULL_PATH)/logs"
	@cp "$(TARGET_DIR)/$(SERVER_BIN)" "$(FULL_PATH)/bin/"
	@cp "$(TARGET_DIR)/$(CLI_BIN)" "$(FULL_PATH)/bin/"
	@cp scripts/start-server.sh scripts/start-cli.sh "$(FULL_PATH)/bin/"
	@chmod +x "$(FULL_PATH)/bin/"*.sh
	@cp conf/config.yaml "$(FULL_PATH)/conf/config.yaml"
	@cp README.md LICENSE NOTICE "$(FULL_PATH)/"
	@printf "Name: $(APP_NAME)\nVersion: $(VERSION)\nBuild: $(ARCH)-$(OS)\nTriple: $(TRIPLE)\nDate: $(DATE)\n" > "$(FULL_PATH)/manifest.txt"
	$(call log,ARCHIVE,Compressing...)
	@cd $(DIST_ROOT) && tar -czf "$(FULL_NAME).tar.gz" "$(FULL_NAME)"
	@cd $(DIST_ROOT) && zip -rq "$(FULL_NAME).zip" "$(FULL_NAME)"
	$(call success,Ready: $(DIST_ROOT)/$(FULL_NAME).tar.gz)

dist-host: build-host
	$(call log,DIST,Layout: $(FULL_PATH) [host])
	@rm -rf "$(FULL_PATH)"
	@mkdir -p "$(FULL_PATH)/bin" "$(FULL_PATH)/conf" "$(FULL_PATH)/data" "$(FULL_PATH)/logs"
	@cp "$(HOST_BIN_DIR)/$(SERVER_BIN)" "$(FULL_PATH)/bin/"
	@cp "$(HOST_BIN_DIR)/$(CLI_BIN)" "$(FULL_PATH)/bin/"
	@cp scripts/start-server.sh scripts/start-cli.sh "$(FULL_PATH)/bin/"
	@chmod +x "$(FULL_PATH)/bin/"*.sh
	@cp conf/config.yaml "$(FULL_PATH)/conf/config.yaml"
	@cp README.md LICENSE NOTICE "$(FULL_PATH)/"
	@printf "Name: $(APP_NAME)\nVersion: $(VERSION)\nBuild: $(ARCH)-$(OS)\nDate: $(DATE)\n" > "$(FULL_PATH)/manifest.txt"
	@cd $(DIST_ROOT) && tar -czf "$(FULL_NAME).tar.gz" "$(FULL_NAME)"
	@cd $(DIST_ROOT) && zip -rq "$(FULL_NAME).zip" "$(FULL_NAME)"
	$(call success,Ready: $(DIST_ROOT)/$(FULL_NAME).tar.gz)

run-server: build-host
	$(call log,RUN,start-server.sh)
	@./scripts/start-server.sh

run-cli: build-host
	$(call log,RUN,start-cli.sh)
	@./scripts/start-cli.sh

docker-build:
	$(call log,DOCKER,build $(DOCKER_IMAGE))
	@docker build -t $(DOCKER_IMAGE) .
	$(call success,Image: $(DOCKER_IMAGE))

docker-run: docker-build
	$(call log,DOCKER,run $(DOCKER_IMAGE) :$(DOCKER_PORT))
	@docker run --rm --name monots \
		-p $(DOCKER_PORT):50051 \
		-v monots-data:/opt/monots/data \
		-v monots-logs:/opt/monots/logs \
		$(DOCKER_IMAGE)

docker-up:
	$(call log,DOCKER,compose up)
	@docker compose up -d --build
	$(call success,Listening on http://127.0.0.1:$(DOCKER_PORT))

docker-down:
	$(call log,DOCKER,compose down)
	@docker compose down
	$(call success,Stopped)

clean:
	$(call log,CLEAN,Removing artifacts)
	@cargo clean
	@rm -rf $(DIST_ROOT) data logs
	$(call success,Done)
