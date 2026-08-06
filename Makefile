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

# Auto-detect system environment & normalize architecture.
# Override with: make dist TARGET=aarch64-unknown-linux-gnu
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
    HOST_TRIPLE := $(ARCH)-unknown-linux-gnu
    STATIC_FLAGS :=
else ifeq ($(OS_NAME), Darwin)
    HOST_TRIPLE := $(ARCH)-apple-darwin
    STATIC_FLAGS :=
else ifneq (,$(findstring MINGW,$(OS_NAME))$(findstring MSYS,$(OS_NAME))$(findstring CYGWIN,$(OS_NAME)))
    HOST_TRIPLE := $(ARCH)-pc-windows-msvc
    STATIC_FLAGS := -C target-feature=+crt-static
else
    HOST_TRIPLE := $(ARCH)-unknown-linux-gnu
    STATIC_FLAGS :=
endif

# Explicit TARGET wins (CI / cross / docker-exported host builds).
TRIPLE ?= $(HOST_TRIPLE)
# Derive short arch label from triple (first component).
ARCH_FROM_TRIPLE := $(shell echo "$(TRIPLE)" | cut -d- -f1)
ARCH := $(ARCH_FROM_TRIPLE)

OPTIMIZE_FLAGS := -C opt-level=z -C strip=symbols $(STATIC_FLAGS)

TARGET_DIR  := target/$(TRIPLE)/release
HOST_BIN_DIR := target/release

# Release archives are named by Rust target triple for unambiguous FOSS packages.
FULL_NAME   := $(APP_NAME)-$(VERSION)-$(TRIPLE)
FULL_PATH   := $(DIST_ROOT)/$(FULL_NAME)

C_R := \033[0;31m
C_G := \033[0;32m
C_B := \033[0;34m
C_Y := \033[0;33m
C_0 := \033[0m

log = @printf "$(C_B)[-]$(C_0) %-15s %s\n" "$(1)" "$(2)"
success = @printf "$(C_G)[✔]$(C_0) %s\n" "$(1)"

.PHONY: all help build build-host dev test integration-test clean dist dist-host run-server run-cli \
	docker-build docker-run docker-up docker-down fmt fmt-check clippy check check-ci check-license \
	package-docker .check-env .ensure-target

all: build-host

help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "  build / build-host / dist / dist-host"
	@echo "  package-docker PLATFORM=linux/arm/v7 TRIPLE=armv7-unknown-linux-gnueabihf"
	@echo "  check-ci   license + fmt-check + clippy"
	@echo "  Version $(VERSION) | host $(HOST_TRIPLE) | package $(TRIPLE)"
	@echo ""

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

clippy: .check-env
	$(call log,CLIPPY,cargo clippy \(correctness + suspicious + dead_code\))
	@cargo clippy --workspace --all-targets \
		--exclude monots-integration-tests \
		-- -D clippy::correctness -D clippy::suspicious -D dead_code

check-ci: check-license fmt-check clippy
	$(call success,CI checks passed \(license + fmt + clippy\))

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
	@printf "Name: $(APP_NAME)\nVersion: $(VERSION)\nTriple: $(TRIPLE)\nDate: $(DATE)\n" > "$(FULL_PATH)/manifest.txt"
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
	@printf "Name: $(APP_NAME)\nVersion: $(VERSION)\nTriple: $(TRIPLE)\nHost: $(HOST_TRIPLE)\nDate: $(DATE)\n" > "$(FULL_PATH)/manifest.txt"
	@cd $(DIST_ROOT) && tar -czf "$(FULL_NAME).tar.gz" "$(FULL_NAME)"
	@cd $(DIST_ROOT) && zip -rq "$(FULL_NAME).zip" "$(FULL_NAME)"
	$(call success,Ready: $(DIST_ROOT)/$(FULL_NAME).tar.gz)

# Embedded / foreign Linux arches via Buildx+QEMU (see scripts/package-docker-platform.sh).
# Example: make package-docker PLATFORM=linux/arm/v7 TRIPLE=armv7-unknown-linux-gnueabihf
package-docker:
	@test -n "$(PLATFORM)" || { printf "$(C_R)[X] PLATFORM= required$(C_0)\n"; exit 1; }
	@test -n "$(TRIPLE)" || { printf "$(C_R)[X] TRIPLE= required$(C_0)\n"; exit 1; }
	$(call log,DOCKER,package $(PLATFORM) / $(TRIPLE))
	@chmod +x scripts/package-docker-platform.sh
	@./scripts/package-docker-platform.sh "$(PLATFORM)" "$(TRIPLE)"

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
