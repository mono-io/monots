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

# -----------------------------------------------------------------------
# Integration Test Makefile (FunctionStream-style)
# -----------------------------------------------------------------------
# Usage:
#   make test              — Build server + run Rust integration tests
#   make docker-up         — Start Kafka + MinIO (docker compose)
#   make docker-down       — Stop / remove compose stack
#   make test-stream-docker — docker-up + stream_* sink tests
#   make clean             — Remove test output
#
# Prerequisites:
#   Rust toolchain (cargo). Server binary is built automatically.
#   Docker is required for Kafka / MinIO stream sink ITs.
# -----------------------------------------------------------------------

PROJECT_ROOT := $(shell git -C $(CURDIR) rev-parse --show-toplevel 2>/dev/null || echo $(CURDIR)/../..)

C_G := \033[0;32m
C_B := \033[0;34m
C_0 := \033[0m

log     = @printf "$(C_B)[-]$(C_0) %-12s %s\n" "$(1)" "$(2)"
success = @printf "$(C_G)[✔]$(C_0) %s\n" "$(1)"

.PHONY: test test-stream-docker test-write test-query test-load test-ddl test-recovery docker-up docker-down clean help

help:
	@echo "Integration Test Targets:"
	@echo ""
	@echo "  test               Build monots-server + run cargo integration tests"
	@echo "  test-write         Run tests/write/* binaries"
	@echo "  test-query         Run tests/query/* binaries"
	@echo "  test-load          Run tests/load/* binaries"
	@echo "  test-ddl           Run tests/ddl/* binaries"
	@echo "  test-recovery      Run tests/recovery/* binaries"
	@echo "  docker-up          Start Kafka (19092) + MinIO (19000) + Iceberg REST (18181)"
	@echo "  docker-down        Tear down compose project monots-it"
	@echo "  test-stream-docker docker-up + stream_* sink tests (incl. iceberg)"
	@echo "  clean              Remove target/ test artifacts"
	@echo ""
	@echo "Categories under tests/: auth ddl write query load stream recovery"
	@echo "Example: make test CARGO_TEST_ARGS=\"--test write_dedup\""

docker-up:
	$(call log,DOCKER,compose up kafka+minio+iceberg-rest)
	@docker compose -f $(CURDIR)/docker-compose.yml -p monots-it up -d kafka minio
	@docker compose -f $(CURDIR)/docker-compose.yml -p monots-it up -d minio-init || true
	@docker compose -f $(CURDIR)/docker-compose.yml -p monots-it up -d iceberg-rest || true
	$(call success,Kafka :19092  MinIO :19000  Iceberg REST :18181 \(wait for ports in tests\))

docker-down:
	$(call log,DOCKER,compose down)
	@docker compose -f $(CURDIR)/docker-compose.yml -p monots-it down -v --remove-orphans || true
	$(call success,Compose stack removed)

test:
	$(call log,BUILD,monots-server)
	@$(MAKE) -C $(PROJECT_ROOT) build-host
	$(call log,TEST,cargo test -p monots-integration-tests)
	@cargo test -p monots-integration-tests --manifest-path $(CURDIR)/Cargo.toml $(CARGO_TEST_ARGS)
	$(call success,All integration tests passed)

test-write:
	@$(MAKE) test CARGO_TEST_ARGS="$(CARGO_TEST_ARGS) \
		--test write_arrow_ipc --test write_sql_basics --test write_type_lifecycle \
		--test write_types_all_sql --test write_complex_types --test write_scalar_types_extended \
		--test write_dedup --test write_memory_limit"

test-query:
	@$(MAKE) test CARGO_TEST_ARGS="$(CARGO_TEST_ARGS) \
		--test query_basic --test query_joins --test query_joins_advanced \
		--test query_subqueries --test query_expressions --test query_aggregations \
		--test query_scalar_types --test query_case_sensitivity --test query_scale"

test-load:
	@$(MAKE) test CARGO_TEST_ARGS="$(CARGO_TEST_ARGS) --test load_parquet_bulk"

test-ddl:
	@$(MAKE) test CARGO_TEST_ARGS="$(CARGO_TEST_ARGS) \
		--test ddl_catalog_list --test ddl_catalog_advanced \
		--test ddl_type_validation --test ddl_time_column \
		--test ddl_drop_table --test ddl_complex_types"

test-recovery:
	@$(MAKE) test CARGO_TEST_ARGS="$(CARGO_TEST_ARGS) --test recovery_restart"

test-stream-docker: docker-up
	$(call log,BUILD,monots-server)
	@$(MAKE) -C $(PROJECT_ROOT) build-host
	$(call log,TEST,stream_* sinks)
	@cargo test -p monots-integration-tests --manifest-path $(CURDIR)/Cargo.toml \
		--test stream_filesystem_sink \
		--test stream_delta_sink \
		--test stream_kafka_sink \
		--test stream_iceberg_sink \
		$(CARGO_TEST_ARGS)
	$(call success,Stream Docker ITs passed)
clean:
	$(call log,CLEAN,test artifacts)
	@rm -rf $(CURDIR)/target
	$(call success,Clean complete)
