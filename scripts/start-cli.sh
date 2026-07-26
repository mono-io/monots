#!/usr/bin/env bash
#
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

set -eo pipefail

get_real_path() {
  local source="${BASH_SOURCE[0]}"
  while [ -h "$source" ]; do
    local dir="$(cd -P "$(dirname "$source")" && pwd)"
    source="$(readlink "$source")"
    [[ $source != /* ]] && source="$dir/$source"
  done
  echo "$(cd -P "$(dirname "$source")" && pwd)"
}

read_yaml_value() {
  local key="$1"
  local file="$2"
  local default="$3"
  if [ ! -f "$file" ]; then
    echo "$default"
    return
  fi
  local value
  value="$(grep -E "^[[:space:]]*${key}:" "$file" | head -n 1 | sed -E "s/^[[:space:]]*${key}:[[:space:]]*//" | tr -d '"' | tr -d "'")"
  if [ -z "$value" ]; then
    echo "$default"
  else
    echo "$value"
  fi
}

BIN_DIR="$(get_real_path)"
APP_HOME="$(cd -P "$BIN_DIR/.." && pwd)"

export MONOTS_HOME="$APP_HOME"
export MONOTS_CONF="${MONOTS_CONF:-$APP_HOME/conf/config.yaml}"

BINARY="$BIN_DIR/monots"
if [ ! -f "$BINARY" ]; then
  BINARY="$APP_HOME/target/release/monots"
fi

CLI_HOST="${MONOTS_HOST:-127.0.0.1}"
CLI_PORT="${MONOTS_PORT:-}"
CLI_USER="${MONOTS_USER:-}"
CLI_PASSWORD="${MONOTS_PASSWORD:-}"
SQL=""

if [ ! -f "$BINARY" ]; then
  echo "[ERROR] CLI binary not found at $BINARY"
  exit 1
fi

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--host)
      CLI_HOST="$2"
      shift 2
      ;;
    -p|--port)
      CLI_PORT="$2"
      shift 2
      ;;
    -u|--user)
      CLI_USER="$2"
      shift 2
      ;;
    -P|--password)
      CLI_PASSWORD="$2"
      shift 2
      ;;
    -c|--config)
      export MONOTS_CONF="$2"
      shift 2
      ;;
    --sql)
      SQL="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

if [ -z "$CLI_PORT" ]; then
  CLI_PORT="$(read_yaml_value port "$MONOTS_CONF" "50051")"
fi
if [ -z "$CLI_USER" ]; then
  CLI_USER="$(read_yaml_value username "$MONOTS_CONF" "admin")"
fi
if [ -z "$CLI_PASSWORD" ]; then
  CLI_PASSWORD="$(read_yaml_value password "$MONOTS_CONF" "admin")"
fi

GRPC_URL="http://${CLI_HOST}:${CLI_PORT}"

echo "------------------------------------------------"
echo "MonoTS SQL CLI"
echo "Home:   $MONOTS_HOME"
echo "Server: $GRPC_URL"
echo "------------------------------------------------"

CLI_ARGS=(-H "$GRPC_URL" -u "$CLI_USER" -p "$CLI_PASSWORD")
if [ -n "$SQL" ]; then
  CLI_ARGS+=(--sql "$SQL")
fi

exec "$BINARY" "${CLI_ARGS[@]}"
