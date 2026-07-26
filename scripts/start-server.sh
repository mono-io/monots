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

BIN_DIR="$(get_real_path)"
APP_HOME="$(cd -P "$BIN_DIR/.." && pwd)"

export MONOTS_HOME="$APP_HOME"
export MONOTS_CONF="${MONOTS_CONF:-$APP_HOME/conf/config.yaml}"
export MONOTS_LOG_DIR="${MONOTS_LOG_DIR:-$APP_HOME/logs}"
export MONOTS_DATA_DIR="${MONOTS_DATA_DIR:-$APP_HOME/data}"

BINARY="$BIN_DIR/monots-server"
if [ ! -f "$BINARY" ]; then
  BINARY="$APP_HOME/target/release/monots-server"
fi
FOREGROUND="true"
APP_ARGS=()

if [ ! -f "$BINARY" ]; then
  echo "[ERROR] Binary not found at $BINARY"
  exit 1
fi

while [ $# -gt 0 ]; do
  case "$1" in
    -c|--config)
      export MONOTS_CONF="$2"
      shift 2
      ;;
    -d|--daemon)
      FOREGROUND="false"
      shift
      ;;
    --)
      shift
      APP_ARGS+=("$@")
      break
      ;;
    *)
      APP_ARGS+=("$1")
      shift
      ;;
  esac
done

mkdir -p "$MONOTS_LOG_DIR" "$MONOTS_DATA_DIR"

if [ ! -f "$MONOTS_CONF" ]; then
  echo "[WARN] Config not found: $MONOTS_CONF (server will use built-in defaults)"
fi

echo "------------------------------------------------"
echo "Starting MonoTS Server"
echo "Home:   $MONOTS_HOME"
echo "Config: $MONOTS_CONF"
echo "Mode:   $( [ "$FOREGROUND" = "true" ] && echo "Foreground" || echo "Daemon" )"
echo "------------------------------------------------"

SERVER_ARGS=(--config "$MONOTS_CONF")

if [ "$FOREGROUND" = "true" ]; then
  exec "$BINARY" "${SERVER_ARGS[@]}" "${APP_ARGS[@]}"
else
  LOG_OUT="$MONOTS_LOG_DIR/stdout.log"
  LOG_ERR="$MONOTS_LOG_DIR/stderr.log"
  nohup "$BINARY" "${SERVER_ARGS[@]}" "${APP_ARGS[@]}" >"$LOG_OUT" 2>"$LOG_ERR" &
  PID=$!
  sleep 1
  if kill -0 "$PID" >/dev/null 2>&1; then
    echo "[SUCCESS] Started. PID: $PID"
    echo "Logs: $LOG_OUT"
  else
    echo "[FAILED] Process exited immediately."
    echo "Check error log: $LOG_ERR"
    exit 1
  fi
fi
