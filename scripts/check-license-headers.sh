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

# Verify Apache-2.0 copyright headers on first-party source files.
# Intended to run from the repo root via `make check-license` / `make check` / `make build*`.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

REQUIRED_COPYRIGHT='Copyright 2026 MonoTS Contributors'
REQUIRED_LICENSE='Licensed under the Apache License, Version 2.0'

# Path prefixes (from repo root) excluded from the scan.
SKIP_PREFIXES=(
  './.git/'
  './target/'
  './dist/'
  './.venv/'
  './data/'
  './logs/'
  # Vendored third-party crates (retain upstream Apache headers).
  './patches/'
)

should_skip() {
  local path="$1"
  local p
  for p in "${SKIP_PREFIXES[@]}"; do
    case "$path" in
      "$p"*) return 0 ;;
    esac
  done
  return 1
}

missing=()
while IFS= read -r -d '' file; do
  should_skip "$file" && continue
  # Only inspect the header region (first ~1KiB).
  head_bytes="$(head -c 1024 "$file" 2>/dev/null || true)"
  if [[ "$head_bytes" != *"$REQUIRED_COPYRIGHT"* ]] ||
     [[ "$head_bytes" != *"$REQUIRED_LICENSE"* ]]; then
    missing+=("${file#./}")
  fi
done < <(find . \( -name '*.rs' -o -name '*.sh' -o -name '*.proto' \) -type f -print0)

# Makefiles at known paths.
for mk in Makefile tests/integration/Makefile; do
  if [[ -f "$mk" ]]; then
    head_bytes="$(head -c 1024 "$mk")"
    if [[ "$head_bytes" != *"$REQUIRED_COPYRIGHT"* ]] ||
       [[ "$head_bytes" != *"$REQUIRED_LICENSE"* ]]; then
      missing+=("$mk")
    fi
  fi
done

if ((${#missing[@]} > 0)); then
  printf 'error: missing Apache license header in %d file(s):\n' "${#missing[@]}" >&2
  printf '  %s\n' "${missing[@]}" >&2
  printf '\nAdd the standard MonoTS Apache-2.0 header (see scripts/start-server.sh).\n' >&2
  exit 1
fi

printf 'license headers: ok\n'
