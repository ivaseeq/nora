#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
e2e_root=$(cd -- "$script_dir/.." && pwd)
repo_root=$(cd -- "$e2e_root/../.." && pwd)
nora_bin=${NORA_UI_SMOKE_BIN:-$repo_root/target/debug/nora}
smoke_port=${NORA_UI_SMOKE_PORT:-14080}
run_root=$(mktemp -d "${TMPDIR:-/tmp}/nora-ui-smoke.XXXXXXXX")
server_log="$run_root/nora.log"
server_pid=""

cleanup() {
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    kill -TERM "$server_pid" 2>/dev/null || true
    for _ in {1..40}; do
      if ! kill -0 "$server_pid" 2>/dev/null; then
        break
      fi
      sleep 0.25
    done
    if kill -0 "$server_pid" 2>/dev/null; then
      kill -KILL "$server_pid" 2>/dev/null || true
    fi
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf -- "$run_root"
}
trap cleanup EXIT INT TERM

if [[ ! -x "$nora_bin" ]]; then
  printf 'NORA binary is missing or not executable: %s\n' "$nora_bin" >&2
  printf 'Build it first with: cargo build --locked -p nora-registry\n' >&2
  exit 2
fi
if [[ ! -x "$e2e_root/node_modules/.bin/playwright" ]]; then
  printf 'Playwright is not installed; run npm ci in %s\n' "$e2e_root" >&2
  exit 2
fi

mkdir -p "$run_root/storage"
(
  cd "$repo_root"
  env \
    NORA_HOST=127.0.0.1 \
    NORA_PORT="$smoke_port" \
    NORA_PUBLIC_URL="http://127.0.0.1:$smoke_port" \
    NORA_STORAGE_MODE=local \
    NORA_STORAGE_PATH="$run_root/storage" \
    NORA_REGISTRIES_ENABLE=maven,npm \
    RUST_LOG=warn \
    "$nora_bin" serve
) >"$server_log" 2>&1 &
server_pid=$!

server_ready=0
for _ in {1..120}; do
  if ! kill -0 "$server_pid" 2>/dev/null; then
    printf 'NORA exited before the UI smoke became ready.\n' >&2
    tail -100 "$server_log" >&2
    exit 1
  fi
  if wget -q -O /dev/null "http://127.0.0.1:$smoke_port/health"; then
    server_ready=1
    break
  fi
  sleep 0.25
done
if [[ "$server_ready" != 1 ]]; then
  printf 'NORA did not become healthy within 30 seconds.\n' >&2
  tail -100 "$server_log" >&2
  exit 1
fi

cd "$e2e_root"
NORA_URL="http://127.0.0.1:$smoke_port" \
  ./node_modules/.bin/playwright test \
  tests/ui-openapi.spec.ts \
  --project=chromium-fullhd

NORA_URL="http://127.0.0.1:$smoke_port" \
  ./node_modules/.bin/playwright test \
  tests/ui-contracts.spec.ts \
  tests/ui-responsive.spec.ts \
  --project=chromium-fullhd \
  --project=chromium-mobile-390 \
  --project=chromium-reflow-320 \
  --grep 'language selection|icon-only destinations|WCAG 2\.2 AA automated audit: /ui/$|@responsive /ui/ reflows|@responsive mobile navigation'
