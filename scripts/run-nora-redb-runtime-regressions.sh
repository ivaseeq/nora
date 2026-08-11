#!/usr/bin/env bash
# Focused NORA tests that bind process-level redb recovery policy to evidence.

set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
OUTPUT=${NORA_REDB_RUNTIME_EVIDENCE_DIR:-${1:-}}
TEST_TIMEOUT_SECS=${NORA_REDB_RUNTIME_TEST_TIMEOUT_SECS:-600}
[[ -n "$OUTPUT" ]] || {
    echo "usage: $0 <evidence-output-directory>" >&2
    exit 2
}
for command in cargo jq sha256sum timeout; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 2
    }
done
if [[ ! "$TEST_TIMEOUT_SECS" =~ ^[0-9]+$ ]] \
    || ((TEST_TIMEOUT_SECS < 60 || TEST_TIMEOUT_SECS > 1800)); then
    echo "NORA_REDB_RUNTIME_TEST_TIMEOUT_SECS must be between 60 and 1800" >&2
    exit 2
fi
mkdir -p "$OUTPUT"
OUTPUT=$(cd -- "$OUTPUT" && pwd)

run_test() {
    local phase=$1
    local filter=$2
    local log="$OUTPUT/$phase.log"
    printf 'phase=%s test=%s\n' "$phase" "$filter"
    timeout -s TERM -k 30s "${TEST_TIMEOUT_SECS}s" \
        cargo test --manifest-path "$ROOT/Cargo.toml" --locked \
        -p nora-registry --bin nora "$filter" -- --nocapture >"$log" 2>&1
    grep -Eq 'test result: ok\. [1-9][0-9]* passed' "$log" || {
        echo "NORA redb regression did not execute successfully: $phase" >&2
        return 1
    }
}

run_test timeout_reap timed_out_preflight_child_is_killed_and_reaped_before_fallback
run_test second_open preflight_distinguishes_missing_healthy_and_writer_overlap
run_test generic_io_policy preflight_quarantines_only_typed_corruption_not_generic_io
run_test abnormal_shutdown abnormal_process_shutdown_forces_unclean_preflight_and_immediate_reconcile
run_test shutdown_fence physical_receipt_progress_wakes_shutdown_before_shared_deadline

harness_sha256=$(sha256sum "$ROOT/scripts/run-nora-redb-runtime-regressions.sh" | awk '{print $1}')
timeout_reap_log_sha256=$(sha256sum "$OUTPUT/timeout_reap.log" | awk '{print $1}')
second_open_log_sha256=$(sha256sum "$OUTPUT/second_open.log" | awk '{print $1}')
generic_io_policy_log_sha256=$(sha256sum "$OUTPUT/generic_io_policy.log" | awk '{print $1}')
abnormal_shutdown_log_sha256=$(sha256sum "$OUTPUT/abnormal_shutdown.log" | awk '{print $1}')
shutdown_fence_log_sha256=$(sha256sum "$OUTPUT/shutdown_fence.log" | awk '{print $1}')
jq -n \
    --arg harness_sha256 "$harness_sha256" \
    --arg timeout_reap_log_sha256 "$timeout_reap_log_sha256" \
    --arg second_open_log_sha256 "$second_open_log_sha256" \
    --arg generic_io_policy_log_sha256 "$generic_io_policy_log_sha256" \
    --arg abnormal_shutdown_log_sha256 "$abnormal_shutdown_log_sha256" \
    --arg shutdown_fence_log_sha256 "$shutdown_fence_log_sha256" \
    '{
        schema: 1,
        harness_sha256: $harness_sha256,
        phases: {
            timeout_reap: "pass",
            second_open: "pass",
            generic_io_policy: "pass",
            abnormal_shutdown: "pass",
            shutdown_fence: "pass"
        },
        logs: {
            timeout_reap: $timeout_reap_log_sha256,
            second_open: $second_open_log_sha256,
            generic_io_policy: $generic_io_policy_log_sha256,
            abnormal_shutdown: $abnormal_shutdown_log_sha256,
            shutdown_fence: $shutdown_fence_log_sha256
        }
    }' >"$OUTPUT/nora-redb-runtime-regressions.json"

echo "PASS: focused NORA redb runtime regressions"
