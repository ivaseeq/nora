#!/usr/bin/env bash
# Execute the recovery regressions from the exact resolved redb git checkout.
# This is one component of the production matrix; it does not test NORA/S3.

set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
OUTPUT=${NORA_REDB_UPSTREAM_EVIDENCE_DIR:-${1:-}}
TEST_TIMEOUT_SECS=${NORA_REDB_UPSTREAM_TEST_TIMEOUT_SECS:-600}
[[ -n "$OUTPUT" ]] || {
    echo "usage: $0 <evidence-output-directory>" >&2
    exit 2
}
for command in cargo git python3 sha256sum tar timeout; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 2
    }
done
if [[ ! "$TEST_TIMEOUT_SECS" =~ ^[0-9]+$ ]] \
    || ((TEST_TIMEOUT_SECS < 60 || TEST_TIMEOUT_SECS > 1800)); then
    echo "NORA_REDB_UPSTREAM_TEST_TIMEOUT_SECS must be between 60 and 1800" >&2
    exit 2
fi

mkdir -p "$OUTPUT"
OUTPUT=$(cd -- "$OUTPUT" && pwd)
RUN_ROOT=$(mktemp -d /tmp/nora-redb-upstream-regressions.XXXXXXXX)

cleanup() {
    local exit_code=$?
    trap - EXIT INT TERM
    case "$RUN_ROOT" in
        /tmp/nora-redb-upstream-regressions.*) rm -rf -- "$RUN_ROOT" ;;
        *) echo "refusing to remove unexpected run root: $RUN_ROOT" >&2 ;;
    esac
    exit "$exit_code"
}
trap cleanup EXIT INT TERM

metadata="$RUN_ROOT/cargo-metadata.json"
timeout -s TERM -k 30s "${TEST_TIMEOUT_SECS}s" \
    cargo metadata --manifest-path "$ROOT/Cargo.toml" --locked --format-version 1 \
    >"$metadata"
IFS=$'\t' read -r redb_manifest redb_version redb_revision redb_source \
    < <(python3 - "$metadata" "$ROOT/nora-registry/Cargo.toml" <<'PY'
import json
import os
import re
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    metadata = json.load(handle)
manifest = os.path.realpath(sys.argv[2])
packages = {package["id"]: package for package in metadata["packages"]}
workspace = next(
    package for package in packages.values()
    if os.path.realpath(package["manifest_path"]) == manifest
)
node = next(node for node in metadata["resolve"]["nodes"] if node["id"] == workspace["id"])
redb_ids = [dependency["pkg"] for dependency in node["deps"] if dependency["name"] == "redb"]
if len(redb_ids) != 1:
    raise SystemExit("expected exactly one direct redb dependency")
redb = packages[redb_ids[0]]
source = redb.get("source", "")
match = re.fullmatch(r"git\+https://github\.com/cberner/redb\?rev=([0-9a-f]{40})#([0-9a-f]{40})", source)
if not match or match.group(1) != match.group(2):
    raise SystemExit("redb is not resolved to one exact canonical git revision")
print(redb["manifest_path"], redb["version"], match.group(2), source, sep="\t")
PY
)

redb_source_dir=$(dirname -- "$redb_manifest")
[[ $(git -C "$redb_source_dir" rev-parse HEAD) == "$redb_revision" ]] || {
    echo "resolved redb checkout does not match cargo metadata" >&2
    exit 1
}
mkdir -p "$RUN_ROOT/redb"
git -C "$redb_source_dir" archive --format=tar "$redb_revision" \
    | tar -xf - -C "$RUN_ROOT/redb"
cp -- "$ROOT/scripts/redb-enospc-regression.rs" \
    "$RUN_ROOT/redb/tests/nora_enospc.rs"

timeout -s TERM -k 30s "${TEST_TIMEOUT_SECS}s" \
    cargo generate-lockfile --manifest-path "$RUN_ROOT/redb/Cargo.toml"
matrix_lock_sha256=$(sha256sum "$RUN_ROOT/redb/Cargo.lock" | awk '{print $1}')
cp -- "$RUN_ROOT/redb/Cargo.lock" "$OUTPUT/redb-regression-Cargo.lock"
timeout -s TERM -k 30s "${TEST_TIMEOUT_SECS}s" \
    cargo fetch --manifest-path "$RUN_ROOT/redb/Cargo.toml" --locked

run_test() {
    local phase=$1
    local filter=$2
    local target=${3:-all}
    local log="$OUTPUT/$phase.log"
    local -a command=(timeout -s TERM -k 30s "${TEST_TIMEOUT_SECS}s"
        cargo test --manifest-path "$RUN_ROOT/redb/Cargo.toml"
        -p "redb@$redb_version" --locked)
    if [[ "$target" == enospc ]]; then
        command+=(--test nora_enospc)
    fi
    command+=("$filter" -- --nocapture)
    printf 'phase=%s test=%s\n' "$phase" "$filter"
    "${command[@]}" >"$log" 2>&1
    grep -Eq 'test result: ok\. [1-9][0-9]* passed' "$log" || {
        echo "redb regression did not report success: $phase" >&2
        return 1
    }
}

run_test growing_commit_crash crash_during_growing_commit_is_recoverable
run_test double_crash_torn_slot recovery_does_not_launder_torn_slot
run_test torn_region_counts torn_layout_fields_recover_from_file_length
run_test commit_error_poison discarded_allocator_state_poisons_database
run_test generic_io_recovery transient_io_error
run_test enospc_recovery immediate_two_phase_commit_survives_enospc_without_losing_last_commit enospc

script_sha256=$(sha256sum "$ROOT/scripts/run-redb-upstream-regressions.sh" | awk '{print $1}')
enospc_test_sha256=$(sha256sum "$ROOT/scripts/redb-enospc-regression.rs" | awk '{print $1}')
growing_commit_log_sha256=$(sha256sum "$OUTPUT/growing_commit_crash.log" | awk '{print $1}')
double_crash_log_sha256=$(sha256sum "$OUTPUT/double_crash_torn_slot.log" | awk '{print $1}')
torn_region_log_sha256=$(sha256sum "$OUTPUT/torn_region_counts.log" | awk '{print $1}')
commit_error_log_sha256=$(sha256sum "$OUTPUT/commit_error_poison.log" | awk '{print $1}')
generic_io_log_sha256=$(sha256sum "$OUTPUT/generic_io_recovery.log" | awk '{print $1}')
enospc_log_sha256=$(sha256sum "$OUTPUT/enospc_recovery.log" | awk '{print $1}')
if command -v jq >/dev/null 2>&1; then
    jq -n \
        --arg revision "$redb_revision" \
        --arg version "$redb_version" \
        --arg source "$redb_source" \
        --arg lock_sha256 "$matrix_lock_sha256" \
        --arg harness_sha256 "$script_sha256" \
        --arg enospc_test_sha256 "$enospc_test_sha256" \
        --arg growing_commit_log_sha256 "$growing_commit_log_sha256" \
        --arg double_crash_log_sha256 "$double_crash_log_sha256" \
        --arg torn_region_log_sha256 "$torn_region_log_sha256" \
        --arg commit_error_log_sha256 "$commit_error_log_sha256" \
        --arg generic_io_log_sha256 "$generic_io_log_sha256" \
        --arg enospc_log_sha256 "$enospc_log_sha256" \
        '{
            schema: 1,
            redb_revision: $revision,
            redb_version: $version,
            redb_source: $source,
            generated_lock_sha256: $lock_sha256,
            harness_sha256: $harness_sha256,
            enospc_test_sha256: $enospc_test_sha256,
            phases: {
                growing_commit_crash: "pass",
                double_crash_torn_slot: "pass",
                torn_region_counts: "pass",
                commit_error_poison: "pass",
                generic_io_recovery: "pass",
                enospc_recovery: "pass"
            },
            logs: {
                growing_commit_crash: $growing_commit_log_sha256,
                double_crash_torn_slot: $double_crash_log_sha256,
                torn_region_counts: $torn_region_log_sha256,
                commit_error_poison: $commit_error_log_sha256,
                generic_io_recovery: $generic_io_log_sha256,
                enospc_recovery: $enospc_log_sha256
            }
        }' >"$OUTPUT/redb-upstream-regressions.json"
else
    python3 - "$OUTPUT/redb-upstream-regressions.json" "$redb_revision" \
        "$redb_version" "$redb_source" "$matrix_lock_sha256" "$script_sha256" \
        "$enospc_test_sha256" "$growing_commit_log_sha256" \
        "$double_crash_log_sha256" "$torn_region_log_sha256" \
        "$commit_error_log_sha256" "$generic_io_log_sha256" \
        "$enospc_log_sha256" <<'PY'
import json
import sys

(
    path, revision, version, source, lock, harness, enospc,
    growing, double_crash, torn, commit_error, generic_io, enospc_log,
) = sys.argv[1:]
with open(path, "w", encoding="utf-8") as handle:
    json.dump({
        "schema": 1,
        "redb_revision": revision,
        "redb_version": version,
        "redb_source": source,
        "generated_lock_sha256": lock,
        "harness_sha256": harness,
        "enospc_test_sha256": enospc,
        "phases": {
            "growing_commit_crash": "pass",
            "double_crash_torn_slot": "pass",
            "torn_region_counts": "pass",
            "commit_error_poison": "pass",
            "generic_io_recovery": "pass",
            "enospc_recovery": "pass",
        },
        "logs": {
            "growing_commit_crash": growing,
            "double_crash_torn_slot": double_crash,
            "torn_region_counts": torn,
            "commit_error_poison": commit_error,
            "generic_io_recovery": generic_io,
            "enospc_recovery": enospc_log,
        },
    }, handle, sort_keys=True, separators=(",", ":"))
    handle.write("\n")
PY
fi

echo "PASS: exact redb upstream crash/ENOSPC regressions"
echo "redb_revision=$redb_revision"
echo "redb_version=$redb_version"
echo "generated_lock_sha256=$matrix_lock_sha256"
