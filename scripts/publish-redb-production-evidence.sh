#!/usr/bin/env bash
# Publish one completed NORA/redb matrix as an immutable OCI artifact and read
# it back by the registry manifest digest. This script never promotes an image
# or a Helm release.

set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
MATRIX_DIR=${NORA_REDB_EVIDENCE_MATRIX_DIR:-${1:-}}
TARGET_REPOSITORY=${NORA_REDB_EVIDENCE_REPOSITORY:-${2:-}}
OUTPUT=${NORA_REDB_EVIDENCE_OUTPUT:-${3:-}}
ORAS_TIMEOUT_SECS=${NORA_REDB_EVIDENCE_ORAS_TIMEOUT_SECS:-900}
RUST_IMAGE=docker-hub.just-ai.com/infra/artifact-nora@sha256:365bc9b835ea399bf25a9259fa83d6960b4ad1d7349864c62404f5599e211841
MINIO_IMAGE=minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e
MC_IMAGE=minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727
PASS_MARKER_SHA256=9f56e761d79bfdb34304a012586cb04d16b435ef6130091a97702e559260a2f2

if [[ -z "$MATRIX_DIR" || -z "$TARGET_REPOSITORY" || -z "$OUTPUT" ]]; then
    echo "usage: $0 <matrix-evidence-directory> <harbor-repository> <output-directory>" >&2
    exit 2
fi
[[ "$TARGET_REPOSITORY" == docker-hub.just-ai.com/infra/artifact-nora-redb-evidence ]] || {
    echo "evidence repository is outside the approved Harbor repository" >&2
    exit 2
}
if [[ ! "$ORAS_TIMEOUT_SECS" =~ ^[0-9]+$ ]] \
    || ((ORAS_TIMEOUT_SECS < 60 || ORAS_TIMEOUT_SECS > 1800)); then
    echo "NORA_REDB_EVIDENCE_ORAS_TIMEOUT_SECS must be between 60 and 1800" >&2
    exit 2
fi
for command in git jq oras python3 sha256sum tar timeout; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 2
    }
done
if ! git -C "$ROOT" diff --quiet \
    || [[ -n $(git -C "$ROOT" ls-files --others --exclude-standard) ]]; then
    echo "evidence publication requires an exact staged tree" >&2
    exit 1
fi

MATRIX_DIR=$(cd -- "$MATRIX_DIR" && pwd)
mkdir -p "$OUTPUT"
OUTPUT=$(cd -- "$OUTPUT" && pwd)
RUN_ROOT=$(mktemp -d /tmp/nora-redb-evidence-publish.XXXXXXXX)
cleanup() {
    local exit_code=$?
    trap - EXIT INT TERM
    case "$RUN_ROOT" in
        /tmp/nora-redb-evidence-publish.*) rm -rf -- "$RUN_ROOT" ;;
        *) echo "refusing to remove unexpected run root: $RUN_ROOT" >&2 ;;
    esac
    exit "$exit_code"
}
trap cleanup EXIT INT TERM
mkdir -p "$RUN_ROOT/payload" "$RUN_ROOT/readback" "$RUN_ROOT/extracted"
python3 - "$MATRIX_DIR" "$RUN_ROOT/payload" <<'PY'
import os
import pathlib
import shutil
import stat
import sys

source = pathlib.Path(sys.argv[1])
destination = pathlib.Path(sys.argv[2])
for name in ("redb-production-evidence.tar", "redb-production-matrix.json"):
    source_path = source / name
    try:
        descriptor = os.open(source_path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except OSError as error:
        raise SystemExit(f"cannot open regular matrix input {name}: {error.strerror}")
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise SystemExit(f"matrix input is not a regular file: {name}")
        target_path = destination / name
        with os.fdopen(descriptor, "rb", closefd=False) as source_handle, open(
            target_path, "xb"
        ) as target_handle:
            shutil.copyfileobj(source_handle, target_handle, 1024 * 1024)
    finally:
        os.close(descriptor)
PY
bundle="$RUN_ROOT/payload/redb-production-evidence.tar"
matrix="$RUN_ROOT/payload/redb-production-matrix.json"
if [[ ${NORA_GATE_PUBLISH_MODE:-0} == 1 \
    && -n ${NORA_GATE_PUBLISH_SNAPSHOT_READY_FILE:-} \
    && -n ${NORA_GATE_PUBLISH_SNAPSHOT_CONTINUE_FILE:-} ]]; then
    : >"$NORA_GATE_PUBLISH_SNAPSHOT_READY_FILE"
    for _ in {1..200}; do
        [[ -e "$NORA_GATE_PUBLISH_SNAPSHOT_CONTINUE_FILE" ]] && break
        sleep 0.05
    done
    [[ -e "$NORA_GATE_PUBLISH_SNAPSHOT_CONTINUE_FILE" ]] || {
        echo "test publisher snapshot barrier timed out" >&2
        exit 1
    }
fi
jq -e --arg rust_image "$RUST_IMAGE" --arg minio_image "$MINIO_IMAGE" \
    --arg mc_image "$MC_IMAGE" '
    .source_tree as $source_tree
    |
    .schema == 1
    and .production_matrix_complete == true
    and (.source_tree | test("^[0-9a-f]{40}$"))
    and (.source_digest | test("^[0-9a-f]{64}$"))
    and (.cargo_lock_sha256 | test("^[0-9a-f]{64}$"))
    and (.image_ref | test("^docker-hub\\.just-ai\\.com/infra/artifact-nora@sha256:[0-9a-f]{64}$"))
    and (.image_digest | test("^sha256:[0-9a-f]{64}$"))
    and (.engine_revision | type == "string" and length > 0)
    and (.nora_schema_version | type == "number")
    and .execution == {
        source_mode: "verified_git_tree_snapshot",
        source_tree: $source_tree,
        runner_kind: "docker"
    }
    and .redb.repository == "https://github.com/cberner/redb"
    and (.redb.revision | test("^[0-9a-f]{40}$"))
    and (.redb.package_version | test("^[0-9]+\\.[0-9]+\\.[0-9]+$"))
    and (.redb.lock_source | type == "string" and length > 0)
    and ([.harnesses.matrix_sha256, .harnesses.minio_sha256,
          .harnesses.runtime_sha256, .harnesses.upstream_sha256,
          .harnesses.enospc_test_sha256] | all(test("^[0-9a-f]{64}$")))
    and .components.minio.manifest_path == "components/minio-manifest.json"
    and .components.minio.bundle_path == "components/minio-evidence.tar"
    and (.components.minio.manifest_sha256 | test("^[0-9a-f]{64}$"))
    and (.components.minio.bundle_sha256 | test("^[0-9a-f]{64}$"))
    and .components.minio.minio_image == $minio_image
    and .components.minio.mc_image == $mc_image
    and .components.runtime.manifest_path == "runtime/nora-redb-runtime-regressions.json"
    and (.components.runtime.manifest_sha256 | test("^[0-9a-f]{64}$"))
    and .components.runtime.runner_kind == "docker"
    and .components.runtime.runner_image == $rust_image
    and .components.upstream.manifest_path == "upstream/redb-upstream-regressions.json"
    and (.components.upstream.manifest_sha256 | test("^[0-9a-f]{64}$"))
    and .components.upstream.runner_kind == "docker"
    and .components.upstream.runner_image == $rust_image
    and .verified_phases == [
        "sigkill", "enospc", "double_crash", "torn_write", "corruption",
        "generic_io", "timeout", "second_open", "recovery"
    ]
' "$matrix" >/dev/null || {
    echo "matrix manifest is incomplete or malformed" >&2
    exit 1
}
expected_tree=$(jq -er '.source_tree | select(test("^[0-9a-f]{40}$"))' "$matrix")
expected_source_digest=$(jq -er '.source_digest | select(test("^[0-9a-f]{64}$"))' "$matrix")
[[ $(git -C "$ROOT" write-tree) == "$expected_tree" ]] || {
    echo "matrix tree is not the current exact staged tree" >&2
    exit 1
}
[[ $(python3 "$ROOT/scripts/nora-source-digest.py" "$ROOT") == "$expected_source_digest" ]] || {
    echo "matrix canonical source digest differs from the current source" >&2
    exit 1
}
matrix_image_ref=$(jq -er '.image_ref' "$matrix")
matrix_image_digest=$(jq -er '.image_digest' "$matrix")
[[ "${matrix_image_ref##*@}" == "$matrix_image_digest" ]] || {
    echo "matrix image digest differs from its immutable reference" >&2
    exit 1
}

bundle_sha256=$(sha256sum "$bundle" | awk '{print $1}')
matrix_sha256=$(sha256sum "$matrix" | awk '{print $1}')
TARGET="$TARGET_REPOSITORY:redb-$expected_tree-$bundle_sha256"
embedded_matrix_sha256=$(tar -xOf "$bundle" redb-production-matrix.json | sha256sum | awk '{print $1}')
[[ "$embedded_matrix_sha256" == "$matrix_sha256" ]] || {
    echo "evidence tar does not contain the exact matrix manifest" >&2
    exit 1
}
python3 - "$bundle" <<'PY'
import pathlib
import sys
import tarfile

expected = {
    "redb-production-matrix.json",
    "components/minio-manifest.json",
    "components/minio-evidence.tar",
    "runtime/nora-redb-runtime-regressions.json",
    "runtime/timeout_reap.log",
    "runtime/second_open.log",
    "runtime/generic_io_policy.log",
    "runtime/abnormal_shutdown.log",
    "runtime/shutdown_fence.log",
    "upstream/redb-upstream-regressions.json",
    "upstream/redb-regression-Cargo.lock",
    "upstream/growing_commit_crash.log",
    "upstream/double_crash_torn_slot.log",
    "upstream/torn_region_counts.log",
    "upstream/commit_error_poison.log",
    "upstream/generic_io_recovery.log",
    "upstream/enospc_recovery.log",
}
with tarfile.open(sys.argv[1], "r:") as archive:
    seen = set()
    for member in archive.getmembers():
        name = member.name
        if (
            name != str(pathlib.PurePosixPath(name))
            or name.startswith("/")
            or ".." in pathlib.PurePosixPath(name).parts
            or not member.isfile()
            or name in seen
            or name not in expected
        ):
            raise SystemExit("evidence tar is not the canonical member set")
        seen.add(name)
    if seen != expected:
        raise SystemExit("evidence tar lacks a canonical member")
PY
tar -xf "$bundle" -C "$RUN_ROOT/extracted"

minio_manifest="$RUN_ROOT/extracted/components/minio-manifest.json"
minio_bundle="$RUN_ROOT/extracted/components/minio-evidence.tar"
runtime_manifest="$RUN_ROOT/extracted/runtime/nora-redb-runtime-regressions.json"
upstream_manifest="$RUN_ROOT/extracted/upstream/redb-upstream-regressions.json"
upstream_lock="$RUN_ROOT/extracted/upstream/redb-regression-Cargo.lock"
for required in "$minio_manifest" "$minio_bundle" "$runtime_manifest" \
    "$upstream_manifest" "$upstream_lock"; do
    [[ -s "$required" ]] || {
        echo "evidence tar lacks a required component artifact" >&2
        exit 1
    }
done

component_hash_matches() {
    local file=$1 expression=$2 expected
    expected=$(jq -er "$expression | select(test(\"^[0-9a-f]{64}$\"))" "$matrix")
    [[ $(sha256sum "$file" | awk '{print $1}') == "$expected" ]]
}
component_hash_matches "$minio_manifest" '.components.minio.manifest_sha256' \
    || { echo "MinIO component manifest hash differs" >&2; exit 1; }
component_hash_matches "$minio_bundle" '.components.minio.bundle_sha256' \
    || { echo "MinIO component bundle hash differs" >&2; exit 1; }
component_hash_matches "$runtime_manifest" '.components.runtime.manifest_sha256' \
    || { echo "runtime component manifest hash differs" >&2; exit 1; }
component_hash_matches "$upstream_manifest" '.components.upstream.manifest_sha256' \
    || { echo "upstream component manifest hash differs" >&2; exit 1; }

verify_component_logs() {
    local manifest=$1 directory=$2 expected_keys=$3 phase digest log
    jq -e --argjson expected "$expected_keys" '.logs | keys == $expected' \
        "$manifest" >/dev/null || return 1
    while IFS=$'\t' read -r phase digest; do
        [[ "$phase" =~ ^[a-z0-9_]+$ && "$digest" =~ ^[0-9a-f]{64}$ ]] || return 1
        log="$directory/$phase.log"
        [[ -s "$log" && $(sha256sum "$log" | awk '{print $1}') == "$digest" ]] \
            || return 1
    done < <(jq -er '.logs | to_entries[] | [.key, .value] | @tsv' "$manifest")
}

jq -e \
    --arg tree "$expected_tree" --arg lock "$(jq -r '.cargo_lock_sha256' "$matrix")" \
    --arg image "$matrix_image_ref" --arg engine "$(jq -r '.engine_revision' "$matrix")" \
    --argjson schema "$(jq -r '.nora_schema_version' "$matrix")" \
    --arg bundle "$(jq -r '.components.minio.bundle_sha256' "$matrix")" \
    --arg minio "$MINIO_IMAGE" --arg mc "$MC_IMAGE" \
    --arg pass_marker "$PASS_MARKER_SHA256" \
    '.source_tree == $tree and .cargo_lock_sha256 == $lock and .image == $image
     and .engine_revision == $engine and .nora_schema_version == $schema
     and .bundle_sha256 == $bundle and .production_matrix_component_complete == true
     and .minio_image == $minio and .mc_image == $mc
     and .verified_phases == [
        "cold_rebuild", "warm_reopen", "incremental", "single_writer",
        "crash", "corruption", "index_loss", "recovery",
        "disk_full_admission"
     ]
     and (.artifacts | type == "object" and length > 0)
     and .phase_artifacts == {
        cold_rebuild: "phase-cold_rebuild.ok",
        warm_reopen: "phase-warm_reopen.ok",
        incremental: "phase-incremental.ok",
        single_writer: "phase-single_writer.ok",
        crash: "phase-crash.ok",
        corruption: "phase-corruption.ok",
        index_loss: "phase-index_loss.ok",
        recovery: "phase-recovery.ok",
        disk_full_admission: "phase-disk_full_admission.ok"
     }
     and (. as $doc | [.phase_artifacts[]]
          | all(. as $path | $doc.artifacts[$path] == $pass_marker))' \
    "$minio_manifest" >/dev/null || {
    echo "MinIO component identity/phases differ from the matrix" >&2
    exit 1
}
python3 - "$minio_manifest" "$minio_bundle" <<'PY'
import hashlib
import json
import pathlib
import re
import sys
import tarfile

with open(sys.argv[1], encoding="utf-8") as handle:
    artifacts = json.load(handle).get("artifacts")
if not isinstance(artifacts, dict) or not artifacts:
    raise SystemExit("MinIO component manifest lacks its canonical artifact map")
for name, digest in artifacts.items():
    if (
        not isinstance(name, str)
        or name != str(pathlib.PurePosixPath(name))
        or name.startswith("/")
        or ".." in pathlib.PurePosixPath(name).parts
        or not isinstance(digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", digest) is None
    ):
        raise SystemExit("MinIO component manifest contains an unsafe artifact identity")
seen = set()
with tarfile.open(sys.argv[2], "r:") as archive:
    for member in archive.getmembers():
        if member.name in seen or member.name not in artifacts or not member.isfile():
            raise SystemExit("MinIO evidence tar is not the canonical artifact set")
        seen.add(member.name)
        source = archive.extractfile(member)
        if source is None:
            raise SystemExit("MinIO evidence artifact cannot be read")
        digest = hashlib.sha256()
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
        if digest.hexdigest() != artifacts[member.name]:
            raise SystemExit("MinIO evidence artifact digest differs from its manifest")
if seen != set(artifacts):
    raise SystemExit("MinIO evidence tar lacks a canonical artifact")
PY
if ! jq -e --arg harness "$(jq -r '.harnesses.runtime_sha256' "$matrix")" \
    '.harness_sha256 == $harness
     and .phases == {timeout_reap:"pass",second_open:"pass",
        generic_io_policy:"pass",abnormal_shutdown:"pass",shutdown_fence:"pass"}' \
    "$runtime_manifest" >/dev/null \
    || ! verify_component_logs "$runtime_manifest" "$(dirname "$runtime_manifest")" \
        '["abnormal_shutdown","generic_io_policy","second_open","shutdown_fence","timeout_reap"]'; then
    echo "runtime component evidence is incomplete" >&2
    exit 1
fi
if ! jq -e \
    --arg harness "$(jq -r '.harnesses.upstream_sha256' "$matrix")" \
    --arg enospc "$(jq -r '.harnesses.enospc_test_sha256' "$matrix")" \
    --arg revision "$(jq -r '.redb.revision' "$matrix")" \
    --arg lock "$(jq -r '.redb.regression_lock_sha256' "$matrix")" \
    '.harness_sha256 == $harness and .enospc_test_sha256 == $enospc
     and .redb_revision == $revision and .generated_lock_sha256 == $lock
     and .phases == {growing_commit_crash:"pass",double_crash_torn_slot:"pass",
        torn_region_counts:"pass",commit_error_poison:"pass",
        generic_io_recovery:"pass",enospc_recovery:"pass"}' \
    "$upstream_manifest" >/dev/null \
    || [[ $(sha256sum "$upstream_lock" | awk '{print $1}') \
        != "$(jq -r '.redb.regression_lock_sha256' "$matrix")" ]] \
    || ! verify_component_logs "$upstream_manifest" "$(dirname "$upstream_manifest")" \
        '["commit_error_poison","double_crash_torn_slot","enospc_recovery","generic_io_recovery","growing_commit_crash","torn_region_counts"]'; then
    echo "upstream component evidence is incomplete" >&2
    exit 1
fi

(
    cd "$RUN_ROOT/payload"
    timeout -s TERM -k 30s "${ORAS_TIMEOUT_SECS}s" \
        oras push --no-tty --image-spec v1.1 \
        --artifact-type application/vnd.just-ai.nora.redb-evidence.v1 \
        --annotation "io.nora.source-tree=$expected_tree" \
        --annotation "io.nora.source-digest=$expected_source_digest" \
        --annotation "io.nora.evidence-bundle-sha256=$bundle_sha256" \
        --export-manifest "$RUN_ROOT/pushed-manifest.json" \
        "$TARGET" \
        redb-production-evidence.tar:application/vnd.just-ai.nora.redb-evidence.layer.v1.tar \
        redb-production-matrix.json:application/vnd.just-ai.nora.redb-matrix.v1+json
)
local_manifest_digest="sha256:$(sha256sum "$RUN_ROOT/pushed-manifest.json" | awk '{print $1}')"
descriptor=$(timeout -s TERM -k 30s "${ORAS_TIMEOUT_SECS}s" \
    oras manifest fetch --descriptor "$TARGET")
remote_manifest_digest=$(jq -er '.digest | select(test("^sha256:[0-9a-f]{64}$"))' <<<"$descriptor")
[[ "$remote_manifest_digest" == "$local_manifest_digest" ]] || {
    echo "Harbor manifest digest differs from the locally pushed manifest" >&2
    exit 1
}

repository=${TARGET%:*}
immutable_ref="$repository@$remote_manifest_digest"
timeout -s TERM -k 30s "${ORAS_TIMEOUT_SECS}s" \
    oras pull --no-tty --output "$RUN_ROOT/readback" "$immutable_ref" >/dev/null
mapfile -t pulled_files < <(find "$RUN_ROOT/readback" -maxdepth 1 -type f -printf '%f\n' | LC_ALL=C sort)
[[ "${pulled_files[*]}" == "redb-production-evidence.tar redb-production-matrix.json" ]] || {
    echo "immutable OCI evidence read-back returned an unexpected payload" >&2
    exit 1
}
[[ $(sha256sum "$RUN_ROOT/readback/redb-production-evidence.tar" | awk '{print $1}') == "$bundle_sha256" ]] || {
    echo "read-back evidence bundle bytes differ" >&2
    exit 1
}
[[ $(sha256sum "$RUN_ROOT/readback/redb-production-matrix.json" | awk '{print $1}') == "$matrix_sha256" ]] || {
    echo "read-back matrix manifest bytes differ" >&2
    exit 1
}

approval="$OUTPUT/redb-production-approval.json"
jq \
    --arg bundle_uri "oci://$immutable_ref" \
    --arg oci_manifest_digest "$remote_manifest_digest" \
    --arg bundle_sha256 "$bundle_sha256" \
    --arg matrix_manifest_sha256 "$matrix_sha256" \
    '{
        schema: 3,
        redb_source_kind: "git",
        redb_repository: .redb.repository,
        redb_revision: .redb.revision,
        redb_resolved_commit: .redb.revision,
        redb_package_version: .redb.package_version,
        redb_lock_source: .redb.lock_source,
        cargo_lock_sha256: .cargo_lock_sha256,
        engine_revision: .engine_revision,
        nora_schema_version: .nora_schema_version,
        nora_source_digest: .source_digest,
        image_ref: .image_ref,
        image_digest: .image_digest,
        image_source_tree: .source_tree,
        harnesses: .harnesses,
        bundle_uri: $bundle_uri,
        evidence_oci_manifest_digest: $oci_manifest_digest,
        bundle_sha256: $bundle_sha256,
        matrix_manifest_sha256: $matrix_manifest_sha256,
        verified_phases: .verified_phases
    }' "$matrix" >"$approval"

jq -n \
    --arg target "$TARGET" \
    --arg immutable_ref "$immutable_ref" \
    --arg oci_manifest_digest "$remote_manifest_digest" \
    --arg bundle_sha256 "$bundle_sha256" \
    --arg matrix_manifest_sha256 "$matrix_sha256" \
    --arg approval_sha256 "$(sha256sum "$approval" | awk '{print $1}')" \
    '{
        schema: 1,
        tag: $target,
        immutable_ref: $immutable_ref,
        evidence_oci_manifest_digest: $oci_manifest_digest,
        bundle_sha256: $bundle_sha256,
        matrix_manifest_sha256: $matrix_manifest_sha256,
        approval_manifest_sha256: $approval_sha256,
        read_back_verified: true
    }' >"$OUTPUT/redb-production-oci-receipt.json"

echo "PASS: immutable redb production evidence published and read back"
echo "evidence_ref=$immutable_ref"
echo "evidence_oci_manifest_digest=$remote_manifest_digest"
echo "evidence_bundle_sha256=$bundle_sha256"
echo "matrix_manifest_sha256=$matrix_sha256"
echo "approval_manifest=$approval"
