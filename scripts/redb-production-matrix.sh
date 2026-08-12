#!/usr/bin/env bash
# Full production qualification matrix for NORA's selected redb engine.
#
# This script is intentionally non-promoting: it tests one exact image and
# produces a deterministic evidence bundle. OCI publication and image/chart
# promotion are separate, digest-verifying steps.

set -Eeuo pipefail

WORKTREE_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
IMAGE=${NORA_REDB_MATRIX_IMAGE:-${1:-}}
EXPECTED_SOURCE_TREE=${NORA_REDB_MATRIX_SOURCE_TREE:-${2:-}}
EVIDENCE_OUTPUT=${NORA_REDB_MATRIX_EVIDENCE_DIR:-${3:-}}
REQUIRE_REMOTE_DIGEST=${NORA_REDB_MATRIX_REQUIRE_REMOTE_DIGEST:-1}
COMPONENT_TIMEOUT_SECS=${NORA_REDB_MATRIX_COMPONENT_TIMEOUT_SECS:-2700}
RUST_IMAGE=docker-hub.just-ai.com/infra/artifact-nora@sha256:365bc9b835ea399bf25a9259fa83d6960b4ad1d7349864c62404f5599e211841
MINIO_IMAGE=minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e
MC_IMAGE=minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727
PASS_MARKER_SHA256=9f56e761d79bfdb34304a012586cb04d16b435ef6130091a97702e559260a2f2

if [[ -z "$IMAGE" || ! "$EXPECTED_SOURCE_TREE" =~ ^[0-9a-f]{40}$ || -z "$EVIDENCE_OUTPUT" ]]; then
    echo "usage: $0 <image> <40-hex-source-tree> <evidence-output-directory>" >&2
    exit 2
fi
[[ "$REQUIRE_REMOTE_DIGEST" == 0 || "$REQUIRE_REMOTE_DIGEST" == 1 ]] || {
    echo "NORA_REDB_MATRIX_REQUIRE_REMOTE_DIGEST must be 0 or 1" >&2
    exit 2
}
if [[ ! "$COMPONENT_TIMEOUT_SECS" =~ ^[0-9]+$ ]] \
    || ((COMPONENT_TIMEOUT_SECS < 600 || COMPONENT_TIMEOUT_SECS > 7200)); then
    echo "NORA_REDB_MATRIX_COMPONENT_TIMEOUT_SECS must be between 600 and 7200" >&2
    exit 2
fi
[[ ${NORA_REDB_MATRIX_UPSTREAM_RUNNER:-docker} == docker ]] || {
    echo "production qualification supports only the pinned Docker runner" >&2
    exit 2
}
for command in docker git jq python3 sha256sum tar timeout; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 2
    }
done
if [[ "$REQUIRE_REMOTE_DIGEST" == 1 && ! "$IMAGE" =~ @sha256:[0-9a-f]{64}$ ]]; then
    echo "production matrix requires an immutable remote image reference" >&2
    exit 2
fi
mkdir -p "$EVIDENCE_OUTPUT"
EVIDENCE_OUTPUT=$(cd -- "$EVIDENCE_OUTPUT" && pwd)
if [[ -z ${NORA_REDB_MATRIX_SNAPSHOT_ROOT:-} ]]; then
    if ! git -C "$WORKTREE_ROOT" diff --quiet \
        || [[ -n $(git -C "$WORKTREE_ROOT" ls-files --others --exclude-standard) ]]; then
        echo "production matrix requires an exact staged tree with no unstaged or untracked inputs" >&2
        exit 1
    fi
    actual_source_tree=$(git -C "$WORKTREE_ROOT" write-tree)
    [[ "$actual_source_tree" == "$EXPECTED_SOURCE_TREE" ]] || {
        echo "production matrix source tree does not match the reviewed tree" >&2
        exit 1
    }
    bootstrap_root=$(mktemp -d /tmp/nora-redb-production-matrix.XXXXXXXX)
    bootstrap_cleanup() {
        local exit_code=$?
        case "$bootstrap_root" in
            /tmp/nora-redb-production-matrix.*) rm -rf -- "$bootstrap_root" ;;
            *) echo "refusing to remove unexpected bootstrap root" >&2 ;;
        esac
        exit "$exit_code"
    }
    trap bootstrap_cleanup EXIT INT TERM
    mkdir -p "$bootstrap_root/source"
    git -C "$WORKTREE_ROOT" archive --format=tar "$EXPECTED_SOURCE_TREE" \
        | tar -xf - -C "$bootstrap_root/source"
    exec env \
        NORA_REDB_MATRIX_SNAPSHOT_ROOT="$bootstrap_root/source" \
        NORA_REDB_MATRIX_RUN_ROOT="$bootstrap_root" \
        NORA_REDB_MATRIX_COMPONENT_TIMEOUT_SECS="$COMPONENT_TIMEOUT_SECS" \
        NORA_REDB_MATRIX_REQUIRE_REMOTE_DIGEST="$REQUIRE_REMOTE_DIGEST" \
        "$bootstrap_root/source/scripts/redb-production-matrix.sh" \
        "$IMAGE" "$EXPECTED_SOURCE_TREE" "$EVIDENCE_OUTPUT"
fi

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
[[ "$ROOT" == "$(cd -- "$NORA_REDB_MATRIX_SNAPSHOT_ROOT" && pwd)" ]] || {
    echo "production matrix snapshot root does not match the running harness" >&2
    exit 1
}
RUN_ROOT=${NORA_REDB_MATRIX_RUN_ROOT:?production matrix run root is missing}
[[ "$RUN_ROOT" == /tmp/nora-redb-production-matrix.* && -d "$RUN_ROOT" ]] || {
    echo "production matrix run root is invalid" >&2
    exit 1
}
snapshot_git="$RUN_ROOT/snapshot-verify.git"
git init --bare --quiet "$snapshot_git"
GIT_DIR="$snapshot_git" GIT_WORK_TREE="$ROOT" \
    git -c core.autocrlf=false -c core.filemode=true -c core.symlinks=true \
    add -f -A
snapshot_tree=$(GIT_DIR="$snapshot_git" GIT_WORK_TREE="$ROOT" git write-tree)
[[ "$snapshot_tree" == "$EXPECTED_SOURCE_TREE" ]] || {
    echo "production matrix snapshot bytes do not match the reviewed tree" >&2
    exit 1
}
source_digest=$(GIT_DIR="$snapshot_git" GIT_WORK_TREE="$ROOT" \
    python3 "$ROOT/scripts/nora-source-digest.py" "$ROOT")
RUN_TOKEN=$(basename "$RUN_ROOT" | tr -cd 'a-zA-Z0-9_.-')
EVIDENCE="$RUN_ROOT/evidence"
ENGINE_CONTAINER="${RUN_TOKEN}-redb-engine"
mkdir -p "$EVIDENCE/minio" "$EVIDENCE/runtime" "$EVIDENCE/upstream" \
    "$EVIDENCE/components"

cleanup() {
    local exit_code=$?
    trap - EXIT INT TERM
    timeout -s TERM -k 5s 30s docker rm -f "$ENGINE_CONTAINER" \
        >/dev/null 2>&1 || true
    case "$RUN_ROOT" in
        /tmp/nora-redb-production-matrix.*) rm -rf -- "$RUN_ROOT" ;;
        *) echo "refusing to remove unexpected run root: $RUN_ROOT" >&2 ;;
    esac
    exit "$exit_code"
}
trap cleanup EXIT INT TERM

if [[ "$REQUIRE_REMOTE_DIGEST" == 1 ]]; then
    timeout -s TERM -k 30s "${COMPONENT_TIMEOUT_SECS}s" \
        docker pull "$IMAGE" >"$EVIDENCE/image-pull.log"
fi
image_id=$(timeout -s TERM -k 10s 60s \
    docker image inspect --format '{{.Id}}' "$IMAGE")
image_source_tree=$(timeout -s TERM -k 10s 60s docker image inspect \
    --format '{{index .Config.Labels "io.nora.source-tree"}}' "$IMAGE")
image_lock_sha256=$(timeout -s TERM -k 10s 60s docker image inspect \
    --format '{{index .Config.Labels "io.nora.cargo-lock-sha256"}}' "$IMAGE")
image_repo_digests=$(timeout -s TERM -k 10s 60s \
    docker image inspect --format '{{json .RepoDigests}}' "$IMAGE")
cargo_lock_sha256=$(sha256sum "$ROOT/Cargo.lock" | awk '{print $1}')
[[ "$image_source_tree" == "$EXPECTED_SOURCE_TREE" ]] || {
    echo "matrix image source-tree label does not match the reviewed tree" >&2
    exit 1
}
[[ "$image_lock_sha256" == "$cargo_lock_sha256" ]] || {
    echo "matrix image Cargo.lock label does not match the reviewed lock" >&2
    exit 1
}
if [[ "$REQUIRE_REMOTE_DIGEST" == 1 ]]; then
    image_digest=${IMAGE##*@}
    jq -e --arg expected "$IMAGE" \
        'any(.[]; . == $expected)' <<<"$image_repo_digests" >/dev/null || {
        echo "pulled image does not report the exact immutable repository digest" >&2
        exit 1
    }
else
    image_digest=${image_id#sha256:}
fi

engine_revision=$(sed -nE 's/^const ENGINE_REVISION: &str = "([^"]+)";/\1/p' \
    "$ROOT/nora-registry/src/repo_index/redb_store.rs")
schema_version=$(sed -nE 's/^const SCHEMA_VERSION: u32 = ([0-9]+);/\1/p' \
    "$ROOT/nora-registry/src/repo_index/redb_store.rs")
[[ -n "$engine_revision" && -n "$schema_version" ]] || {
    echo "cannot resolve NORA redb engine/schema identity" >&2
    exit 1
}

echo "component=minio-s3"
NORA_REDB_E2E_IMAGE="$IMAGE" \
NORA_REDB_E2E_SOURCE_TREE="$EXPECTED_SOURCE_TREE" \
NORA_REDB_E2E_PRODUCTION_MATRIX=1 \
NORA_REDB_E2E_EVIDENCE_DIR="$EVIDENCE/minio" \
NORA_REDB_E2E_MINIO_IMAGE="$MINIO_IMAGE" \
NORA_REDB_E2E_MC_IMAGE="$MC_IMAGE" \
    timeout -s TERM -k 30s "${COMPONENT_TIMEOUT_SECS}s" \
        "$ROOT/scripts/redb-minio-e2e.sh" | tee "$EVIDENCE/minio-component.log"
mapfile -t minio_evidence_dirs \
    < <(find "$EVIDENCE/minio" -mindepth 1 -maxdepth 1 -type d -print)
((${#minio_evidence_dirs[@]} == 1)) || {
    echo "MinIO component did not produce exactly one evidence directory" >&2
    exit 1
}
minio_manifest=${minio_evidence_dirs[0]}/redb-minio-e2e-evidence.json
minio_bundle=${minio_evidence_dirs[0]}/redb-minio-e2e-evidence.tar
jq -e --arg minio "$MINIO_IMAGE" --arg mc "$MC_IMAGE" \
    --arg pass_marker "$PASS_MARKER_SHA256" '
    .production_matrix_component_complete == true
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
         | all(. as $path | $doc.artifacts[$path] == $pass_marker))
' \
    "$minio_manifest" >/dev/null
cp -- "$minio_manifest" "$EVIDENCE/components/minio-manifest.json"
cp -- "$minio_bundle" "$EVIDENCE/components/minio-evidence.tar"
python3 - "$minio_manifest" "$minio_bundle" <<'PY'
import hashlib
import json
import pathlib
import re
import sys
import tarfile

manifest_path, bundle_path = sys.argv[1:]
with open(manifest_path, encoding="utf-8") as handle:
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
with tarfile.open(bundle_path, "r:") as archive:
    for member in archive.getmembers():
        name = member.name
        if name in seen or name not in artifacts or not member.isfile():
            raise SystemExit("MinIO evidence tar is not the canonical artifact set")
        seen.add(name)
        source = archive.extractfile(member)
        if source is None:
            raise SystemExit("MinIO evidence artifact cannot be read")
        digest = hashlib.sha256()
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
        if digest.hexdigest() != artifacts[name]:
            raise SystemExit("MinIO evidence artifact digest differs from its manifest")
if seen != set(artifacts):
    raise SystemExit("MinIO evidence tar lacks a canonical artifact")
PY

echo "component=redb-engine"
timeout -s TERM -k 30s "${COMPONENT_TIMEOUT_SECS}s" \
    docker run --rm \
    --name "$ENGINE_CONTAINER" \
    --user 0:0 \
    -v "$ROOT:/source:ro" \
    -v "$EVIDENCE:/evidence" \
    -w /source \
    -e CARGO_TARGET_DIR=/tmp/cargo-target \
    "$RUST_IMAGE" \
    /bin/sh -c '/source/scripts/run-nora-redb-runtime-regressions.sh /evidence/runtime && /source/scripts/run-redb-upstream-regressions.sh /evidence/upstream && chown -R '"$(id -u):$(id -g)"' /evidence' \
    | tee "$EVIDENCE/engine-components.log"
runtime_manifest="$EVIDENCE/runtime/nora-redb-runtime-regressions.json"
jq -e '
    .phases.timeout_reap == "pass"
    and .phases.second_open == "pass"
    and .phases.generic_io_policy == "pass"
    and .phases.abnormal_shutdown == "pass"
    and .phases.shutdown_fence == "pass"
' "$runtime_manifest" >/dev/null
upstream_manifest="$EVIDENCE/upstream/redb-upstream-regressions.json"
jq -e '
    .phases.growing_commit_crash == "pass"
    and .phases.double_crash_torn_slot == "pass"
    and .phases.torn_region_counts == "pass"
    and .phases.commit_error_poison == "pass"
    and .phases.generic_io_recovery == "pass"
    and .phases.enospc_recovery == "pass"
' "$upstream_manifest" >/dev/null
redb_revision=$(jq -er '.redb_revision | select(test("^[0-9a-f]{40}$"))' \
    "$upstream_manifest")
redb_version=$(jq -er '.redb_version | select(test("^[0-9]+\\.[0-9]+\\.[0-9]+$"))' \
    "$upstream_manifest")
redb_source=$(jq -er '.redb_source | select(startswith("git+https://github.com/cberner/redb?rev="))' \
    "$upstream_manifest")
redb_regression_lock_sha256=$(jq -er \
    '.generated_lock_sha256 | select(test("^[0-9a-f]{64}$"))' \
    "$upstream_manifest")
[[ "$redb_source" == "git+https://github.com/cberner/redb?rev=$redb_revision#$redb_revision" ]] || {
    echo "upstream regression source is not the exact canonical redb revision" >&2
    exit 1
}

minio_manifest_sha256=$(sha256sum "$minio_manifest" | awk '{print $1}')
minio_bundle_sha256=$(sha256sum "$minio_bundle" | awk '{print $1}')
runtime_manifest_sha256=$(sha256sum "$runtime_manifest" | awk '{print $1}')
upstream_manifest_sha256=$(sha256sum "$upstream_manifest" | awk '{print $1}')
matrix_harness_sha256=$(sha256sum "$ROOT/scripts/redb-production-matrix.sh" | awk '{print $1}')
dev_harness_sha256=$(sha256sum "$ROOT/scripts/redb-minio-e2e.sh" | awk '{print $1}')
runtime_harness_sha256=$(sha256sum "$ROOT/scripts/run-nora-redb-runtime-regressions.sh" | awk '{print $1}')
upstream_harness_sha256=$(sha256sum "$ROOT/scripts/run-redb-upstream-regressions.sh" | awk '{print $1}')
enospc_test_sha256=$(sha256sum "$ROOT/scripts/redb-enospc-regression.rs" | awk '{print $1}')

verify_component_logs() {
    local manifest=$1 directory=$2 phase digest log
    while IFS=$'\t' read -r phase digest; do
        [[ "$phase" =~ ^[a-z0-9_]+$ && "$digest" =~ ^[0-9a-f]{64}$ ]] || {
            echo "component manifest contains an invalid log identity" >&2
            return 1
        }
        log="$directory/$phase.log"
        [[ -s "$log" && $(sha256sum "$log" | awk '{print $1}') == "$digest" ]] || {
            echo "component log is missing or differs from its manifest: $phase" >&2
            return 1
        }
    done < <(jq -er '.logs | to_entries[] | [.key, .value] | @tsv' "$manifest")
}

jq -e \
    --arg source_tree "$EXPECTED_SOURCE_TREE" \
    --arg lock "$cargo_lock_sha256" \
    --arg image "$IMAGE" \
    --arg engine "$engine_revision" \
    --argjson schema "$schema_version" \
    '.source_tree == $source_tree
     and .cargo_lock_sha256 == $lock
     and .image == $image
     and .engine_revision == $engine
     and .nora_schema_version == $schema
     and .production_matrix_component_complete == true' \
    "$minio_manifest" >/dev/null
jq -e --arg harness "$runtime_harness_sha256" \
    '.harness_sha256 == $harness and ([.phases[]] | all(. == "pass"))' \
    "$runtime_manifest" >/dev/null
jq -e --arg harness "$upstream_harness_sha256" \
    --arg enospc "$enospc_test_sha256" \
    --arg revision "$redb_revision" \
    '.harness_sha256 == $harness
     and .enospc_test_sha256 == $enospc
     and .redb_revision == $revision
     and ([.phases[]] | all(. == "pass"))' \
    "$upstream_manifest" >/dev/null
verify_component_logs "$runtime_manifest" "$EVIDENCE/runtime"
verify_component_logs "$upstream_manifest" "$EVIDENCE/upstream"

jq -n \
    --arg source_tree "$EXPECTED_SOURCE_TREE" \
    --arg source_digest "$source_digest" \
    --arg cargo_lock_sha256 "$cargo_lock_sha256" \
    --arg image_ref "$IMAGE" \
    --arg image_digest "$image_digest" \
    --arg image_id "$image_id" \
    --argjson image_repo_digests "$image_repo_digests" \
    --arg engine_revision "$engine_revision" \
    --argjson schema_version "$schema_version" \
    --arg redb_revision "$redb_revision" \
    --arg redb_version "$redb_version" \
    --arg redb_source "$redb_source" \
    --arg redb_regression_lock_sha256 "$redb_regression_lock_sha256" \
    --arg matrix_harness_sha256 "$matrix_harness_sha256" \
    --arg dev_harness_sha256 "$dev_harness_sha256" \
    --arg runtime_harness_sha256 "$runtime_harness_sha256" \
    --arg upstream_harness_sha256 "$upstream_harness_sha256" \
    --arg enospc_test_sha256 "$enospc_test_sha256" \
    --arg minio_manifest_sha256 "$minio_manifest_sha256" \
    --arg minio_bundle_sha256 "$minio_bundle_sha256" \
    --arg runtime_manifest_sha256 "$runtime_manifest_sha256" \
    --arg upstream_manifest_sha256 "$upstream_manifest_sha256" \
    --arg rust_image "$RUST_IMAGE" \
    --arg minio_image "$MINIO_IMAGE" \
    --arg mc_image "$MC_IMAGE" \
    '{
        schema: 1,
        production_matrix_complete: true,
        source_tree: $source_tree,
        source_digest: $source_digest,
        cargo_lock_sha256: $cargo_lock_sha256,
        image_ref: $image_ref,
        image_digest: $image_digest,
        image_id: $image_id,
        image_repo_digests: $image_repo_digests,
        engine_revision: $engine_revision,
        nora_schema_version: $schema_version,
        execution: {
            source_mode: "verified_git_tree_snapshot",
            source_tree: $source_tree,
            runner_kind: "docker"
        },
        redb: {
            repository: "https://github.com/cberner/redb",
            revision: $redb_revision,
            package_version: $redb_version,
            lock_source: $redb_source,
            regression_lock_sha256: $redb_regression_lock_sha256
        },
        harnesses: {
            matrix_sha256: $matrix_harness_sha256,
            minio_sha256: $dev_harness_sha256,
            runtime_sha256: $runtime_harness_sha256,
            upstream_sha256: $upstream_harness_sha256,
            enospc_test_sha256: $enospc_test_sha256
        },
        components: {
            minio: {
                manifest_path: "components/minio-manifest.json",
                bundle_path: "components/minio-evidence.tar",
                manifest_sha256: $minio_manifest_sha256,
                bundle_sha256: $minio_bundle_sha256,
                minio_image: $minio_image,
                mc_image: $mc_image
            },
            runtime: {
                manifest_path: "runtime/nora-redb-runtime-regressions.json",
                manifest_sha256: $runtime_manifest_sha256,
                runner_kind: "docker",
                runner_image: $rust_image
            },
            upstream: {
                manifest_path: "upstream/redb-upstream-regressions.json",
                manifest_sha256: $upstream_manifest_sha256,
                runner_kind: "docker",
                runner_image: $rust_image
            }
        },
        verified_phases: [
            "sigkill",
            "enospc",
            "double_crash",
            "torn_write",
            "corruption",
            "generic_io",
            "timeout",
            "second_open",
            "recovery"
        ]
    }' >"$EVIDENCE/redb-production-matrix.json"

bundle="$RUN_ROOT/redb-production-evidence.tar"
bundle_members=(
    redb-production-matrix.json
    components/minio-manifest.json
    components/minio-evidence.tar
    runtime/nora-redb-runtime-regressions.json
    runtime/timeout_reap.log
    runtime/second_open.log
    runtime/generic_io_policy.log
    runtime/abnormal_shutdown.log
    runtime/shutdown_fence.log
    upstream/redb-upstream-regressions.json
    upstream/redb-regression-Cargo.lock
    upstream/growing_commit_crash.log
    upstream/double_crash_torn_slot.log
    upstream/torn_region_counts.log
    upstream/commit_error_poison.log
    upstream/generic_io_recovery.log
    upstream/enospc_recovery.log
)
for member in "${bundle_members[@]}"; do
    [[ -f "$EVIDENCE/$member" && ! -L "$EVIDENCE/$member" ]] || {
        echo "production evidence lacks a canonical regular member: $member" >&2
        exit 1
    }
done
mapfile -t actual_bundle_members \
    < <(cd -- "$EVIDENCE" && find components runtime upstream \
        -type f -print | LC_ALL=C sort)
mapfile -t expected_component_members \
    < <(printf '%s\n' "${bundle_members[@]:1}" | LC_ALL=C sort)
[[ "${actual_bundle_members[*]}" == "${expected_component_members[*]}" ]] || {
    echo "production evidence contains a non-canonical component member" >&2
    exit 1
}
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
    -C "$EVIDENCE" -cf "$bundle" -- "${bundle_members[@]}"
bundle_sha256=$(sha256sum "$bundle" | awk '{print $1}')
matrix_manifest_sha256=$(sha256sum "$EVIDENCE/redb-production-matrix.json" | awk '{print $1}')
destination="$EVIDENCE_OUTPUT/$RUN_TOKEN"
[[ ! -e "$destination" ]] || {
    echo "evidence destination already exists: $destination" >&2
    exit 1
}
mkdir -p "$destination"
cp -- "$bundle" "$destination/redb-production-evidence.tar"
cp -- "$EVIDENCE/redb-production-matrix.json" "$destination/"

echo "PASS: full redb production qualification matrix"
echo "evidence_dir=$destination"
echo "source_tree=$EXPECTED_SOURCE_TREE"
echo "source_digest=$source_digest"
echo "image_ref=$IMAGE"
echo "image_digest=$image_digest"
echo "engine_revision=$engine_revision"
echo "schema_version=$schema_version"
echo "evidence_bundle_sha256=$bundle_sha256"
echo "matrix_manifest_sha256=$matrix_manifest_sha256"
