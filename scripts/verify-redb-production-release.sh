#!/usr/bin/env bash

set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
MANIFEST="$ROOT/nora-registry/Cargo.toml"
LOCKFILE="$ROOT/Cargo.lock"
ALLOWLIST="$ROOT/scripts/redb-production-allowlist.txt"
STORE="$ROOT/nora-registry/src/repo_index/redb_store.rs"
HARNESS="$ROOT/scripts/redb-minio-e2e.sh"
MATRIX_HARNESS="$ROOT/scripts/redb-production-matrix.sh"
RUNTIME_HARNESS="$ROOT/scripts/run-nora-redb-runtime-regressions.sh"
UPSTREAM_HARNESS="$ROOT/scripts/run-redb-upstream-regressions.sh"
ENOSPC_TEST="$ROOT/scripts/redb-enospc-regression.rs"
SOURCE_DIGEST_TOOL="$ROOT/scripts/nora-source-digest.py"
ORAS_TIMEOUT_SECS=${NORA_REDB_RELEASE_ORAS_TIMEOUT_SECS:-900}
RUST_IMAGE=docker-hub.just-ai.com/infra/artifact-nora@sha256:365bc9b835ea399bf25a9259fa83d6960b4ad1d7349864c62404f5599e211841
MINIO_IMAGE=minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e
MC_IMAGE=minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727
PASS_MARKER_SHA256=9f56e761d79bfdb34304a012586cb04d16b435ef6130091a97702e559260a2f2

for required in "$MANIFEST" "$LOCKFILE" "$ALLOWLIST" "$STORE" "$HARNESS" \
    "$MATRIX_HARNESS" "$RUNTIME_HARNESS" "$UPSTREAM_HARNESS" "$ENOSPC_TEST" \
    "$SOURCE_DIGEST_TOOL"; do
    if [[ ! -f "$required" ]]; then
        echo "release blocked: required release evidence file is missing" >&2
        exit 1
    fi
done
if [[ ! "$ORAS_TIMEOUT_SECS" =~ ^[0-9]+$ ]] \
    || ((ORAS_TIMEOUT_SECS < 60 || ORAS_TIMEOUT_SECS > 1800)); then
    echo "release blocked: NORA_REDB_RELEASE_ORAS_TIMEOUT_SECS must be between 60 and 1800" >&2
    exit 2
fi
for command in cargo git jq oras python3 sha256sum tar timeout; do
    command -v "$command" >/dev/null || {
        echo "release blocked: required verifier command is missing: $command" >&2
        exit 1
    }
done

tracked_blob_oid() {
    local relative=$1 entry mode oid stage path
    entry=$(git -C "$ROOT" ls-files --stage -- "$relative")
    read -r mode oid stage path <<<"$entry"
    if [[ -z "$entry" || "$stage" != 0 || ! "$mode" =~ ^100(644|755)$ || "$path" != "$relative" ]]; then
        return 1
    fi
    printf '%s\n' "$oid"
}

if ! git -C "$ROOT" diff --quiet -- \
    || [[ -n $(git -C "$ROOT" ls-files --others --exclude-standard) ]]; then
    echo "release blocked: source tree has unstaged or untracked inputs" >&2
    exit 1
fi
source_digest=$(python3 "$SOURCE_DIGEST_TOOL" "$ROOT")
if [[ ! "$source_digest" =~ ^[0-9a-f]{64}$ ]]; then
    echo "release blocked: canonical source digest is missing" >&2
    exit 1
fi
tracked_blob_oid nora-registry/Cargo.toml >/dev/null || {
    echo "release blocked: Cargo manifest is not one tracked regular stage-0 blob" >&2
    exit 1
}
tracked_blob_oid Cargo.lock >/dev/null || {
    echo "release blocked: Cargo.lock is not one tracked regular stage-0 blob" >&2
    exit 1
}
allowlist_oid=$(tracked_blob_oid scripts/redb-production-allowlist.txt) || {
    echo "release blocked: redb allowlist is not one tracked regular stage-0 blob" >&2
    exit 1
}
store_oid=$(tracked_blob_oid nora-registry/src/repo_index/redb_store.rs) || {
    echo "release blocked: redb store source is not one tracked regular stage-0 blob" >&2
    exit 1
}
harness_oid=$(tracked_blob_oid scripts/redb-minio-e2e.sh) || {
    echo "release blocked: redb harness is not one tracked regular stage-0 blob" >&2
    exit 1
}
matrix_harness_oid=$(tracked_blob_oid scripts/redb-production-matrix.sh) || {
    echo "release blocked: production matrix is not one tracked regular stage-0 blob" >&2
    exit 1
}
runtime_harness_oid=$(tracked_blob_oid scripts/run-nora-redb-runtime-regressions.sh) || {
    echo "release blocked: runtime regression harness is not one tracked regular stage-0 blob" >&2
    exit 1
}
upstream_harness_oid=$(tracked_blob_oid scripts/run-redb-upstream-regressions.sh) || {
    echo "release blocked: upstream regression harness is not one tracked regular stage-0 blob" >&2
    exit 1
}
enospc_test_oid=$(tracked_blob_oid scripts/redb-enospc-regression.rs) || {
    echo "release blocked: ENOSPC regression is not one tracked regular stage-0 blob" >&2
    exit 1
}
tracked_blob_oid scripts/nora-source-digest.py >/dev/null || {
    echo "release blocked: source-digest tool is not one tracked regular stage-0 blob" >&2
    exit 1
}
harness_digest=$(git -C "$ROOT" cat-file blob "$harness_oid" | sha256sum | awk '{print $1}')
matrix_harness_digest=$(git -C "$ROOT" cat-file blob "$matrix_harness_oid" | sha256sum | awk '{print $1}')
runtime_harness_digest=$(git -C "$ROOT" cat-file blob "$runtime_harness_oid" | sha256sum | awk '{print $1}')
upstream_harness_digest=$(git -C "$ROOT" cat-file blob "$upstream_harness_oid" | sha256sum | awk '{print $1}')
enospc_test_digest=$(git -C "$ROOT" cat-file blob "$enospc_test_oid" | sha256sum | awk '{print $1}')

dependency_identity=$(python3 - "$MANIFEST" <<'PY'
import re
import sys
import tomllib

canonical_repository = "https://github.com/cberner/redb"
with open(sys.argv[1], "rb") as handle:
    manifest = tomllib.load(handle)
dependency = manifest.get("dependencies", {}).get("redb")
if not isinstance(dependency, dict):
    raise SystemExit("release blocked: redb must use an exact reviewed git dependency")
for forbidden in ("version", "branch", "tag", "path", "workspace", "registry"):
    if dependency.get(forbidden):
        raise SystemExit(f"release blocked: redb dependency must not set {forbidden}")
repository = dependency.get("git", "")
revision = dependency.get("rev", "")
if repository != canonical_repository:
    raise SystemExit("release blocked: redb git repository is not the reviewed upstream")
if not re.fullmatch(r"[0-9a-f]{40}", revision):
    raise SystemExit("release blocked: redb must pin one full 40-hex revision")
print(f"git\t{revision}\t{repository}")
PY
)
IFS=$'\t' read -r source_kind source_ref source_repository <<<"$dependency_identity"

RUN_ROOT=$(mktemp -d /tmp/nora-redb-release-verify.XXXXXXXX)
cleanup() {
    local exit_code=$?
    trap - EXIT INT TERM
    case "$RUN_ROOT" in
        /tmp/nora-redb-release-verify.*) rm -rf -- "$RUN_ROOT" ;;
        *) echo "release blocked: refusing to remove unexpected verifier path" >&2 ;;
    esac
    exit "$exit_code"
}
trap cleanup EXIT INT TERM
metadata="$RUN_ROOT/cargo-metadata.json"
if ! timeout -s TERM -k 30s 600s \
    cargo metadata --locked --format-version 1 >"$metadata"; then
    echo "release blocked: cargo metadata --locked failed" >&2
    exit 1
fi

resolved_identity=$(python3 - "$metadata" "$LOCKFILE" "$MANIFEST" "$source_ref" "$source_repository" <<'PY'
import json
import os
import re
import sys
import tomllib

metadata_path, lock_path, manifest_path, expected_revision, expected_repository = sys.argv[1:]
with open(metadata_path, encoding="utf-8") as handle:
    metadata = json.load(handle)
manifest_path = os.path.realpath(manifest_path)
packages = {package["id"]: package for package in metadata.get("packages", [])}
workspace_package = next(
    (package for package in packages.values() if os.path.realpath(package["manifest_path"]) == manifest_path),
    None,
)
if workspace_package is None:
    raise SystemExit("release blocked: nora-registry is missing from cargo metadata")
resolve = metadata.get("resolve") or {}
node = next((node for node in resolve.get("nodes", []) if node["id"] == workspace_package["id"]), None)
if node is None:
    raise SystemExit("release blocked: nora-registry dependency resolution is missing")
redb_ids = [dependency["pkg"] for dependency in node.get("deps", []) if dependency.get("name") == "redb"]
if len(redb_ids) != 1:
    raise SystemExit("release blocked: expected one resolved direct redb dependency")
resolved = packages.get(redb_ids[0])
if resolved is None:
    raise SystemExit("release blocked: resolved redb package is missing")
source = resolved.get("source", "")
prefix = f"git+{expected_repository}?rev={expected_revision}#"
if not isinstance(source, str) or not source.startswith(prefix):
    raise SystemExit("release blocked: resolved redb source does not match the reviewed git revision")
resolved_commit = source.removeprefix(prefix)
if resolved_commit != expected_revision:
    raise SystemExit("release blocked: Cargo resolved a different redb commit")
resolved_version = resolved.get("version", "")
if not re.fullmatch(r"\d+\.\d+\.\d+", resolved_version):
    raise SystemExit("release blocked: resolved redb package version is invalid")

with open(lock_path, "rb") as handle:
    lock = tomllib.load(handle)
locked = [
    package
    for package in lock.get("package", [])
    if package.get("name") == "redb"
    and package.get("version") == resolved_version
    and package.get("source") == source
]
if len(locked) != 1:
    raise SystemExit("release blocked: Cargo.lock lacks one exact reviewed redb git package")
if locked[0].get("checksum"):
    raise SystemExit("release blocked: git redb lock entry unexpectedly has a registry checksum")
print(f"{resolved_version}\t{resolved_commit}\t{source}")
PY
)
IFS=$'\t' read -r package_version resolved_commit resolved_source <<<"$resolved_identity"
lock_digest=$(sha256sum "$LOCKFILE" | awk '{print $1}')

engine_revision=$(git -C "$ROOT" cat-file blob "$store_oid" \
    | sed -nE 's/^const ENGINE_REVISION: &str = "([^"]+)";/\1/p')
schema_version=$(git -C "$ROOT" cat-file blob "$store_oid" \
    | sed -nE 's/^const SCHEMA_VERSION: u32 = ([0-9]+);/\1/p')
if [[ -z "$engine_revision" || -z "$schema_version" ]]; then
    echo "release blocked: redb engine/schema identity is missing" >&2
    exit 1
fi

approval=$(git -C "$ROOT" cat-file blob "$allowlist_oid" \
    | awk -v kind="$source_kind" -v ref="$source_ref" \
        '$1 == kind && $2 == ref && $1 !~ /^#/ { print; exit }')
if [[ -z "$approval" ]]; then
    echo "release blocked: redb $source_kind revision $source_ref has no reviewed crash/ENOSPC/recovery evidence" >&2
    exit 1
fi
read -r approved_kind approved_ref approved_commit approved_engine approved_schema evidence_digest evidence_locator extra <<<"$approval"
if [[ -n "${extra:-}" \
    || "$approved_kind" != "$source_kind" \
    || "$approved_ref" != "$source_ref" \
    || "$approved_commit" != "$resolved_commit" \
    || "$approved_engine" != "$engine_revision" \
    || "$approved_schema" != "$schema_version" \
    || ! "$evidence_digest" =~ ^[0-9a-f]{64}$ \
    || ! "$evidence_locator" =~ ^scripts/redb-production-evidence/[A-Za-z0-9._-]+\.json$ ]]; then
    echo "release blocked: redb approval is not bound to resolved checksum, engine, schema and evidence" >&2
    exit 1
fi

evidence_oid=$(tracked_blob_oid "$evidence_locator") || {
    echo "release blocked: reviewed evidence is not one tracked regular stage-0 blob" >&2
    exit 1
}
actual_evidence_digest=$(git -C "$ROOT" cat-file blob "$evidence_oid" \
    | sha256sum | awk '{print $1}')
if [[ "$actual_evidence_digest" != "$evidence_digest" ]]; then
    echo "release blocked: reviewed redb evidence manifest digest does not match the allowlist" >&2
    exit 1
fi
evidence_identity=$(python3 - "$ROOT" "$evidence_oid" "$source_kind" "$source_repository" "$source_ref" \
    "$resolved_commit" "$package_version" "$resolved_source" "$lock_digest" \
    "$engine_revision" "$schema_version" "$source_digest" "$harness_digest" \
    "$matrix_harness_digest" "$runtime_harness_digest" "$upstream_harness_digest" \
    "$enospc_test_digest" <<'PY'
import json
import re
import sys

import subprocess

(root, evidence_oid, source_kind, repository, revision, resolved_commit, package_version,
 lock_source, lock_digest, engine, schema, source_digest, minio_sha256,
 matrix_sha256, runtime_sha256, upstream_sha256, enospc_sha256) = sys.argv[1:]
contents = subprocess.check_output(
    ["git", "-C", root, "cat-file", "blob", evidence_oid],
)
evidence = json.loads(contents)
required_phases = [
    "sigkill",
    "enospc",
    "double_crash",
    "torn_write",
    "corruption",
    "generic_io",
    "timeout",
    "second_open",
    "recovery",
]
if evidence.get("schema") != 3:
    raise SystemExit("release blocked: unsupported redb evidence manifest schema")
if (
    evidence.get("redb_source_kind") != source_kind
    or evidence.get("redb_repository") != repository
    or evidence.get("redb_revision") != revision
    or evidence.get("redb_resolved_commit") != resolved_commit
    or evidence.get("redb_package_version") != package_version
    or evidence.get("redb_lock_source") != lock_source
    or evidence.get("cargo_lock_sha256") != lock_digest
    or evidence.get("engine_revision") != engine
    or evidence.get("nora_schema_version") != int(schema)
    or evidence.get("nora_source_digest") != source_digest
    or evidence.get("harnesses") != {
        "matrix_sha256": matrix_sha256,
        "minio_sha256": minio_sha256,
        "runtime_sha256": runtime_sha256,
        "upstream_sha256": upstream_sha256,
        "enospc_test_sha256": enospc_sha256,
    }
):
    raise SystemExit("release blocked: redb evidence manifest identity does not match the release")
bundle_sha256 = evidence.get("bundle_sha256", "")
bundle_uri = evidence.get("bundle_uri", "")
oci_digest = evidence.get("evidence_oci_manifest_digest", "")
matrix_manifest_sha256 = evidence.get("matrix_manifest_sha256", "")
image_ref = evidence.get("image_ref", "")
image_digest = evidence.get("image_digest", "")
image_source_tree = evidence.get("image_source_tree", "")
if (
    not re.fullmatch(r"[0-9a-f]{64}", bundle_sha256)
    or not re.fullmatch(r"[0-9a-f]{64}", matrix_manifest_sha256)
    or not isinstance(bundle_uri, str)
    or not re.fullmatch(
        r"oci://docker-hub\.just-ai\.com/infra/artifact-nora-redb-evidence@sha256:[0-9a-f]{64}",
        bundle_uri,
    )
    or not re.fullmatch(r"sha256:[0-9a-f]{64}", oci_digest)
    or not re.fullmatch(
        r"docker-hub\.just-ai\.com/infra/artifact-nora@sha256:[0-9a-f]{64}",
        image_ref,
    )
    or not re.fullmatch(r"sha256:[0-9a-f]{64}", image_digest)
    or not re.fullmatch(r"[0-9a-f]{40}", image_source_tree)
):
    raise SystemExit("release blocked: redb evidence manifest lacks an immutable bundle locator")
if bundle_uri.rsplit("@", 1)[-1] != oci_digest:
    raise SystemExit("release blocked: evidence OCI digest differs from its immutable URI")
if image_ref.rsplit("@", 1)[-1] != image_digest:
    raise SystemExit("release blocked: tested image digest differs from its immutable reference")
if evidence.get("verified_phases") != required_phases:
    raise SystemExit("release blocked: redb evidence manifest lacks the required recovery phases")
print(
    bundle_uri,
    oci_digest,
    bundle_sha256,
    matrix_manifest_sha256,
    image_ref,
    image_digest,
    image_source_tree,
    sep="\t",
)
PY
)
IFS=$'\t' read -r bundle_uri evidence_oci_digest bundle_sha256 \
    matrix_manifest_sha256 image_ref image_digest image_source_tree \
    <<<"$evidence_identity"

oras_target=${bundle_uri#oci://}
descriptor=$(timeout -s TERM -k 30s "${ORAS_TIMEOUT_SECS}s" \
    oras manifest fetch --descriptor "$oras_target")
remote_oci_digest=$(jq -er '.digest | select(test("^sha256:[0-9a-f]{64}$"))' \
    <<<"$descriptor")
[[ "$remote_oci_digest" == "$evidence_oci_digest" ]] || {
    echo "release blocked: remote evidence OCI manifest digest changed" >&2
    exit 1
}
mkdir -p "$RUN_ROOT/evidence"
timeout -s TERM -k 30s "${ORAS_TIMEOUT_SECS}s" \
    oras pull --no-tty --output "$RUN_ROOT/evidence" "$oras_target" >/dev/null
mapfile -t evidence_files \
    < <(find "$RUN_ROOT/evidence" -maxdepth 1 -type f -printf '%f\n' | LC_ALL=C sort)
[[ "${evidence_files[*]}" == "redb-production-evidence.tar redb-production-matrix.json" ]] || {
    echo "release blocked: OCI evidence payload contains unexpected files" >&2
    exit 1
}
bundle="$RUN_ROOT/evidence/redb-production-evidence.tar"
matrix="$RUN_ROOT/evidence/redb-production-matrix.json"
[[ $(sha256sum "$bundle" | awk '{print $1}') == "$bundle_sha256" \
    && $(sha256sum "$matrix" | awk '{print $1}') == "$matrix_manifest_sha256" ]] || {
    echo "release blocked: OCI evidence payload digest does not match approval" >&2
    exit 1
}
[[ $(tar -xOf "$bundle" redb-production-matrix.json | sha256sum | awk '{print $1}') \
    == "$matrix_manifest_sha256" ]] || {
    echo "release blocked: evidence bundle embeds a different matrix manifest" >&2
    exit 1
}
jq -e \
    --arg source_digest "$source_digest" \
    --arg lock_digest "$lock_digest" \
    --arg image_ref "$image_ref" \
    --arg image_digest "$image_digest" \
    --arg image_source_tree "$image_source_tree" \
    --arg engine "$engine_revision" \
    --argjson schema "$schema_version" \
    --arg revision "$resolved_commit" \
    --arg matrix_harness_sha256 "$matrix_harness_digest" \
    --arg minio_harness_sha256 "$harness_digest" \
    --arg runtime_harness_sha256 "$runtime_harness_digest" \
    --arg upstream_harness_sha256 "$upstream_harness_digest" \
    --arg enospc_test_sha256 "$enospc_test_digest" \
    --arg rust_image "$RUST_IMAGE" --arg minio_image "$MINIO_IMAGE" \
    --arg mc_image "$MC_IMAGE" \
    '
        .schema == 1
        and .production_matrix_complete == true
        and .source_digest == $source_digest
        and .cargo_lock_sha256 == $lock_digest
        and .source_tree == $image_source_tree
        and .image_ref == $image_ref
        and .image_digest == $image_digest
        and .engine_revision == $engine
        and .nora_schema_version == $schema
        and .execution == {
            source_mode: "verified_git_tree_snapshot",
            source_tree: $image_source_tree,
            runner_kind: "docker"
        }
        and .redb.revision == $revision
        and .harnesses == {
            matrix_sha256: $matrix_harness_sha256,
            minio_sha256: $minio_harness_sha256,
            runtime_sha256: $runtime_harness_sha256,
            upstream_sha256: $upstream_harness_sha256,
            enospc_test_sha256: $enospc_test_sha256
        }
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
            "sigkill", "enospc", "double_crash", "torn_write",
            "corruption", "generic_io", "timeout", "second_open",
            "recovery"
        ]
    ' "$matrix" >/dev/null || {
    echo "release blocked: fetched production matrix does not match the approved release" >&2
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
            raise SystemExit("release blocked: evidence tar is not the canonical member set")
        seen.add(name)
    if seen != expected:
        raise SystemExit("release blocked: evidence tar lacks a canonical member")
PY
mkdir -p "$RUN_ROOT/extracted"
tar -xf "$bundle" -C "$RUN_ROOT/extracted"
minio_manifest="$RUN_ROOT/extracted/components/minio-manifest.json"
minio_bundle="$RUN_ROOT/extracted/components/minio-evidence.tar"
runtime_manifest="$RUN_ROOT/extracted/runtime/nora-redb-runtime-regressions.json"
upstream_manifest="$RUN_ROOT/extracted/upstream/redb-upstream-regressions.json"
upstream_lock="$RUN_ROOT/extracted/upstream/redb-regression-Cargo.lock"
for required in "$minio_manifest" "$minio_bundle" "$runtime_manifest" \
    "$upstream_manifest" "$upstream_lock"; do
    [[ -s "$required" ]] || {
        echo "release blocked: evidence tar lacks a required component artifact" >&2
        exit 1
    }
done

component_hash_matches() {
    local file=$1 expression=$2 expected
    expected=$(jq -er "$expression | select(test(\"^[0-9a-f]{64}$\"))" "$matrix")
    [[ $(sha256sum "$file" | awk '{print $1}') == "$expected" ]]
}
if ! component_hash_matches "$minio_manifest" '.components.minio.manifest_sha256' \
    || ! component_hash_matches "$minio_bundle" '.components.minio.bundle_sha256' \
    || ! component_hash_matches "$runtime_manifest" '.components.runtime.manifest_sha256' \
    || ! component_hash_matches "$upstream_manifest" '.components.upstream.manifest_sha256'; then
    echo "release blocked: component artifact digest differs from the matrix" >&2
    exit 1
fi

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
    --arg tree "$image_source_tree" --arg lock "$lock_digest" \
    --arg image "$image_ref" --arg engine "$engine_revision" \
    --argjson schema "$schema_version" \
    --arg minio_bundle "$(jq -r '.components.minio.bundle_sha256' "$matrix")" \
    --arg minio "$MINIO_IMAGE" --arg mc "$MC_IMAGE" \
    --arg pass_marker "$PASS_MARKER_SHA256" \
    '.source_tree == $tree and .cargo_lock_sha256 == $lock and .image == $image
     and .engine_revision == $engine and .nora_schema_version == $schema
     and .bundle_sha256 == $minio_bundle
     and .production_matrix_component_complete == true
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
    echo "release blocked: MinIO component identity/phases differ from the matrix" >&2
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
    raise SystemExit("release blocked: MinIO manifest lacks its canonical artifact map")
for name, digest in artifacts.items():
    if (
        not isinstance(name, str)
        or name != str(pathlib.PurePosixPath(name))
        or name.startswith("/")
        or ".." in pathlib.PurePosixPath(name).parts
        or not isinstance(digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", digest) is None
    ):
        raise SystemExit("release blocked: MinIO manifest has an unsafe artifact identity")
seen = set()
with tarfile.open(sys.argv[2], "r:") as archive:
    for member in archive.getmembers():
        if member.name in seen or member.name not in artifacts or not member.isfile():
            raise SystemExit("release blocked: MinIO tar is not the canonical artifact set")
        seen.add(member.name)
        source = archive.extractfile(member)
        if source is None:
            raise SystemExit("release blocked: MinIO evidence artifact cannot be read")
        digest = hashlib.sha256()
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
        if digest.hexdigest() != artifacts[member.name]:
            raise SystemExit("release blocked: MinIO artifact digest differs from its manifest")
if seen != set(artifacts):
    raise SystemExit("release blocked: MinIO tar lacks a canonical artifact")
PY
if ! jq -e --arg harness "$runtime_harness_digest" \
    '.harness_sha256 == $harness
     and .phases == {timeout_reap:"pass",second_open:"pass",
        generic_io_policy:"pass",abnormal_shutdown:"pass",shutdown_fence:"pass"}' \
    "$runtime_manifest" >/dev/null \
    || ! verify_component_logs "$runtime_manifest" "$(dirname "$runtime_manifest")" \
        '["abnormal_shutdown","generic_io_policy","second_open","shutdown_fence","timeout_reap"]'; then
    echo "release blocked: runtime component evidence is incomplete" >&2
    exit 1
fi
if ! jq -e \
    --arg harness "$upstream_harness_digest" --arg enospc "$enospc_test_digest" \
    --arg revision "$resolved_commit" \
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
    echo "release blocked: upstream component evidence is incomplete" >&2
    exit 1
fi

echo "redb source=$source_kind revision=$source_ref resolved=$resolved_commit package=$package_version lock=$lock_digest engine=$engine_revision schema=$schema_version source=$source_digest evidence=$evidence_digest locator=$evidence_locator oci=$evidence_oci_digest image=$image_digest is approved for production release"
