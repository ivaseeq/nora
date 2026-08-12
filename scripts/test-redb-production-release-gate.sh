#!/usr/bin/env bash

set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
VERIFY="$ROOT/scripts/verify-redb-production-release.sh"
PUBLISH="$ROOT/scripts/publish-redb-production-evidence.sh"
DIGEST_TOOL="$ROOT/scripts/nora-source-digest.py"
FIXTURE=$(mktemp -d /tmp/nora-redb-release-gate.XXXXXXXX)
FIXTURE_VERIFY="$FIXTURE/scripts/verify-redb-production-release.sh"
FIXTURE_PUBLISH="$FIXTURE/scripts/publish-redb-production-evidence.sh"
METADATA_FIXTURE="$FIXTURE/.git/nora-gate-metadata.json"
OCI_FIXTURE="$FIXTURE/.git/nora-gate-oci"

cleanup() {
    case "$FIXTURE" in
        /tmp/nora-redb-release-gate.*) rm -rf -- "$FIXTURE" ;;
        *) echo "refusing to remove unexpected fixture path" >&2 ;;
    esac
}
trap cleanup EXIT INT TERM

mkdir -p \
    "$FIXTURE/nora-registry/src/repo_index" \
    "$FIXTURE/scripts/redb-production-evidence" \
    "$FIXTURE/bin"
cat >"$FIXTURE/nora-registry/src/repo_index/redb_store.rs" <<'EOF'
const SCHEMA_VERSION: u32 = 6;
const ENGINE_REVISION: &str = "redb-reviewed-git";
EOF
cat >"$FIXTURE/scripts/redb-minio-e2e.sh" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
echo fixture
EOF
for harness in redb-production-matrix.sh run-nora-redb-runtime-regressions.sh \
    run-redb-upstream-regressions.sh; do
    cat >"$FIXTURE/scripts/$harness" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
echo fixture
EOF
    chmod +x "$FIXTURE/scripts/$harness"
done
cat >"$FIXTURE/scripts/redb-enospc-regression.rs" <<'EOF'
#[test]
fn fixture_enospc() {}
EOF
cp -- "$DIGEST_TOOL" "$FIXTURE/scripts/nora-source-digest.py"
cp -- "$VERIFY" "$FIXTURE_VERIFY"
cp -- "$PUBLISH" "$FIXTURE_PUBLISH"
cat >"$FIXTURE/bin/cargo" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
[[ "$*" == "metadata --locked --format-version 1" ]]
cat "$NORA_GATE_METADATA_FIXTURE"
EOF
cat >"$FIXTURE/bin/oras" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
fail() {
    echo "mock oras: $*" >&2
    exit 2
}
if [[ "$1" == push ]]; then
    shift
    export_manifest=
    push_ref=
    while (($#)); do
        case "$1" in
            --export-manifest) export_manifest=$2; shift 2 ;;
            --artifact-type|--annotation|--image-spec) shift 2 ;;
            --no-tty) shift ;;
            docker-hub.just-ai.com/infra/artifact-nora-redb-evidence:redb-*)
                [[ -z "$push_ref" ]] || exit 2
                push_ref=$1
                shift
                ;;
            *) shift ;;
        esac
    done
    [[ -n "$export_manifest" && ${NORA_GATE_PUBLISH_MODE:-0} == 1 \
        && "$push_ref" =~ ^docker-hub\.just-ai\.com/infra/artifact-nora-redb-evidence:redb-[0-9a-f]{40}-[0-9a-f]{64}$ ]] \
        || fail "unexpected content-addressed push reference"
    printf '%s\n' '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.unknown.config.v1+json","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":2},"layers":[]}' \
        >"$export_manifest"
    mkdir -p "$NORA_GATE_OCI_FIXTURE/published"
    cp -- redb-production-evidence.tar redb-production-matrix.json \
        "$NORA_GATE_OCI_FIXTURE/published/"
    cp -- "$export_manifest" "$NORA_GATE_OCI_FIXTURE/pushed-manifest.json"
    printf '%s\n' "$push_ref" >"$NORA_GATE_OCI_FIXTURE/pushed-ref"
elif [[ "$1 $2" == "manifest fetch" && "$3" == "--descriptor" ]]; then
    if [[ ${NORA_GATE_PUBLISH_MODE:-0} == 1 ]]; then
        [[ "$4" == "$(<"$NORA_GATE_OCI_FIXTURE/pushed-ref")" ]] \
            || fail "unexpected descriptor reference"
        digest=$(sha256sum "$NORA_GATE_OCI_FIXTURE/pushed-manifest.json" | awk '{print $1}')
    else
        [[ "$4" == "$NORA_GATE_EXPECTED_RELEASE_REF" ]] \
            || fail "unexpected release descriptor reference"
        digest=$NORA_GATE_OCI_DIGEST
    fi
    printf '{"digest":"sha256:%s"}\n' "$digest"
elif [[ "$1" == pull ]]; then
    shift
    output=
    pull_ref=
    while (($#)); do
        case "$1" in
            --no-tty) shift ;;
            --output) output=$2; shift 2 ;;
            --*) shift ;;
            *) [[ -z "$pull_ref" ]] || exit 2; pull_ref=$1; shift ;;
        esac
    done
    [[ -n "$output" && -n "$pull_ref" ]]
    mkdir -p "$output"
    if [[ ${NORA_GATE_PUBLISH_MODE:-0} == 1 ]]; then
        pushed_ref=$(<"$NORA_GATE_OCI_FIXTURE/pushed-ref")
        pushed_repository=${pushed_ref%:*}
        pushed_digest=$(sha256sum "$NORA_GATE_OCI_FIXTURE/pushed-manifest.json" | awk '{print $1}')
        [[ "$pull_ref" == "$pushed_repository@sha256:$pushed_digest" ]] \
            || fail "unexpected immutable publisher pull reference"
        cp -- "$NORA_GATE_OCI_FIXTURE/published/redb-production-evidence.tar" "$output/"
        cp -- "$NORA_GATE_OCI_FIXTURE/published/redb-production-matrix.json" "$output/"
    else
        [[ "$pull_ref" == "$NORA_GATE_EXPECTED_RELEASE_REF" ]] \
            || fail "unexpected immutable release pull reference"
        cp -- "$NORA_GATE_OCI_FIXTURE/redb-production-evidence.tar" "$output/"
        cp -- "$NORA_GATE_OCI_FIXTURE/redb-production-matrix.json" "$output/"
    fi
    if [[ ${NORA_GATE_TAMPER_READBACK:-0} == 1 ]]; then
        printf 'tampered\n' >>"$output/redb-production-evidence.tar"
    fi
else
    echo "unexpected mock oras invocation: $*" >&2
    exit 2
fi
EOF
chmod +x "$FIXTURE/bin/cargo" "$FIXTURE/bin/oras" "$FIXTURE_VERIFY" "$FIXTURE_PUBLISH"

REVISION=$(printf 'a%.0s' {1..40})
OTHER_REVISION=$(printf 'c%.0s' {1..40})
OTHER_BUNDLE=$(printf 'd%.0s' {1..64})
OCI_DIGEST=$(printf 'e%.0s' {1..64})
OTHER_OCI_DIGEST=$(printf 'c%.0s' {1..64})
IMAGE_DIGEST=$(printf 'f%.0s' {1..64})
IMAGE_TREE=$(printf '1%.0s' {1..40})
RUST_IMAGE=docker-hub.just-ai.com/infra/artifact-nora@sha256:365bc9b835ea399bf25a9259fa83d6960b4ad1d7349864c62404f5599e211841
MINIO_IMAGE=minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e
MC_IMAGE=minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727
REPOSITORY=https://github.com/cberner/redb
LOCK_SOURCE="git+$REPOSITORY?rev=$REVISION#$REVISION"
MINIO_PHASE_FILES=(
    phase-cold_rebuild.ok
    phase-warm_reopen.ok
    phase-incremental.ok
    phase-single_writer.ok
    phase-crash.ok
    phase-corruption.ok
    phase-index_loss.ok
    phase-recovery.ok
    phase-disk_full_admission.ok
)

write_valid_fixture() {
    : >"$FIXTURE/.git/info/exclude"
    cat >"$FIXTURE/nora-registry/src/repo_index/redb_store.rs" <<'EOF'
const SCHEMA_VERSION: u32 = 6;
const ENGINE_REVISION: &str = "redb-reviewed-git";
EOF
    cat >"$FIXTURE/nora-registry/Cargo.toml" <<EOF
[package]
name = "nora-registry"
version = "1.1.0"

[dependencies]
redb = { git = "$REPOSITORY", rev = "$REVISION", features = ["logging"] }
EOF
    cat >"$FIXTURE/Cargo.lock" <<EOF
version = 4

[[package]]
name = "redb"
version = "4.1.0"
source = "$LOCK_SOURCE"
EOF
    python3 - "$METADATA_FIXTURE" "$FIXTURE" "$REVISION" "$REPOSITORY" <<'PY'
import json
import os
import sys

metadata_path = sys.argv[1]
root = os.path.realpath(sys.argv[2])
revision = sys.argv[3]
repository = sys.argv[4]
workspace_id = f"path+file://{root}/nora-registry#1.1.0"
source = f"git+{repository}?rev={revision}#{revision}"
redb_id = f"git+{repository}?rev={revision}#4.1.0"
metadata = {
    "packages": [
        {
            "id": workspace_id,
            "name": "nora-registry",
            "version": "1.1.0",
            "source": None,
            "manifest_path": f"{root}/nora-registry/Cargo.toml",
        },
        {
            "id": redb_id,
            "name": "redb",
            "version": "4.1.0",
            "source": source,
            "manifest_path": "/cargo/git/redb/Cargo.toml",
        },
    ],
    "resolve": {
        "nodes": [
            {"id": workspace_id, "deps": [{"name": "redb", "pkg": redb_id}]},
            {"id": redb_id, "deps": []},
        ]
    },
}
with open(metadata_path, "w", encoding="utf-8") as handle:
    json.dump(metadata, handle)
PY
    local lock_digest source_digest harness_digest matrix_harness_digest
    local runtime_harness_digest upstream_harness_digest enospc_test_digest
    local matrix_digest bundle_digest log_digest regression_lock_digest minio_artifacts
    local minio_manifest_digest minio_bundle_digest runtime_manifest_digest upstream_manifest_digest
    lock_digest=$(sha256sum "$FIXTURE/Cargo.lock" | awk '{print $1}')
    git -C "$FIXTURE" add -A
    source_digest=$(python3 "$DIGEST_TOOL" "$FIXTURE")
    harness_digest=$(sha256sum "$FIXTURE/scripts/redb-minio-e2e.sh" | awk '{print $1}')
    matrix_harness_digest=$(sha256sum "$FIXTURE/scripts/redb-production-matrix.sh" | awk '{print $1}')
    runtime_harness_digest=$(sha256sum "$FIXTURE/scripts/run-nora-redb-runtime-regressions.sh" | awk '{print $1}')
    upstream_harness_digest=$(sha256sum "$FIXTURE/scripts/run-redb-upstream-regressions.sh" | awk '{print $1}')
    enospc_test_digest=$(sha256sum "$FIXTURE/scripts/redb-enospc-regression.rs" | awk '{print $1}')
    rm -rf -- "$OCI_FIXTURE"
    mkdir -p "$OCI_FIXTURE/payload/components" "$OCI_FIXTURE/payload/runtime" \
        "$OCI_FIXTURE/payload/upstream" "$OCI_FIXTURE/minio-inner"
    printf '%s\n' fixture-log >"$OCI_FIXTURE/payload/runtime/timeout_reap.log"
    for phase in second_open generic_io_policy abnormal_shutdown shutdown_fence; do
        cp -- "$OCI_FIXTURE/payload/runtime/timeout_reap.log" \
            "$OCI_FIXTURE/payload/runtime/$phase.log"
    done
    printf '%s\n' fixture-log >"$OCI_FIXTURE/payload/upstream/growing_commit_crash.log"
    for phase in double_crash_torn_slot torn_region_counts commit_error_poison \
        generic_io_recovery enospc_recovery; do
        cp -- "$OCI_FIXTURE/payload/upstream/growing_commit_crash.log" \
            "$OCI_FIXTURE/payload/upstream/$phase.log"
    done
    log_digest=$(sha256sum "$OCI_FIXTURE/payload/runtime/timeout_reap.log" | awk '{print $1}')
    printf '%s\n' fixture-regression-lock \
        >"$OCI_FIXTURE/payload/upstream/redb-regression-Cargo.lock"
    regression_lock_digest=$(sha256sum \
        "$OCI_FIXTURE/payload/upstream/redb-regression-Cargo.lock" | awk '{print $1}')
    for phase in cold_rebuild warm_reopen incremental single_writer crash \
        corruption index_loss recovery disk_full_admission; do
        printf 'pass\n' >"$OCI_FIXTURE/minio-inner/phase-$phase.ok"
    done
    minio_artifacts=$(python3 - "$OCI_FIXTURE/minio-inner" <<'PY'
import hashlib
import json
import pathlib
import sys
root = pathlib.Path(sys.argv[1])
print(json.dumps({
    path.name: hashlib.sha256(path.read_bytes()).hexdigest()
    for path in sorted(root.glob("phase-*.ok"))
}, sort_keys=True, separators=(",", ":")))
PY
)
    tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
        -C "$OCI_FIXTURE/minio-inner" \
        -cf "$OCI_FIXTURE/payload/components/minio-evidence.tar" -- \
        "${MINIO_PHASE_FILES[@]}"
    minio_bundle_digest=$(sha256sum \
        "$OCI_FIXTURE/payload/components/minio-evidence.tar" | awk '{print $1}')
    cat >"$OCI_FIXTURE/payload/components/minio-manifest.json" <<EOF
{"schema":1,"source_tree":"$IMAGE_TREE","cargo_lock_sha256":"$lock_digest","image":"docker-hub.just-ai.com/infra/artifact-nora@sha256:$IMAGE_DIGEST","engine_revision":"redb-reviewed-git","nora_schema_version":6,"minio_image":"$MINIO_IMAGE","mc_image":"$MC_IMAGE","bundle_sha256":"$minio_bundle_digest","artifacts":$minio_artifacts,"phase_artifacts":{"cold_rebuild":"phase-cold_rebuild.ok","warm_reopen":"phase-warm_reopen.ok","incremental":"phase-incremental.ok","single_writer":"phase-single_writer.ok","crash":"phase-crash.ok","corruption":"phase-corruption.ok","index_loss":"phase-index_loss.ok","recovery":"phase-recovery.ok","disk_full_admission":"phase-disk_full_admission.ok"},"verified_phases":["cold_rebuild","warm_reopen","incremental","single_writer","crash","corruption","index_loss","recovery","disk_full_admission"],"production_matrix_component_complete":true}
EOF
    cat >"$OCI_FIXTURE/payload/runtime/nora-redb-runtime-regressions.json" <<EOF
{"schema":1,"harness_sha256":"$runtime_harness_digest","phases":{"timeout_reap":"pass","second_open":"pass","generic_io_policy":"pass","abnormal_shutdown":"pass","shutdown_fence":"pass"},"logs":{"timeout_reap":"$log_digest","second_open":"$log_digest","generic_io_policy":"$log_digest","abnormal_shutdown":"$log_digest","shutdown_fence":"$log_digest"}}
EOF
    cat >"$OCI_FIXTURE/payload/upstream/redb-upstream-regressions.json" <<EOF
{"schema":1,"redb_revision":"$REVISION","redb_version":"4.1.0","redb_source":"$LOCK_SOURCE","generated_lock_sha256":"$regression_lock_digest","harness_sha256":"$upstream_harness_digest","enospc_test_sha256":"$enospc_test_digest","phases":{"growing_commit_crash":"pass","double_crash_torn_slot":"pass","torn_region_counts":"pass","commit_error_poison":"pass","generic_io_recovery":"pass","enospc_recovery":"pass"},"logs":{"growing_commit_crash":"$log_digest","double_crash_torn_slot":"$log_digest","torn_region_counts":"$log_digest","commit_error_poison":"$log_digest","generic_io_recovery":"$log_digest","enospc_recovery":"$log_digest"}}
EOF
    minio_manifest_digest=$(sha256sum \
        "$OCI_FIXTURE/payload/components/minio-manifest.json" | awk '{print $1}')
    runtime_manifest_digest=$(sha256sum \
        "$OCI_FIXTURE/payload/runtime/nora-redb-runtime-regressions.json" | awk '{print $1}')
    upstream_manifest_digest=$(sha256sum \
        "$OCI_FIXTURE/payload/upstream/redb-upstream-regressions.json" | awk '{print $1}')
    cat >"$OCI_FIXTURE/payload/redb-production-matrix.json" <<EOF
{"schema":1,"production_matrix_complete":true,"source_tree":"$IMAGE_TREE","source_digest":"$source_digest","cargo_lock_sha256":"$lock_digest","image_ref":"docker-hub.just-ai.com/infra/artifact-nora@sha256:$IMAGE_DIGEST","image_digest":"sha256:$IMAGE_DIGEST","engine_revision":"redb-reviewed-git","nora_schema_version":6,"execution":{"source_mode":"verified_git_tree_snapshot","source_tree":"$IMAGE_TREE","runner_kind":"docker"},"redb":{"repository":"$REPOSITORY","revision":"$REVISION","package_version":"4.1.0","lock_source":"$LOCK_SOURCE","regression_lock_sha256":"$regression_lock_digest"},"harnesses":{"matrix_sha256":"$matrix_harness_digest","minio_sha256":"$harness_digest","runtime_sha256":"$runtime_harness_digest","upstream_sha256":"$upstream_harness_digest","enospc_test_sha256":"$enospc_test_digest"},"components":{"minio":{"manifest_path":"components/minio-manifest.json","bundle_path":"components/minio-evidence.tar","manifest_sha256":"$minio_manifest_digest","bundle_sha256":"$minio_bundle_digest","minio_image":"$MINIO_IMAGE","mc_image":"$MC_IMAGE"},"runtime":{"manifest_path":"runtime/nora-redb-runtime-regressions.json","manifest_sha256":"$runtime_manifest_digest","runner_kind":"docker","runner_image":"$RUST_IMAGE"},"upstream":{"manifest_path":"upstream/redb-upstream-regressions.json","manifest_sha256":"$upstream_manifest_digest","runner_kind":"docker","runner_image":"$RUST_IMAGE"}},"verified_phases":["sigkill","enospc","double_crash","torn_write","corruption","generic_io","timeout","second_open","recovery"]}
EOF
    build_remote_bundle
    cp -- "$OCI_FIXTURE/payload/redb-production-matrix.json" \
        "$OCI_FIXTURE/redb-production-matrix.json"
    matrix_digest=$(sha256sum "$OCI_FIXTURE/redb-production-matrix.json" | awk '{print $1}')
    bundle_digest=$(sha256sum "$OCI_FIXTURE/redb-production-evidence.tar" | awk '{print $1}')
    rm -f -- "$FIXTURE/scripts/redb-production-evidence/$REVISION.json"
    cat >"$FIXTURE/scripts/redb-production-evidence/$REVISION.json" <<EOF
{"schema":3,"redb_source_kind":"git","redb_repository":"$REPOSITORY","redb_revision":"$REVISION","redb_resolved_commit":"$REVISION","redb_package_version":"4.1.0","redb_lock_source":"$LOCK_SOURCE","cargo_lock_sha256":"$lock_digest","engine_revision":"redb-reviewed-git","nora_schema_version":6,"nora_source_digest":"$source_digest","image_ref":"docker-hub.just-ai.com/infra/artifact-nora@sha256:$IMAGE_DIGEST","image_digest":"sha256:$IMAGE_DIGEST","image_source_tree":"$IMAGE_TREE","harnesses":{"matrix_sha256":"$matrix_harness_digest","minio_sha256":"$harness_digest","runtime_sha256":"$runtime_harness_digest","upstream_sha256":"$upstream_harness_digest","enospc_test_sha256":"$enospc_test_digest"},"bundle_uri":"oci://docker-hub.just-ai.com/infra/artifact-nora-redb-evidence@sha256:$OCI_DIGEST","evidence_oci_manifest_digest":"sha256:$OCI_DIGEST","bundle_sha256":"$bundle_digest","matrix_manifest_sha256":"$matrix_digest","verified_phases":["sigkill","enospc","double_crash","torn_write","corruption","generic_io","timeout","second_open","recovery"]}
EOF
    local evidence
    evidence=$(sha256sum "$FIXTURE/scripts/redb-production-evidence/$REVISION.json" | awk '{print $1}')
    printf 'git %s %s %s %s %s %s\n' \
        "$REVISION" "$REVISION" redb-reviewed-git 6 "$evidence" \
        "scripts/redb-production-evidence/$REVISION.json" \
        >"$FIXTURE/scripts/redb-production-allowlist.txt"
    git -C "$FIXTURE" add -A
}

REMOTE_BUNDLE_MEMBERS=(
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

build_remote_bundle() {
    tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
        -C "$OCI_FIXTURE/payload" -cf "$OCI_FIXTURE/redb-production-evidence.tar" \
        -- "${REMOTE_BUNDLE_MEMBERS[@]}"
}

refresh_minio_fixture() {
    local artifacts bundle temporary
    tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
        -C "$OCI_FIXTURE/minio-inner" \
        -cf "$OCI_FIXTURE/payload/components/minio-evidence.tar" -- \
        "${MINIO_PHASE_FILES[@]}"
    artifacts=$(python3 - "$OCI_FIXTURE/minio-inner" <<'PY'
import hashlib
import json
import pathlib
import sys
root = pathlib.Path(sys.argv[1])
print(json.dumps({
    path.name: hashlib.sha256(path.read_bytes()).hexdigest()
    for path in sorted(root.glob("phase-*.ok"))
}, sort_keys=True, separators=(",", ":")))
PY
)
    bundle=$(sha256sum "$OCI_FIXTURE/payload/components/minio-evidence.tar" \
        | awk '{print $1}')
    temporary="$FIXTURE/.git/minio-refreshed.json"
    jq --argjson artifacts "$artifacts" --arg bundle "$bundle" \
        '.artifacts = $artifacts | .bundle_sha256 = $bundle' \
        "$OCI_FIXTURE/payload/components/minio-manifest.json" >"$temporary"
    mv -- "$temporary" "$OCI_FIXTURE/payload/components/minio-manifest.json"
}

refresh_component_hashes() {
    local temporary="$FIXTURE/.git/matrix-components-updated.json"
    jq \
        --arg minio_manifest "$(sha256sum "$OCI_FIXTURE/payload/components/minio-manifest.json" | awk '{print $1}')" \
        --arg minio_bundle "$(sha256sum "$OCI_FIXTURE/payload/components/minio-evidence.tar" | awk '{print $1}')" \
        --arg runtime_manifest "$(sha256sum "$OCI_FIXTURE/payload/runtime/nora-redb-runtime-regressions.json" | awk '{print $1}')" \
        --arg upstream_manifest "$(sha256sum "$OCI_FIXTURE/payload/upstream/redb-upstream-regressions.json" | awk '{print $1}')" \
        '.components.minio.manifest_sha256 = $minio_manifest
         | .components.minio.bundle_sha256 = $minio_bundle
         | .components.runtime.manifest_sha256 = $runtime_manifest
         | .components.upstream.manifest_sha256 = $upstream_manifest' \
        "$OCI_FIXTURE/payload/redb-production-matrix.json" >"$temporary"
    mv -- "$temporary" "$OCI_FIXTURE/payload/redb-production-matrix.json"
}

run_gate() {
    PATH="$FIXTURE/bin:$PATH" \
        NORA_GATE_METADATA_FIXTURE="$METADATA_FIXTURE" \
        NORA_GATE_OCI_FIXTURE="$OCI_FIXTURE" \
        NORA_GATE_OCI_DIGEST="$OCI_DIGEST" \
        NORA_GATE_EXPECTED_RELEASE_REF="docker-hub.just-ai.com/infra/artifact-nora-redb-evidence@sha256:$OCI_DIGEST" \
        "$FIXTURE_VERIFY"
}

prepare_publisher_fixture() {
    write_valid_fixture
    local publisher_tree temporary
    publisher_tree=$(git -C "$FIXTURE" write-tree)
    temporary="$FIXTURE/.git/minio-publisher.json"
    jq --arg tree "$publisher_tree" '.source_tree = $tree' \
        "$OCI_FIXTURE/payload/components/minio-manifest.json" >"$temporary"
    mv -- "$temporary" "$OCI_FIXTURE/payload/components/minio-manifest.json"
    temporary="$FIXTURE/.git/matrix-publisher.json"
    jq --arg tree "$publisher_tree" \
        '.source_tree = $tree | .execution.source_tree = $tree' \
        "$OCI_FIXTURE/payload/redb-production-matrix.json" >"$temporary"
    mv -- "$temporary" "$OCI_FIXTURE/payload/redb-production-matrix.json"
    refresh_component_hashes
    build_remote_bundle
    cp -- "$OCI_FIXTURE/payload/redb-production-matrix.json" \
        "$OCI_FIXTURE/redb-production-matrix.json"
}

run_publisher() {
    local output=$1
    PATH="$FIXTURE/bin:$PATH" \
        NORA_GATE_PUBLISH_MODE=1 \
        NORA_GATE_OCI_FIXTURE="$OCI_FIXTURE" \
        NORA_GATE_OCI_DIGEST="$OCI_DIGEST" \
        "$FIXTURE_PUBLISH" "$OCI_FIXTURE" \
        docker-hub.just-ai.com/infra/artifact-nora-redb-evidence "$output"
}

expect_publisher_rejected() {
    local description=$1 expected=$2 output=$3 actual
    if actual=$(run_publisher "$output" 2>&1); then
        echo "publisher self-test unexpectedly accepted: $description" >&2
        exit 1
    fi
    if [[ "$actual" != *"$expected"* ]]; then
        echo "publisher self-test rejected for the wrong reason: $description" >&2
        exit 1
    fi
}

refresh_evidence_approval() {
    local evidence
    evidence=$(sha256sum "$FIXTURE/scripts/redb-production-evidence/$REVISION.json" | awk '{print $1}')
    printf 'git %s %s %s %s %s %s\n' \
        "$REVISION" "$REVISION" redb-reviewed-git 6 "$evidence" \
        "scripts/redb-production-evidence/$REVISION.json" \
        >"$FIXTURE/scripts/redb-production-allowlist.txt"
    git -C "$FIXTURE" add -A
}

refresh_outer_approval() {
    cp -- "$OCI_FIXTURE/payload/redb-production-matrix.json" \
        "$OCI_FIXTURE/redb-production-matrix.json"
    local matrix_digest bundle_digest temporary
    matrix_digest=$(sha256sum "$OCI_FIXTURE/redb-production-matrix.json" | awk '{print $1}')
    bundle_digest=$(sha256sum "$OCI_FIXTURE/redb-production-evidence.tar" | awk '{print $1}')
    temporary="$FIXTURE/.git/approval-updated.json"
    jq --arg matrix "$matrix_digest" --arg bundle "$bundle_digest" \
        '.matrix_manifest_sha256 = $matrix | .bundle_sha256 = $bundle' \
        "$FIXTURE/scripts/redb-production-evidence/$REVISION.json" >"$temporary"
    mv -- "$temporary" "$FIXTURE/scripts/redb-production-evidence/$REVISION.json"
    refresh_evidence_approval
}

refresh_remote_payload_approval() {
    build_remote_bundle
    refresh_outer_approval
}

expect_rejected() {
    local description=$1
    local expected=${2:-} output
    if output=$(run_gate 2>&1); then
        echo "release-gate self-test unexpectedly accepted: $description" >&2
        exit 1
    fi
    if [[ -n "$expected" && "$output" != *"$expected"* ]]; then
        echo "release-gate self-test rejected for the wrong reason: $description" >&2
        exit 1
    fi
}

git -C "$FIXTURE" init -q
write_valid_fixture
run_gate >/dev/null

if ! NORA_RELEASE_GATE_ROOT=/tmp/nora-redb-attacker-root run_gate >/dev/null; then
    echo "release-gate self-test allowed an environment root override" >&2
    exit 1
fi

write_valid_fixture
baseline_source=$(python3 "$DIGEST_TOOL" "$FIXTURE")
printf '\n' >>"$FIXTURE/scripts/redb-production-evidence/$REVISION.json"
refresh_evidence_approval
if [[ $(python3 "$DIGEST_TOOL" "$FIXTURE") != "$baseline_source" ]]; then
    echo "release-gate self-test changed source identity for approval metadata" >&2
    exit 1
fi
run_gate >/dev/null

write_valid_fixture
printf '\n// load-bearing mutation\n' >>"$FIXTURE/nora-registry/src/repo_index/redb_store.rs"
git -C "$FIXTURE" add nora-registry/src/repo_index/redb_store.rs
expect_rejected "load-bearing source mutation" "identity does not match the release"

write_valid_fixture
perl -0pi -e 's/rev = "a{40}"/branch = "master"/' "$FIXTURE/nora-registry/Cargo.toml"
git -C "$FIXTURE" add nora-registry/Cargo.toml
expect_rejected "branch instead of full revision" "must not set branch"

write_valid_fixture
perl -0pi -e 's#https://github.com/cberner/redb#https://github.com/example/redb#' "$FIXTURE/nora-registry/Cargo.toml"
git -C "$FIXTURE" add nora-registry/Cargo.toml
expect_rejected "substituted git repository" "not the reviewed upstream"

write_valid_fixture
python3 - "$METADATA_FIXTURE" "$REVISION" "$OTHER_REVISION" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as handle:
    value = json.load(handle)
value["packages"][1]["source"] = value["packages"][1]["source"].replace(sys.argv[2], sys.argv[3], 1)
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    json.dump(value, handle)
PY
expect_rejected "resolved metadata commit substitution" "resolved redb source does not match"

write_valid_fixture
perl -0pi -e "s/$REVISION/$OTHER_REVISION/g" "$FIXTURE/Cargo.lock"
git -C "$FIXTURE" add Cargo.lock
expect_rejected "Cargo.lock source substitution" "Cargo.lock lacks one exact reviewed"

write_valid_fixture
perl -0pi -e "s/git $REVISION $REVISION/git $REVISION $OTHER_REVISION/" \
    "$FIXTURE/scripts/redb-production-allowlist.txt"
git -C "$FIXTURE" add scripts/redb-production-allowlist.txt
expect_rejected "allowlist resolved commit substitution" "approval is not bound"

write_valid_fixture
approval_row=$(<"$FIXTURE/scripts/redb-production-allowlist.txt")
printf '%s\n' "$approval_row" >>"$FIXTURE/scripts/redb-production-allowlist.txt"
git -C "$FIXTURE" add scripts/redb-production-allowlist.txt
expect_rejected "duplicate allowlist approval" "exactly one reviewed"

write_valid_fixture
printf '\n' >>"$FIXTURE/scripts/redb-production-evidence/$REVISION.json"
git -C "$FIXTURE" add "scripts/redb-production-evidence/$REVISION.json"
expect_rejected "modified evidence manifest" "manifest digest does not match"

write_valid_fixture
printf '%s\n' "scripts/redb-production-evidence/$REVISION.json" \
    >>"$FIXTURE/.git/info/exclude"
git -C "$FIXTURE" update-index --force-remove \
    "scripts/redb-production-evidence/$REVISION.json"
expect_rejected "ignored untracked evidence manifest" "not one tracked regular stage-0 blob"

write_valid_fixture
run_gate >/dev/null

write_valid_fixture
cp -- "$FIXTURE/scripts/redb-production-evidence/$REVISION.json" \
    "$FIXTURE/.git/evidence-target.json"
rm -f -- "$FIXTURE/scripts/redb-production-evidence/$REVISION.json"
ln -s ../../.git/evidence-target.json \
    "$FIXTURE/scripts/redb-production-evidence/$REVISION.json"
refresh_evidence_approval
expect_rejected "symlink evidence manifest" "not one tracked regular stage-0 blob"

write_valid_fixture
run_gate >/dev/null

write_valid_fixture
perl -0pi -e "s/sha256:$OCI_DIGEST/sha256:$OTHER_OCI_DIGEST/" \
    "$FIXTURE/scripts/redb-production-evidence/$REVISION.json"
refresh_evidence_approval
expect_rejected "evidence OCI URI digest substitution" "OCI digest differs from its immutable URI"

write_valid_fixture
printf 'tampered' >>"$OCI_FIXTURE/redb-production-evidence.tar"
expect_rejected "remote evidence bundle byte substitution" "payload digest does not match approval"

write_valid_fixture
perl -0pi -e "s/sha256:$IMAGE_DIGEST/sha256:$OTHER_BUNDLE/" \
    "$OCI_FIXTURE/payload/redb-production-matrix.json"
refresh_remote_payload_approval
expect_rejected "remote matrix image substitution" "production matrix does not match"

write_valid_fixture
jq --arg digest "$OTHER_BUNDLE" '.harnesses.runtime_sha256 = $digest' \
    "$OCI_FIXTURE/payload/redb-production-matrix.json" \
    >"$FIXTURE/.git/matrix-updated.json"
mv -- "$FIXTURE/.git/matrix-updated.json" \
    "$OCI_FIXTURE/payload/redb-production-matrix.json"
refresh_remote_payload_approval
expect_rejected "remote matrix harness substitution" "production matrix does not match"

write_valid_fixture
jq '.verified_phases -= ["double_crash"]' \
    "$OCI_FIXTURE/payload/redb-production-matrix.json" \
    >"$FIXTURE/.git/matrix-updated.json"
mv -- "$FIXTURE/.git/matrix-updated.json" \
    "$OCI_FIXTURE/payload/redb-production-matrix.json"
refresh_remote_payload_approval
expect_rejected "remote matrix missing required phase" "production matrix does not match"

write_valid_fixture
jq '.verified_phases += ["never_executed"]' \
    "$OCI_FIXTURE/payload/redb-production-matrix.json" \
    >"$FIXTURE/.git/matrix-updated.json"
mv -- "$FIXTURE/.git/matrix-updated.json" \
    "$OCI_FIXTURE/payload/redb-production-matrix.json"
refresh_remote_payload_approval
expect_rejected "remote matrix contains an unexecuted phase" \
    "production matrix does not match"

write_valid_fixture
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
    -C "$OCI_FIXTURE/payload" -cf "$OCI_FIXTURE/redb-production-evidence.tar" \
    redb-production-matrix.json
refresh_outer_approval
expect_rejected "matrix-only remote evidence bundle" \
    "evidence tar lacks a canonical member"

write_valid_fixture
printf '%s\n' tampered-component-log \
    >>"$OCI_FIXTURE/payload/runtime/timeout_reap.log"
refresh_remote_payload_approval
expect_rejected "tampered runtime component log" \
    "runtime component evidence is incomplete"

write_valid_fixture
rm -f -- "$OCI_FIXTURE/payload/upstream/redb-regression-Cargo.lock"
missing_lock_members=()
for member in "${REMOTE_BUNDLE_MEMBERS[@]}"; do
    [[ "$member" == upstream/redb-regression-Cargo.lock ]] \
        || missing_lock_members+=("$member")
done
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
    -C "$OCI_FIXTURE/payload" -cf "$OCI_FIXTURE/redb-production-evidence.tar" \
    -- "${missing_lock_members[@]}"
refresh_outer_approval
expect_rejected "missing upstream component lock" \
    "evidence tar lacks a canonical member"

write_valid_fixture
jq '.verified_phases -= ["corruption"]' \
    "$OCI_FIXTURE/payload/components/minio-manifest.json" \
    >"$FIXTURE/.git/minio-manifest-updated.json"
mv -- "$FIXTURE/.git/minio-manifest-updated.json" \
    "$OCI_FIXTURE/payload/components/minio-manifest.json"
refresh_component_hashes
refresh_remote_payload_approval
expect_rejected "MinIO component missing required phase" \
    "MinIO component identity/phases differ"

write_valid_fixture
printf 'fail\n' >"$OCI_FIXTURE/minio-inner/phase-corruption.ok"
refresh_minio_fixture
refresh_component_hashes
refresh_remote_payload_approval
expect_rejected "MinIO component has a non-pass phase marker" \
    "MinIO component identity/phases differ"

write_valid_fixture
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
    -C "$OCI_FIXTURE/minio-inner" \
    -cf "$OCI_FIXTURE/payload/components/minio-evidence.tar" --files-from /dev/null
minio_bundle_digest=$(sha256sum \
    "$OCI_FIXTURE/payload/components/minio-evidence.tar" | awk '{print $1}')
jq --arg bundle "$minio_bundle_digest" '.bundle_sha256 = $bundle' \
    "$OCI_FIXTURE/payload/components/minio-manifest.json" \
    >"$FIXTURE/.git/minio-manifest-updated.json"
mv -- "$FIXTURE/.git/minio-manifest-updated.json" \
    "$OCI_FIXTURE/payload/components/minio-manifest.json"
refresh_component_hashes
refresh_remote_payload_approval
expect_rejected "incomplete inner MinIO evidence bundle" \
    "MinIO tar lacks a canonical artifact"

write_valid_fixture
printf '%s\n' should-not-be-published >"$OCI_FIXTURE/payload/unexpected.txt"
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
    -C "$OCI_FIXTURE/payload" -cf "$OCI_FIXTURE/redb-production-evidence.tar" \
    -- "${REMOTE_BUNDLE_MEMBERS[@]}" unexpected.txt
refresh_outer_approval
expect_rejected "unexpected outer evidence member" \
    "evidence tar is not the canonical member set"

write_valid_fixture
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
    -C "$OCI_FIXTURE/payload" -cf "$OCI_FIXTURE/redb-production-evidence.tar" \
    -- "${REMOTE_BUNDLE_MEMBERS[@]}" runtime/timeout_reap.log
refresh_outer_approval
expect_rejected "duplicate outer evidence member" \
    "evidence tar is not the canonical member set"

prepare_publisher_fixture
publisher_output="$FIXTURE/.git/publisher-positive"
run_publisher "$publisher_output" >/dev/null
publisher_tree=$(git -C "$FIXTURE" write-tree)
publisher_bundle_sha=$(sha256sum "$OCI_FIXTURE/redb-production-evidence.tar" | awk '{print $1}')
jq -e --arg tag \
    "docker-hub.just-ai.com/infra/artifact-nora-redb-evidence:redb-$publisher_tree-$publisher_bundle_sha" \
    '.read_back_verified == true and .tag == $tag
     and (.immutable_ref | test("@sha256:[0-9a-f]{64}$"))' \
    "$publisher_output/redb-production-oci-receipt.json" >/dev/null

prepare_publisher_fixture
snapshot_ready="$FIXTURE/.git/publisher-snapshot-ready"
snapshot_continue="$FIXTURE/.git/publisher-snapshot-continue"
snapshot_log="$FIXTURE/.git/publisher-snapshot.log"
snapshot_output="$FIXTURE/.git/publisher-snapshot-output"
rm -f -- "$snapshot_ready" "$snapshot_continue"
NORA_GATE_PUBLISH_SNAPSHOT_READY_FILE="$snapshot_ready" \
NORA_GATE_PUBLISH_SNAPSHOT_CONTINUE_FILE="$snapshot_continue" \
    run_publisher "$snapshot_output" >"$snapshot_log" 2>&1 &
snapshot_pid=$!
for _ in {1..200}; do
    [[ -e "$snapshot_ready" ]] && break
    sleep 0.05
done
[[ -e "$snapshot_ready" ]] || {
    echo "publisher input-snapshot self-test did not reach its barrier" >&2
    kill "$snapshot_pid" 2>/dev/null || true
    wait "$snapshot_pid" 2>/dev/null || true
    exit 1
}
printf 'post-snapshot-matrix-mutation\n' >>"$OCI_FIXTURE/redb-production-matrix.json"
printf 'post-snapshot-bundle-mutation\n' >>"$OCI_FIXTURE/redb-production-evidence.tar"
: >"$snapshot_continue"
if ! wait "$snapshot_pid"; then
    cat "$snapshot_log" >&2
    echo "publisher did not isolate caller-owned inputs before validation" >&2
    exit 1
fi
jq -e '.read_back_verified == true' \
    "$snapshot_output/redb-production-oci-receipt.json" >/dev/null

prepare_publisher_fixture
jq '.verified_phases += ["never_executed"]' \
    "$OCI_FIXTURE/payload/redb-production-matrix.json" \
    >"$FIXTURE/.git/publisher-matrix-updated.json"
mv -- "$FIXTURE/.git/publisher-matrix-updated.json" \
    "$OCI_FIXTURE/payload/redb-production-matrix.json"
build_remote_bundle
cp -- "$OCI_FIXTURE/payload/redb-production-matrix.json" \
    "$OCI_FIXTURE/redb-production-matrix.json"
expect_publisher_rejected "publisher unexecuted phase" \
    "matrix manifest is incomplete" "$FIXTURE/.git/publisher-extra-phase"

prepare_publisher_fixture
printf 'fail\n' >"$OCI_FIXTURE/minio-inner/phase-corruption.ok"
refresh_minio_fixture
refresh_component_hashes
build_remote_bundle
cp -- "$OCI_FIXTURE/payload/redb-production-matrix.json" \
    "$OCI_FIXTURE/redb-production-matrix.json"
expect_publisher_rejected "publisher non-pass MinIO marker" \
    "MinIO component identity/phases differ" "$FIXTURE/.git/publisher-fail-marker"

prepare_publisher_fixture
run_publisher "$FIXTURE/.git/publisher-reference-baseline" >/dev/null
if PATH="$FIXTURE/bin:$PATH" NORA_GATE_PUBLISH_MODE=1 \
    NORA_GATE_OCI_FIXTURE="$OCI_FIXTURE" \
    oras manifest fetch --descriptor \
        docker-hub.just-ai.com/infra/artifact-nora-redb-evidence:mutable \
        >/dev/null 2>&1; then
    echo "mock ORAS accepted a mutable descriptor reference" >&2
    exit 1
fi
if PATH="$FIXTURE/bin:$PATH" NORA_GATE_PUBLISH_MODE=1 \
    NORA_GATE_OCI_FIXTURE="$OCI_FIXTURE" \
    oras pull --no-tty --output "$FIXTURE/.git/wrong-pull" \
        docker-hub.just-ai.com/infra/artifact-nora-redb-evidence:mutable \
        >/dev/null 2>&1; then
    echo "mock ORAS accepted a mutable pull reference" >&2
    exit 1
fi

prepare_publisher_fixture
NORA_GATE_TAMPER_READBACK=1 \
    expect_publisher_rejected "publisher tampered immutable readback" \
        "read-back evidence bundle bytes differ" \
        "$FIXTURE/.git/publisher-tampered-readback"

prepare_publisher_fixture
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
    -C "$OCI_FIXTURE/payload" -cf "$OCI_FIXTURE/redb-production-evidence.tar" \
    redb-production-matrix.json
expect_publisher_rejected "publisher matrix-only bundle" \
    "evidence tar lacks a canonical member" "$FIXTURE/.git/publisher-matrix-only"

prepare_publisher_fixture
printf '%s\n' publisher-tampered-log \
    >>"$OCI_FIXTURE/payload/runtime/timeout_reap.log"
build_remote_bundle
expect_publisher_rejected "publisher tampered runtime log" \
    "runtime component evidence is incomplete" "$FIXTURE/.git/publisher-tampered-log"

matrix_positive_root=$(mktemp -d /tmp/nora-redb-production-matrix.XXXXXXXX)
mkdir -p "$matrix_positive_root/source" "$matrix_positive_root/fake-bin" \
    "$FIXTURE/.git/matrix-positive-evidence"
matrix_positive_tree=$(git -C "$ROOT" write-tree)
git -C "$ROOT" archive --format=tar "$matrix_positive_tree" \
    | tar -xf - -C "$matrix_positive_root/source"
cat >"$matrix_positive_root/fake-bin/docker" <<'EOF'
#!/usr/bin/env bash
echo "post-digest-fake-docker-boundary" >&2
exit 93
EOF
chmod +x "$matrix_positive_root/fake-bin/docker"
if PATH="$matrix_positive_root/fake-bin:$PATH" \
    NORA_REDB_MATRIX_SNAPSHOT_ROOT="$matrix_positive_root/source" \
    NORA_REDB_MATRIX_RUN_ROOT="$matrix_positive_root" \
    NORA_REDB_MATRIX_REQUIRE_REMOTE_DIGEST=0 \
    "$matrix_positive_root/source/scripts/redb-production-matrix.sh" \
        "docker-hub.just-ai.com/infra/artifact-nora@sha256:$IMAGE_DIGEST" \
        "$matrix_positive_tree" "$FIXTURE/.git/matrix-positive-evidence" \
        >"$FIXTURE/.git/matrix-positive.log" 2>&1; then
    echo "production matrix positive snapshot test passed the fake Docker boundary" >&2
    exit 1
fi
grep -q 'post-digest-fake-docker-boundary' \
    "$FIXTURE/.git/matrix-positive.log" || {
    echo "production matrix exact snapshot did not reach the post-digest boundary" >&2
    exit 1
}
[[ ! -e "$matrix_positive_root" ]] || {
    echo "production matrix exact snapshot did not clean its private run root" >&2
    find "$matrix_positive_root" -depth -delete
    exit 1
}

matrix_bypass_root=$(mktemp -d /tmp/nora-redb-production-matrix.XXXXXXXX)
mkdir -p "$matrix_bypass_root/source/scripts" "$FIXTURE/.git/matrix-bypass-evidence"
cp -- "$ROOT/scripts/redb-production-matrix.sh" \
    "$matrix_bypass_root/source/scripts/redb-production-matrix.sh"
if NORA_REDB_MATRIX_SNAPSHOT_ROOT="$matrix_bypass_root/source" \
    NORA_REDB_MATRIX_RUN_ROOT="$matrix_bypass_root" \
    NORA_REDB_MATRIX_SOURCE_DIGEST="$(printf 'b%.0s' {1..64})" \
    NORA_REDB_MATRIX_REQUIRE_REMOTE_DIGEST=0 \
    "$matrix_bypass_root/source/scripts/redb-production-matrix.sh" \
        "docker-hub.just-ai.com/infra/artifact-nora@sha256:$IMAGE_DIGEST" \
        "$IMAGE_TREE" "$FIXTURE/.git/matrix-bypass-evidence" \
        >"$FIXTURE/.git/matrix-bypass.log" 2>&1; then
    echo "production matrix accepted an externally supplied modified snapshot" >&2
    rm -rf -- "$matrix_bypass_root"
    exit 1
fi
grep -q 'snapshot bytes do not match the reviewed tree' \
    "$FIXTURE/.git/matrix-bypass.log" || {
    echo "production matrix bypass test failed for the wrong reason" >&2
    rm -rf -- "$matrix_bypass_root"
    exit 1
}
rm -rf -- "$matrix_bypass_root"

write_valid_fixture
rm -f -- "$FIXTURE/scripts/redb-production-evidence/$REVISION.json"
git -C "$FIXTURE" add -u -- "scripts/redb-production-evidence/$REVISION.json"
expect_rejected "missing evidence manifest" "not one tracked regular stage-0 blob"

write_valid_fixture
cat >"$FIXTURE/nora-registry/Cargo.toml" <<'EOF'
[package]
name = "nora-registry"
version = "1.1.0"

[dependencies]
redb = { version = "=4.2.0", features = ["logging"] }
EOF
git -C "$FIXTURE" add nora-registry/Cargo.toml
expect_rejected "unreviewed source kind" "must not set version"

echo "PASS: redb production gate and publisher bind canonical source, runners, components and evidence"
