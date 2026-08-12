#!/usr/bin/env bash
# One fail-closed app-side entrypoint for redb production qualification and
# promotion verification. Qualification builds and pushes the candidate image
# from the exact reviewed Git tree, tests that immutable digest, publishes the
# evidence bundle, and emits one reviewable patch. It never edits Git or
# deploys Helm. Verification derives the approved image/tree exclusively from
# the checked-in allowlist and evidence manifest.

set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
MATRIX="$ROOT/scripts/redb-production-matrix.sh"
PUBLISH="$ROOT/scripts/publish-redb-production-evidence.sh"
PROMOTION="$ROOT/scripts/verify-redb-promotion.sh"
RELEASE_SELF_TEST="$ROOT/scripts/test-redb-production-release-gate.sh"
IMAGE_REPOSITORY=docker-hub.just-ai.com/infra/artifact-nora
EVIDENCE_REPOSITORY=docker-hub.just-ai.com/infra/artifact-nora-redb-evidence
ALLOWLIST_REL=scripts/redb-production-allowlist.txt
EVIDENCE_DIR_REL=scripts/redb-production-evidence
MANIFEST_REL=nora-registry/Cargo.toml
LOCK_REL=Cargo.lock
REVIEW_TREE=

usage() {
    cat >&2 <<'EOF'
usage:
  redb-production-gate.sh qualify <40-hex-source-tree> <new-output-directory>
  redb-production-gate.sh verify

qualify archives the exact clean staged tree, builds and pushes its amd64
Harbor image by digest, runs the full matrix, publishes immutable evidence,
and emits a two-file approval patch. It does not modify Git, an image tag, a
chart, or a Helm release.

verify accepts no image or tree identity from the caller. It derives both from
the one checked-in approval for the current exact redb revision and performs
the read-only production release/image gate. Chart/Helm verification remains
the separate downstream gate.
EOF
    exit 2
}

fail() {
    echo "redb production gate blocked: $*" >&2
    exit 1
}

tracked_blob_oid() {
    local relative=$1 entry mode oid stage path
    entry=$(git -C "$ROOT" ls-files --stage -- "$relative")
    read -r mode oid stage path <<<"$entry"
    if [[ -z "$entry" || "$stage" != 0 \
        || ! "$mode" =~ ^100(644|755)$ || "$path" != "$relative" ]]; then
        return 1
    fi
    printf '%s\n' "$oid"
}

tree_blob_oid() {
    local tree=$1 relative=$2 entry metadata path mode type oid
    entry=$(git -C "$ROOT" ls-tree "$tree" -- "$relative")
    metadata=${entry%%$'\t'*}
    path=${entry#*$'\t'}
    read -r mode type oid <<<"$metadata"
    if [[ -z "$entry" || "$entry" != *$'\t'* \
        || ! "$mode" =~ ^100(644|755)$ || "$type" != blob \
        || "$path" != "$relative" ]]; then
        return 1
    fi
    printf '%s\n' "$oid"
}

reviewed_blob_oid() {
    local relative=$1
    if [[ -n "$REVIEW_TREE" ]]; then
        tree_blob_oid "$REVIEW_TREE" "$relative"
    else
        tracked_blob_oid "$relative"
    fi
}

require_commands() {
    local command
    for command in "$@"; do
        command -v "$command" >/dev/null \
            || fail "required command is missing: $command"
    done
}

require_clean_inputs() {
    git -C "$ROOT" diff --quiet -- \
        || fail "source tree has unstaged inputs"
    [[ -z $(git -C "$ROOT" ls-files --others --exclude-standard) ]] \
        || fail "source tree has untracked inputs"
}

require_exact_index() {
    local expected_tree=$1
    [[ "$expected_tree" =~ ^[0-9a-f]{40}$ ]] \
        || fail "source tree must be one full 40-hex Git tree"
    git -C "$ROOT" cat-file -e "$expected_tree^{tree}" 2>/dev/null \
        || fail "source tree is not an existing Git tree object"
    require_clean_inputs
    [[ $(git -C "$ROOT" write-tree) == "$expected_tree" ]] \
        || fail "reviewed source tree is not the current exact staged tree"
}

prepare_output() {
    local requested=$1 parent name resolved
    [[ ! -e "$requested" && ! -L "$requested" ]] \
        || fail "qualification output must not already exist"
    parent=$(dirname -- "$requested")
    name=$(basename -- "$requested")
    [[ "$name" != . && "$name" != .. && -n "$name" ]] \
        || fail "qualification output name is invalid"
    [[ -d "$parent" ]] \
        || fail "qualification output parent must already exist"
    parent=$(cd -- "$parent" && pwd -P)
    resolved="$parent/$name"
    case "$resolved/" in
        "$ROOT"/*) fail "qualification output must be outside the Git worktree" ;;
    esac
    mkdir -- "$resolved"
    printf '%s\n' "$resolved"
}

read_redb_revision() {
    local manifest_oid
    manifest_oid=$(reviewed_blob_oid "$MANIFEST_REL") \
        || fail "Cargo manifest is not one tracked regular stage-0 blob"
    git -C "$ROOT" cat-file blob "$manifest_oid" | python3 -c '
import re
import sys
import tomllib

dependency = tomllib.loads(sys.stdin.buffer.read().decode())\
    .get("dependencies", {}).get("redb")
if not isinstance(dependency, dict):
    raise SystemExit("redb production gate blocked: redb must use an exact reviewed git dependency")
for forbidden in ("version", "branch", "tag", "path", "workspace", "registry"):
    if dependency.get(forbidden):
        raise SystemExit(f"redb production gate blocked: redb dependency must not set {forbidden}")
if dependency.get("git") != "https://github.com/cberner/redb":
    raise SystemExit("redb production gate blocked: redb git repository is not the reviewed upstream")
revision = dependency.get("rev", "")
if not re.fullmatch(r"[0-9a-f]{40}", revision):
    raise SystemExit("redb production gate blocked: redb must pin one full 40-hex revision")
print(revision)
'
}

load_unique_reviewed_approval() {
    local revision=$1 row approved_kind approved_ref approved_commit
    local approved_engine approved_schema evidence_digest evidence_locator extra
    local actual_digest
    local -a rows

    CURRENT_ALLOWLIST_OID=$(reviewed_blob_oid "$ALLOWLIST_REL") \
        || fail "redb allowlist is not one tracked regular stage-0 blob"
    mapfile -t rows < <(
        git -C "$ROOT" cat-file blob "$CURRENT_ALLOWLIST_OID" \
            | awk -v revision="$revision" \
                '$1 == "git" && $2 == revision {print}'
    )
    ((${#rows[@]} == 1)) \
        || fail "current redb revision must have exactly one approval row"
    row=${rows[0]}
    read -r approved_kind approved_ref approved_commit approved_engine \
        approved_schema evidence_digest evidence_locator extra <<<"$row"
    [[ -z "${extra:-}" \
        && "$approved_kind" == git \
        && "$approved_ref" == "$revision" \
        && "$approved_commit" == "$revision" \
        && "$approved_engine" =~ ^[A-Za-z0-9._-]+$ \
        && "$approved_schema" =~ ^[0-9]+$ \
        && "$evidence_digest" =~ ^[0-9a-f]{64}$ \
        && "$evidence_locator" == "$EVIDENCE_DIR_REL/$revision.json" ]] \
        || fail "current redb approval row is malformed or not canonical"
    CURRENT_EVIDENCE_OID=$(reviewed_blob_oid "$evidence_locator") \
        || fail "current redb evidence is not one tracked regular stage-0 blob"
    actual_digest=$(git -C "$ROOT" cat-file blob "$CURRENT_EVIDENCE_OID" \
        | sha256sum | awk '{print $1}')
    [[ "$actual_digest" == "$evidence_digest" ]] \
        || fail "current redb evidence digest does not match the allowlist"

    CURRENT_EVIDENCE_LOCATOR=$evidence_locator
}

build_and_push_image() {
    local source_tree=$1 lock_digest=$2 output=$3
    local metadata="$output/image-build-metadata.json"
    local remote_manifest="$output/image-remote-manifest.json"
    local metadata_digest remote_manifest_digest descriptor_digest
    local image_ref receipt="$output/image-build-receipt.json"

    reviewed_blob_oid Dockerfile >/dev/null \
        || fail "Dockerfile is not one tracked regular stage-0 blob"
    if ! git -C "$ROOT" archive --format=tar "$source_tree" \
        | docker buildx build \
            --platform linux/amd64 \
            --file Dockerfile \
            --build-arg "NORA_SOURCE_TREE=$source_tree" \
            --build-arg "NORA_CARGO_LOCK_SHA256=$lock_digest" \
            --metadata-file "$metadata" \
            --output "type=image,name=$IMAGE_REPOSITORY,push-by-digest=true,name-canonical=true,push=true" \
            -; then
        fail "exact-tree image build/push failed"
    fi
    [[ -f "$metadata" && ! -L "$metadata" ]] \
        || fail "buildx did not produce regular build metadata"
    BUILD_IMAGE_DIGEST=$(jq -er '
        .["containerimage.digest"]
        | select(test("^sha256:[0-9a-f]{64}$"))
    ' "$metadata") || fail "build metadata lacks an immutable image digest"
    descriptor_digest=$(jq -er '
        .["containerimage.descriptor"].digest
        | select(test("^sha256:[0-9a-f]{64}$"))
    ' "$metadata") || fail "build metadata lacks an immutable descriptor digest"
    [[ "$descriptor_digest" == "$BUILD_IMAGE_DIGEST" ]] \
        || fail "build result and descriptor digests differ"
    image_ref="$IMAGE_REPOSITORY@$BUILD_IMAGE_DIGEST"

    docker buildx imagetools inspect "$image_ref" \
        --format '{{json .Manifest}}' >"$remote_manifest" \
        || fail "cannot read the pushed image back by digest"
    [[ -f "$remote_manifest" && ! -L "$remote_manifest" ]] \
        || fail "image read-back manifest is not a regular file"
    jq -e --arg digest "$BUILD_IMAGE_DIGEST" '
        .digest == $digest
        and (.mediaType | type == "string" and length > 0)
    ' "$remote_manifest" >/dev/null \
        || fail "remote image read-back does not match the build digest"

    metadata_digest=$(sha256sum "$metadata" | awk '{print $1}')
    remote_manifest_digest=$(sha256sum "$remote_manifest" | awk '{print $1}')
    jq -n \
        --arg source_tree "$source_tree" \
        --arg cargo_lock_sha256 "$lock_digest" \
        --arg image_ref "$image_ref" \
        --arg image_digest "$BUILD_IMAGE_DIGEST" \
        --arg build_metadata_sha256 "$metadata_digest" \
        --arg remote_manifest_sha256 "$remote_manifest_digest" \
        '{
            schema: 1,
            source_mode: "git_archive",
            source_tree: $source_tree,
            cargo_lock_sha256: $cargo_lock_sha256,
            platform: "linux/amd64",
            image_ref: $image_ref,
            image_digest: $image_digest,
            build_metadata_sha256: $build_metadata_sha256,
            remote_manifest_sha256: $remote_manifest_sha256,
            pushed_by_digest: true,
            remote_read_back_verified: true
        }' >"$receipt"
    jq -e --arg image "$image_ref" --arg tree "$source_tree" \
        '.schema == 1 and .image_ref == $image and .source_tree == $tree
         and .pushed_by_digest == true and .remote_read_back_verified == true' \
        "$receipt" >/dev/null \
        || fail "image build receipt is incomplete"

    BUILD_IMAGE_REF=$image_ref
    BUILD_RECEIPT=$receipt
}

append_required_diff() {
    local before=$1 after=$2 relative=$3 patch=$4 status=0
    if diff -q -- "$before" "$after" >/dev/null; then
        fail "qualification did not change $relative"
    fi
    diff -u --label "a/$relative" --label "b/$relative" \
        "$before" "$after" >>"$patch" || status=$?
    ((status == 1)) || fail "could not create the review patch for $relative"
}

run_qualify() {
    (($# == 2)) || usage
    local source_tree=$1 requested_output=$2 output lock_oid lock_digest revision
    local image matrix_log matrix_dir reported_dir resolved_dir
    local publication_log approval_source receipt_source
    local approval receipt approval_sha evidence_oci_digest source_digest locator row
    local generated_root candidate_approval candidate_allowlist before_approval before_allowlist
    local patch patch_sha allowlist_sha build_receipt_sha qualification_receipt
    local -a reported_matrix_dirs=() matrix_dirs=() candidate_rows=()

    require_commands diff docker env git jq python3 realpath sha256sum tee
    require_exact_index "$source_tree"
    REVIEW_TREE=$source_tree
    revision=$(read_redb_revision)
    load_unique_reviewed_approval "$revision"
    lock_oid=$(reviewed_blob_oid "$LOCK_REL") \
        || fail "Cargo.lock is not one tracked regular stage-0 blob"
    lock_digest=$(git -C "$ROOT" cat-file blob "$lock_oid" \
        | sha256sum | awk '{print $1}')
    output=$(prepare_output "$requested_output")
    mkdir -- "$output/matrix" "$output/publication"

    "$RELEASE_SELF_TEST"
    build_and_push_image "$source_tree" "$lock_digest" "$output"
    image=$BUILD_IMAGE_REF

    # The image is already immutable. Before the expensive matrix, independently
    # pull it and prove that its labels bind the exact archive tree and lockfile.
    env \
        -u NORA_PROMOTION_IMAGE \
        -u NORA_PROMOTION_CHANNEL \
        -u NORA_PROMOTION_SOURCE_TREE \
        NORA_ALLOW_UNSTABLE_REDB_TEST=1 \
        "$PROMOTION" "$image" test "$source_tree"

    matrix_log="$output/matrix.log"
    env \
        -u NORA_REDB_MATRIX_IMAGE \
        -u NORA_REDB_MATRIX_SOURCE_TREE \
        -u NORA_REDB_MATRIX_EVIDENCE_DIR \
        -u NORA_REDB_MATRIX_REQUIRE_REMOTE_DIGEST \
        -u NORA_REDB_MATRIX_COMPONENT_TIMEOUT_SECS \
        -u NORA_REDB_MATRIX_UPSTREAM_RUNNER \
        -u NORA_REDB_MATRIX_SNAPSHOT_ROOT \
        -u NORA_REDB_MATRIX_RUN_ROOT \
        "$MATRIX" "$image" "$source_tree" "$output/matrix" \
        | tee "$matrix_log"
    mapfile -t reported_matrix_dirs \
        < <(sed -n 's/^evidence_dir=//p' "$matrix_log")
    for reported_dir in "${reported_matrix_dirs[@]}"; do
        [[ -d "$reported_dir" && ! -L "$reported_dir" ]] || continue
        resolved_dir=$(realpath -e -- "$reported_dir") || continue
        [[ $(dirname -- "$resolved_dir") == "$output/matrix" \
            && $(basename -- "$resolved_dir") == nora-redb-production-matrix.* \
            && -f "$resolved_dir/redb-production-evidence.tar" \
            && ! -L "$resolved_dir/redb-production-evidence.tar" \
            && -f "$resolved_dir/redb-production-matrix.json" \
            && ! -L "$resolved_dir/redb-production-matrix.json" ]] \
            || continue
        matrix_dirs+=("$resolved_dir")
    done
    ((${#matrix_dirs[@]} == 1)) \
        || fail "matrix did not report exactly one canonical evidence directory"
    matrix_dir=${matrix_dirs[0]}

    publication_log="$output/publication.log"
    env \
        -u NORA_REDB_EVIDENCE_MATRIX_DIR \
        -u NORA_REDB_EVIDENCE_REPOSITORY \
        -u NORA_REDB_EVIDENCE_OUTPUT \
        -u NORA_REDB_EVIDENCE_ORAS_TIMEOUT_SECS \
        -u NORA_GATE_PUBLISH_MODE \
        -u NORA_GATE_PUBLISH_SNAPSHOT_READY_FILE \
        -u NORA_GATE_PUBLISH_SNAPSHOT_CONTINUE_FILE \
        "$PUBLISH" "$matrix_dir" "$EVIDENCE_REPOSITORY" "$output/publication" \
        | tee "$publication_log"

    approval_source="$output/publication/redb-production-approval.json"
    receipt_source="$output/publication/redb-production-oci-receipt.json"
    [[ -f "$approval_source" && ! -L "$approval_source" \
        && -f "$receipt_source" && ! -L "$receipt_source" ]] \
        || fail "publisher did not produce canonical approval and receipt files"
    approval="$output/generated-approval.json"
    receipt="$output/generated-receipt.json"
    python3 - "$approval_source" "$receipt_source" "$approval" "$receipt" <<'PY'
import os
import shutil
import stat
import sys

for source, destination in zip(sys.argv[1:3], sys.argv[3:5], strict=True):
    try:
        descriptor = os.open(source, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except OSError as error:
        raise SystemExit(f"cannot snapshot publisher output: {error.strerror}")
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise SystemExit("publisher output is not a regular file")
        with os.fdopen(descriptor, "rb", closefd=False) as source_handle, open(
            destination, "xb"
        ) as destination_handle:
            shutil.copyfileobj(source_handle, destination_handle, 1024 * 1024)
    finally:
        os.close(descriptor)
PY
    jq -e \
        --arg image "$image" \
        --arg image_digest "$BUILD_IMAGE_DIGEST" \
        --arg tree "$source_tree" \
        --arg lock "$lock_digest" \
        --arg revision "$revision" '
        .schema == 3
        and .redb_revision == $revision
        and .redb_resolved_commit == $revision
        and .cargo_lock_sha256 == $lock
        and .image_ref == $image
        and .image_digest == $image_digest
        and .image_source_tree == $tree
        and (.nora_source_digest | test("^[0-9a-f]{64}$"))
        and (.bundle_uri | test("^oci://docker-hub\\.just-ai\\.com/infra/artifact-nora-redb-evidence@sha256:[0-9a-f]{64}$"))
        and (.evidence_oci_manifest_digest | test("^sha256:[0-9a-f]{64}$"))
        and (. as $approval
             | $approval.bundle_uri
             | endswith("@" + $approval.evidence_oci_manifest_digest))
        and (.bundle_sha256 | test("^[0-9a-f]{64}$"))
        and (.matrix_manifest_sha256 | test("^[0-9a-f]{64}$"))
    ' "$approval" >/dev/null \
        || fail "generated approval does not bind the built image, exact tree and immutable evidence"
    approval_sha=$(sha256sum "$approval" | awk '{print $1}')
    jq -e --arg approval_sha "$approval_sha" '
        .schema == 1
        and .read_back_verified == true
        and .approval_manifest_sha256 == $approval_sha
        and (.immutable_ref | test("^docker-hub\\.just-ai\\.com/infra/artifact-nora-redb-evidence@sha256:[0-9a-f]{64}$"))
        and (.evidence_oci_manifest_digest | test("^sha256:[0-9a-f]{64}$"))
        and (. as $receipt
             | $receipt.immutable_ref
             | endswith("@" + $receipt.evidence_oci_manifest_digest))
    ' "$receipt" >/dev/null \
        || fail "publisher receipt does not prove immutable read-back of these approval bytes"

    locator="$EVIDENCE_DIR_REL/$revision.json"
    [[ "$locator" == "$CURRENT_EVIDENCE_LOCATOR" ]] \
        || fail "qualification would change the canonical evidence locator"
    resolved=$(jq -er '.redb_resolved_commit | select(test("^[0-9a-f]{40}$"))' "$approval")
    engine=$(jq -er '.engine_revision | select(test("^[A-Za-z0-9._-]+$"))' "$approval")
    schema=$(jq -er '.nora_schema_version | select(type == "number" and . >= 0 and floor == .)' "$approval")
    [[ "$resolved" == "$revision" ]] \
        || fail "approval redb revision and resolved commit differ"
    row="git $revision $resolved $engine $schema $approval_sha $locator"

    generated_root="$output/generated"
    candidate_approval="$generated_root/$locator"
    candidate_allowlist="$generated_root/$ALLOWLIST_REL"
    mkdir -p -- "$(dirname -- "$candidate_approval")" "$(dirname -- "$candidate_allowlist")"
    cp -- "$approval" "$candidate_approval"
    [[ $(sha256sum "$candidate_approval" | awk '{print $1}') == "$approval_sha" ]] \
        || fail "approval bytes changed while preparing the checked-in artifact"

    before_allowlist="$output/.allowlist.before"
    git -C "$ROOT" cat-file blob "$CURRENT_ALLOWLIST_OID" >"$before_allowlist"
    python3 - "$before_allowlist" "$candidate_allowlist" "$revision" "$row" <<'PY'
import pathlib
import sys

source, destination, revision, replacement = sys.argv[1:]
lines = pathlib.Path(source).read_text(encoding="utf-8").splitlines(keepends=True)
matches = [
    index
    for index, line in enumerate(lines)
    if len(fields := line.split()) >= 2
    and fields[0] == "git"
    and fields[1] == revision
]
if len(matches) != 1:
    raise SystemExit("redb production gate blocked: allowlist replacement is not unique")
lines[matches[0]] = replacement + "\n"
pathlib.Path(destination).write_text("".join(lines), encoding="utf-8")
PY
    mapfile -t candidate_rows < <(
        awk -v revision="$revision" \
            '$1 == "git" && $2 == revision {print}' "$candidate_allowlist"
    )
    if ((${#candidate_rows[@]} != 1)) \
        || [[ "${candidate_rows[0]}" != "$row" ]]; then
        fail "generated allowlist does not contain one exact replacement row"
    fi

    before_approval="$output/.approval.before"
    git -C "$ROOT" cat-file blob "$CURRENT_EVIDENCE_OID" >"$before_approval"
    patch="$output/approval.patch"
    : >"$patch"
    append_required_diff "$before_approval" "$candidate_approval" "$locator" "$patch"
    append_required_diff "$before_allowlist" "$candidate_allowlist" "$ALLOWLIST_REL" "$patch"
    require_exact_index "$source_tree"
    git -C "$ROOT" apply --check -- "$patch" \
        || fail "generated approval patch does not apply to the exact reviewed tree"
    rm -f -- "$before_approval" "$before_allowlist"

    patch_sha=$(sha256sum "$patch" | awk '{print $1}')
    allowlist_sha=$(sha256sum "$candidate_allowlist" | awk '{print $1}')
    build_receipt_sha=$(sha256sum "$BUILD_RECEIPT" | awk '{print $1}')
    evidence_oci_digest=$(jq -er '.evidence_oci_manifest_digest' "$approval")
    source_digest=$(jq -er '.nora_source_digest' "$approval")
    qualification_receipt="$output/redb-production-qualification-receipt.json"
    jq -n \
        --arg source_tree "$source_tree" \
        --arg source_digest "$source_digest" \
        --arg cargo_lock_sha256 "$lock_digest" \
        --arg image_ref "$image" \
        --arg image_digest "$BUILD_IMAGE_DIGEST" \
        --arg image_build_receipt_sha256 "$build_receipt_sha" \
        --arg evidence_oci_manifest_digest "$evidence_oci_digest" \
        --arg approval_locator "$locator" \
        --arg approval_sha256 "$approval_sha" \
        --arg allowlist_sha256 "$allowlist_sha" \
        --arg approval_patch_sha256 "$patch_sha" \
        '{
            schema: 1,
            source_tree: $source_tree,
            source_digest: $source_digest,
            cargo_lock_sha256: $cargo_lock_sha256,
            image_ref: $image_ref,
            image_digest: $image_digest,
            image_build_receipt_sha256: $image_build_receipt_sha256,
            evidence_oci_manifest_digest: $evidence_oci_manifest_digest,
            approval_locator: $approval_locator,
            approval_sha256: $approval_sha256,
            allowlist_sha256: $allowlist_sha256,
            approval_patch_sha256: $approval_patch_sha256,
            integration_apply_required: true
        }' >"$qualification_receipt"

    echo "PASS: exact-tree image and successor redb evidence are qualified and read back"
    echo "image=$image"
    echo "approval_patch=$patch"
    echo "candidate_approval=$candidate_approval"
    echo "candidate_allowlist=$candidate_allowlist"
    echo "qualification_receipt=$qualification_receipt"
    echo "next=review and apply the two-file patch, stage both files, then run:"
    printf '  git -C %q apply %q\n' "$ROOT" "$patch"
    printf '  git -C %q add -- %q %q\n' \
        "$ROOT" "$locator" "$ALLOWLIST_REL"
    printf '  %q verify\n' "$0"
}

run_verify() {
    (($# == 0)) || usage
    local revision evidence_identity image source_tree

    require_commands env git jq python3 sha256sum
    require_clean_inputs
    REVIEW_TREE=
    revision=$(read_redb_revision)
    load_unique_reviewed_approval "$revision"
    evidence_identity=$(git -C "$ROOT" cat-file blob "$CURRENT_EVIDENCE_OID" \
        | jq -er --arg revision "$revision" '
            select(
                .schema == 3
                and .redb_revision == $revision
                and .redb_resolved_commit == $revision
                and (.image_ref | test("^docker-hub\\.just-ai\\.com/infra/artifact-nora@sha256:[0-9a-f]{64}$"))
                and (.image_digest | test("^sha256:[0-9a-f]{64}$"))
                and .image_ref == ("docker-hub.just-ai.com/infra/artifact-nora@" + .image_digest)
                and (.image_source_tree | test("^[0-9a-f]{40}$"))
            )
            | [.image_ref, .image_source_tree]
            | @tsv
        ') || fail "checked-in approval does not contain one canonical image/tree identity"
    IFS=$'\t' read -r image source_tree <<<"$evidence_identity"

    "$RELEASE_SELF_TEST"
    # Production promotion invokes the deep release verifier itself before it
    # pulls the exact image, so calling it again here would only duplicate work.
    env \
        -u NORA_PROMOTION_IMAGE \
        -u NORA_PROMOTION_CHANNEL \
        -u NORA_PROMOTION_SOURCE_TREE \
        -u NORA_ALLOW_UNSTABLE_REDB_TEST \
        "$PROMOTION" "$image" production "$source_tree"

    echo "PASS: checked-in redb evidence and immutable Harbor image are approved"
    echo "image=$image"
    echo "image_source_tree=$source_tree"
    echo "next=run the separate chart package/render and Helm digest gate"
}

(($# >= 1)) || usage
command=$1
shift
case "$command" in
    qualify) run_qualify "$@" ;;
    verify) run_verify "$@" ;;
    *) usage ;;
esac
