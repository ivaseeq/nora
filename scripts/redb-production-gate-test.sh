#!/usr/bin/env bash

set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
FIXTURE=$(mktemp -d /tmp/nora-redb-production-gate-test.XXXXXXXX)

cleanup() {
    local exit_code=$?
    trap - EXIT INT TERM
    case "$FIXTURE" in
        /tmp/nora-redb-production-gate-test.*) rm -rf -- "$FIXTURE" ;;
        *) echo "refusing to remove unexpected production-gate fixture" >&2 ;;
    esac
    exit "$exit_code"
}
trap cleanup EXIT INT TERM

REPO="$FIXTURE/repo"
BIN="$FIXTURE/bin"
LOG="$FIXTURE/calls.log"
REVISION=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
IMAGE_DIGEST="sha256:$(printf 'f%.0s' {1..64})"
IMAGE="docker-hub.just-ai.com/infra/artifact-nora@$IMAGE_DIGEST"
mkdir -p \
    "$REPO/scripts/redb-production-evidence" \
    "$REPO/nora-registry" \
    "$BIN"
cp -- "$ROOT/scripts/redb-production-gate.sh" "$REPO/scripts/"

cat >"$REPO/scripts/test-redb-production-release-gate.sh" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
echo self-test >>"$NORA_GATE_TEST_LOG"
EOF

cat >"$REPO/scripts/verify-redb-production-release.sh" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
echo release-verify >>"$NORA_GATE_TEST_LOG"
[[ ${NORA_GATE_TEST_RELEASE_FAIL:-0} != 1 ]]
EOF

cat >"$REPO/scripts/verify-redb-promotion.sh" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
image=${NORA_PROMOTION_IMAGE:-$1}
channel=${NORA_PROMOTION_CHANNEL:-$2}
tree=${NORA_PROMOTION_SOURCE_TREE:-$3}
if [[ "$channel" == test ]]; then
    [[ ${NORA_ALLOW_UNSTABLE_REDB_TEST:-0} == 1 ]]
else
    "$(dirname -- "$0")/verify-redb-production-release.sh"
fi
echo "promotion $channel $image $tree" >>"$NORA_GATE_TEST_LOG"
EOF

cat >"$REPO/scripts/redb-production-matrix.sh" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
image=${NORA_REDB_MATRIX_IMAGE:-$1}
tree=${NORA_REDB_MATRIX_SOURCE_TREE:-$2}
output=${NORA_REDB_MATRIX_EVIDENCE_DIR:-$3}
[[ ${NORA_REDB_MATRIX_REQUIRE_REMOTE_DIGEST:-1} == 1 \
    && -z ${NORA_REDB_MATRIX_SNAPSHOT_ROOT:-} \
    && -z ${NORA_REDB_MATRIX_RUN_ROOT:-} ]]
evidence="$output/nora-redb-production-matrix.fixture"
mkdir -- "$evidence"
printf 'bundle\n' >"$evidence/redb-production-evidence.tar"
printf '{"image":"%s","tree":"%s"}\n' "$image" "$tree" \
    >"$evidence/redb-production-matrix.json"
printf '%s\n%s\n' "$image" "$tree" >"$evidence/identity"
echo "matrix $image $tree" >>"$NORA_GATE_TEST_LOG"
echo PASS
component_evidence="$NORA_GATE_TEST_ROOT/component-evidence"
mkdir -p -- "$component_evidence"
printf 'component bundle\n' >"$component_evidence/redb-minio-e2e-evidence.tar"
echo "evidence_dir=$component_evidence"
echo "evidence_dir=$evidence"
if [[ ${NORA_GATE_TEST_DUPLICATE_MATRIX_DIR:-0} == 1 ]]; then
    echo "evidence_dir=$evidence"
fi
EOF

cat >"$REPO/scripts/publish-redb-production-evidence.sh" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
matrix=${NORA_REDB_EVIDENCE_MATRIX_DIR:-$1}
repository=${NORA_REDB_EVIDENCE_REPOSITORY:-$2}
output=${NORA_REDB_EVIDENCE_OUTPUT:-$3}
[[ -z ${NORA_GATE_PUBLISH_MODE:-} \
    && -z ${NORA_GATE_PUBLISH_SNAPSHOT_READY_FILE:-} \
    && -z ${NORA_GATE_PUBLISH_SNAPSHOT_CONTINUE_FILE:-} ]]
readarray -t identity <"$matrix/identity"
image=${identity[0]}
tree=${identity[1]}
image_digest=${image##*@}
root=$(cd -- "$(dirname -- "$0")/.." && pwd)
lock_digest=$(sha256sum "$root/Cargo.lock" | awk '{print $1}')
revision=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
oci=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
bundle=cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
matrix_digest=dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
source_digest=1111111111111111111111111111111111111111111111111111111111111111
mkdir -p "$output"
cat >"$output/redb-production-approval.json" <<JSON
{"schema":3,"redb_revision":"$revision","redb_resolved_commit":"$revision","engine_revision":"redb-c419f099-dev","nora_schema_version":6,"cargo_lock_sha256":"$lock_digest","nora_source_digest":"$source_digest","image_ref":"$image","image_digest":"$image_digest","image_source_tree":"$tree","bundle_uri":"oci://$repository@sha256:$oci","evidence_oci_manifest_digest":"sha256:$oci","bundle_sha256":"$bundle","matrix_manifest_sha256":"$matrix_digest"}
JSON
approval_sha=$(sha256sum "$output/redb-production-approval.json" | awk '{print $1}')
if [[ ${NORA_GATE_TEST_BAD_RECEIPT:-0} == 1 ]]; then
    approval_sha=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
fi
cat >"$output/redb-production-oci-receipt.json" <<JSON
{"schema":1,"immutable_ref":"$repository@sha256:$oci","evidence_oci_manifest_digest":"sha256:$oci","approval_manifest_sha256":"$approval_sha","read_back_verified":true}
JSON
echo "publish $repository" >>"$NORA_GATE_TEST_LOG"
echo PASS
EOF

cat >"$BIN/docker" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
digest="sha256:$(printf 'f%.0s' {1..64})"
if [[ "$1 $2" == "buildx build" ]]; then
    shift 2
    platform= file= tree= lock= metadata= output= context=
    while (($#)); do
        case "$1" in
            --platform) platform=$2; shift 2 ;;
            --file) file=$2; shift 2 ;;
            --build-arg)
                case "$2" in
                    NORA_SOURCE_TREE=*) tree=${2#*=} ;;
                    NORA_CARGO_LOCK_SHA256=*) lock=${2#*=} ;;
                    *) exit 2 ;;
                esac
                shift 2
                ;;
            --metadata-file) metadata=$2; shift 2 ;;
            --output) output=$2; shift 2 ;;
            -) context=-; shift ;;
            *) echo "unexpected mock build argument: $1" >&2; exit 2 ;;
        esac
    done
    [[ "$platform" == linux/amd64 && "$file" == Dockerfile \
        && "$tree" =~ ^[0-9a-f]{40}$ && "$lock" =~ ^[0-9a-f]{64}$ \
        && -n "$metadata" && "$context" == - \
        && "$output" == "type=image,name=docker-hub.just-ai.com/infra/artifact-nora,push-by-digest=true,name-canonical=true,push=true" ]]
    tar -tf - >"$NORA_GATE_TEST_ROOT/archive-members.txt"
    grep -qx Dockerfile "$NORA_GATE_TEST_ROOT/archive-members.txt"
    grep -qx Cargo.lock "$NORA_GATE_TEST_ROOT/archive-members.txt"
    printf '{"containerimage.digest":"%s","containerimage.descriptor":{"digest":"%s"}}\n' \
        "$digest" "$digest" >"$metadata"
    echo "build $platform $file $tree $lock" >>"$NORA_GATE_TEST_LOG"
elif [[ "$1 $2 $3" == "buildx imagetools inspect" ]]; then
    image=$4
    [[ "$image" == "docker-hub.just-ai.com/infra/artifact-nora@$digest" ]]
    remote_digest=$digest
    if [[ ${NORA_GATE_TEST_BAD_REMOTE_DIGEST:-0} == 1 ]]; then
        remote_digest="sha256:$(printf 'e%.0s' {1..64})"
    fi
    printf '{"digest":"%s","mediaType":"application/vnd.oci.image.manifest.v1+json"}\n' \
        "$remote_digest"
    echo "read-back $image" >>"$NORA_GATE_TEST_LOG"
else
    echo "unexpected mock docker invocation: $*" >&2
    exit 2
fi
EOF

chmod +x "$REPO/scripts/"*.sh "$BIN/docker"
cat >"$REPO/nora-registry/Cargo.toml" <<EOF
[package]
name = "nora-registry"
version = "1.0.0"

[dependencies]
redb = { git = "https://github.com/cberner/redb", rev = "$REVISION" }
EOF
cat >"$REPO/Cargo.lock" <<EOF
version = 4

[[package]]
name = "redb"
version = "4.1.0"
source = "git+https://github.com/cberner/redb?rev=$REVISION#$REVISION"
EOF
cat >"$REPO/Dockerfile" <<'EOF'
FROM scratch
ARG NORA_SOURCE_TREE
ARG NORA_CARGO_LOCK_SHA256
LABEL io.nora.source-tree="$NORA_SOURCE_TREE" \
      io.nora.cargo-lock-sha256="$NORA_CARGO_LOCK_SHA256"
EOF
printf '{}\n' >"$REPO/scripts/redb-production-evidence/$REVISION.json"
old_evidence_sha=$(sha256sum \
    "$REPO/scripts/redb-production-evidence/$REVISION.json" | awk '{print $1}')
cat >"$REPO/scripts/redb-production-allowlist.txt" <<EOF
# fixture approval
git $REVISION $REVISION redb-old 5 $old_evidence_sha scripts/redb-production-evidence/$REVISION.json
EOF

git -C "$REPO" init -q
git -C "$REPO" config user.name fixture
git -C "$REPO" config user.email fixture@example.invalid
git -C "$REPO" add -A
git -C "$REPO" commit -qm fixture

TREE=$(git -C "$REPO" rev-parse 'HEAD^{tree}')
OTHER_TREE=$(git -C "$REPO" mktree </dev/null)
LOCK_DIGEST=$(sha256sum "$REPO/Cargo.lock" | awk '{print $1}')
export NORA_GATE_TEST_LOG="$LOG"
export NORA_GATE_TEST_ROOT="$FIXTURE"
export PATH="$BIN:$PATH"

expect_failure() {
    local expected=$1
    shift
    local output status
    set +e
    output=$("$@" 2>&1)
    status=$?
    set -e
    ((status != 0)) || {
        echo "expected production gate failure: $expected" >&2
        exit 1
    }
    [[ -z "$expected" || "$output" == *"$expected"* ]] || {
        echo "production gate failed for an unexpected reason" >&2
        echo "$output" >&2
        exit 1
    }
}

env \
    NORA_PROMOTION_IMAGE=docker-hub.just-ai.com/infra/artifact-nora@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee \
    NORA_PROMOTION_CHANNEL=production \
    NORA_PROMOTION_SOURCE_TREE=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
    NORA_REDB_MATRIX_IMAGE=attacker.invalid/image:latest \
    NORA_REDB_MATRIX_SOURCE_TREE=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
    NORA_REDB_MATRIX_EVIDENCE_DIR="$FIXTURE/attacker-matrix" \
    NORA_REDB_MATRIX_REQUIRE_REMOTE_DIGEST=0 \
    NORA_REDB_MATRIX_SNAPSHOT_ROOT="$FIXTURE/attacker-source" \
    NORA_REDB_MATRIX_RUN_ROOT="$FIXTURE/attacker-run" \
    NORA_REDB_EVIDENCE_MATRIX_DIR="$FIXTURE/attacker-matrix" \
    NORA_REDB_EVIDENCE_REPOSITORY=attacker.invalid/evidence \
    NORA_REDB_EVIDENCE_OUTPUT="$FIXTURE/attacker-publication" \
    NORA_GATE_PUBLISH_MODE=1 \
    NORA_GATE_PUBLISH_SNAPSHOT_READY_FILE="$FIXTURE/attacker-ready" \
    NORA_GATE_PUBLISH_SNAPSHOT_CONTINUE_FILE="$FIXTURE/attacker-continue" \
    "$REPO/scripts/redb-production-gate.sh" qualify "$TREE" "$FIXTURE/success" \
    >/dev/null
expected_approval="$FIXTURE/success/generated/scripts/redb-production-evidence/$REVISION.json"
expected_allowlist="$FIXTURE/success/generated/scripts/redb-production-allowlist.txt"
expected_patch="$FIXTURE/success/approval.patch"
[[ -s "$expected_approval" && -s "$expected_allowlist" && -s "$expected_patch" ]]
approval_sha=$(sha256sum "$expected_approval" | awk '{print $1}')
expected_row="git $REVISION $REVISION redb-c419f099-dev 6 $approval_sha scripts/redb-production-evidence/$REVISION.json"
[[ $(tail -n 1 "$expected_allowlist") == "$expected_row" ]]
git -C "$REPO" apply --check -- "$expected_patch"
grep -q "^--- a/scripts/redb-production-evidence/$REVISION.json$" "$expected_patch"
grep -q '^--- a/scripts/redb-production-allowlist.txt$' "$expected_patch"
jq -e --arg image "$IMAGE" --arg tree "$TREE" --arg lock "$LOCK_DIGEST" '
    .schema == 1 and .source_mode == "git_archive"
    and .source_tree == $tree and .cargo_lock_sha256 == $lock
    and .image_ref == $image and .pushed_by_digest == true
    and .remote_read_back_verified == true
' "$FIXTURE/success/image-build-receipt.json" >/dev/null
jq -e --arg image "$IMAGE" --arg tree "$TREE" '
    .schema == 1 and .source_tree == $tree and .image_ref == $image
    and .integration_apply_required == true
    and (.approval_patch_sha256 | test("^[0-9a-f]{64}$"))
' "$FIXTURE/success/redb-production-qualification-receipt.json" >/dev/null
[[ $(<"$LOG") == *"build linux/amd64 Dockerfile $TREE $LOCK_DIGEST"* ]]
[[ $(<"$LOG") == *"read-back $IMAGE"* ]]
[[ $(<"$LOG") == *"promotion test $IMAGE $TREE"* ]]
[[ $(<"$LOG") == *"matrix $IMAGE $TREE"* ]]
[[ $(<"$LOG") == *"publish docker-hub.just-ai.com/infra/artifact-nora-redb-evidence"* ]]

expect_failure "not the current exact staged tree" \
    "$REPO/scripts/redb-production-gate.sh" qualify \
    "$OTHER_TREE" "$FIXTURE/wrong-tree"
expect_failure "outside the Git worktree" \
    "$REPO/scripts/redb-production-gate.sh" qualify "$TREE" \
    "$REPO/generated-evidence"
ln -s -- "$REPO" "$FIXTURE/repo-link"
expect_failure "outside the Git worktree" \
    "$FIXTURE/repo-link/scripts/redb-production-gate.sh" qualify "$TREE" \
    "$REPO/symlink-generated-evidence"
expect_failure "remote image read-back does not match" \
    env NORA_GATE_TEST_BAD_REMOTE_DIGEST=1 \
    "$REPO/scripts/redb-production-gate.sh" qualify "$TREE" \
    "$FIXTURE/bad-image-readback"
expect_failure "exactly one canonical evidence directory" \
    env NORA_GATE_TEST_DUPLICATE_MATRIX_DIR=1 \
    "$REPO/scripts/redb-production-gate.sh" qualify "$TREE" \
    "$FIXTURE/duplicate-matrix"
expect_failure "receipt does not prove immutable read-back" \
    env NORA_GATE_TEST_BAD_RECEIPT=1 \
    "$REPO/scripts/redb-production-gate.sh" qualify "$TREE" \
    "$FIXTURE/bad-receipt"

cp -- "$REPO/scripts/redb-production-allowlist.txt" "$FIXTURE/original-allowlist.txt"
duplicate_row=$(tail -n 1 "$REPO/scripts/redb-production-allowlist.txt")
printf '%s\n' "$duplicate_row" >>"$REPO/scripts/redb-production-allowlist.txt"
git -C "$REPO" add scripts/redb-production-allowlist.txt
duplicate_tree=$(git -C "$REPO" write-tree)
expect_failure "exactly one approval row" \
    "$REPO/scripts/redb-production-gate.sh" qualify "$duplicate_tree" \
    "$FIXTURE/duplicate-current-approval"
cp -- "$FIXTURE/original-allowlist.txt" \
    "$REPO/scripts/redb-production-allowlist.txt"
git -C "$REPO" add scripts/redb-production-allowlist.txt
[[ $(git -C "$REPO" write-tree) == "$TREE" ]]

git -C "$REPO" apply -- "$expected_patch"
git -C "$REPO" add \
    "scripts/redb-production-evidence/$REVISION.json" \
    scripts/redb-production-allowlist.txt
: >"$LOG"
env \
    NORA_PROMOTION_IMAGE=docker-hub.just-ai.com/infra/artifact-nora@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee \
    NORA_PROMOTION_CHANNEL=test \
    NORA_PROMOTION_SOURCE_TREE=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
    NORA_ALLOW_UNSTABLE_REDB_TEST=1 \
    "$REPO/scripts/redb-production-gate.sh" verify >/dev/null
mapfile -t verify_calls <"$LOG"
[[ "${verify_calls[*]}" == \
    "self-test release-verify promotion production $IMAGE $TREE" ]]
expect_failure "usage:" \
    "$REPO/scripts/redb-production-gate.sh" verify "$IMAGE" "$TREE"

cp -- "$REPO/scripts/redb-production-allowlist.txt" "$FIXTURE/approved-allowlist.txt"
duplicate_row=$(tail -n 1 "$REPO/scripts/redb-production-allowlist.txt")
printf '%s\n' "$duplicate_row" >>"$REPO/scripts/redb-production-allowlist.txt"
git -C "$REPO" add scripts/redb-production-allowlist.txt
expect_failure "exactly one approval row" \
    "$REPO/scripts/redb-production-gate.sh" verify
cp -- "$FIXTURE/approved-allowlist.txt" \
    "$REPO/scripts/redb-production-allowlist.txt"
git -C "$REPO" add scripts/redb-production-allowlist.txt

: >"$LOG"
expect_failure "" env NORA_GATE_TEST_RELEASE_FAIL=1 \
    "$REPO/scripts/redb-production-gate.sh" verify
[[ $(<"$LOG") == $'self-test\nrelease-verify' ]]

echo "PASS: redb production entrypoint builds the exact tree and preserves one approval path"
