#!/usr/bin/env bash

set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
IMAGE=${NORA_PROMOTION_IMAGE:-${1:-}}
CHANNEL=${NORA_PROMOTION_CHANNEL:-${2:-production}}
EXPECTED_SOURCE_TREE=${NORA_PROMOTION_SOURCE_TREE:-${3:-}}

if [[ -z "$IMAGE" || ! "$EXPECTED_SOURCE_TREE" =~ ^[0-9a-f]{40}$ ]]; then
    echo "usage: $0 <harbor-image@sha256:digest> <test|production> <40-hex-source-tree>" >&2
    exit 2
fi
[[ "$IMAGE" =~ ^docker-hub\.just-ai\.com/infra/artifact-nora@sha256:[0-9a-f]{64}$ ]] || {
    echo "promotion blocked: image must be the immutable approved Harbor repository digest" >&2
    exit 2
}
for command in docker git jq python3 sha256sum; do
    command -v "$command" >/dev/null || {
        echo "promotion blocked: missing required command: $command" >&2
        exit 2
    }
done
if ! git -C "$ROOT" diff --quiet \
    || [[ -n $(git -C "$ROOT" ls-files --others --exclude-standard) ]]; then
    echo "promotion blocked: source tree has unstaged or untracked inputs" >&2
    exit 1
fi
case "$CHANNEL" in
    production)
        "$ROOT/scripts/verify-redb-production-release.sh"
        current_revision=$(python3 - "$ROOT/nora-registry/Cargo.toml" <<'PY'
import sys
import tomllib

with open(sys.argv[1], "rb") as handle:
    dependency = tomllib.load(handle)["dependencies"]["redb"]
print(dependency["rev"])
PY
        )
        allowlist_oid=$(git -C "$ROOT" ls-files --stage -- \
            scripts/redb-production-allowlist.txt | awk '$3 == 0 {print $2}')
        [[ -n "$allowlist_oid" ]] || {
            echo "promotion blocked: production allowlist is not a tracked stage-0 blob" >&2
            exit 1
        }
        mapfile -t approved_locators < <(
            git -C "$ROOT" cat-file blob "$allowlist_oid" \
                | awk -v revision="$current_revision" \
                    '$1 == "git" && $2 == revision && NF == 7 {print $7}'
        )
        ((${#approved_locators[@]} == 1)) || {
            echo "promotion blocked: current redb revision has no unique approved evidence locator" >&2
            exit 1
        }
        approval_locator=${approved_locators[0]}
        approval_entry=$(git -C "$ROOT" ls-files --stage -- "$approval_locator")
        read -r approval_mode approval_oid approval_stage approval_path <<<"$approval_entry"
        [[ "$approval_stage" == 0 \
            && ("$approval_mode" == 100644 || "$approval_mode" == 100755) \
            && "$approval_path" == "$approval_locator" ]] || {
            echo "promotion blocked: approved evidence is not a tracked regular stage-0 blob" >&2
            exit 1
        }
        if ! git -C "$ROOT" cat-file blob "$approval_oid" \
            | jq -er --arg image "$IMAGE" --arg tree "$EXPECTED_SOURCE_TREE" \
                'select(.schema == 3 and .image_ref == $image and .image_source_tree == $tree) | $image' \
            >/dev/null; then
            echo "promotion blocked: production evidence does not bind this exact image/tree" >&2
            exit 1
        fi
        ;;
    test)
        if [[ ${NORA_ALLOW_UNSTABLE_REDB_TEST:-0} != 1 ]]; then
            echo "promotion blocked: test-only non-release redb requires NORA_ALLOW_UNSTABLE_REDB_TEST=1" >&2
            exit 1
        fi
        [[ $(git -C "$ROOT" write-tree) == "$EXPECTED_SOURCE_TREE" ]] || {
            echo "promotion blocked: test image tree is not the current exact staged tree" >&2
            exit 1
        }
        ;;
    *)
        echo "promotion blocked: channel must be test or production" >&2
        exit 2
        ;;
esac

docker pull "$IMAGE" >/dev/null
expected_lock=$(sha256sum "$ROOT/Cargo.lock" | awk '{print $1}')
actual_tree=$(docker image inspect \
    --format '{{index .Config.Labels "io.nora.source-tree"}}' "$IMAGE")
actual_lock=$(docker image inspect \
    --format '{{index .Config.Labels "io.nora.cargo-lock-sha256"}}' "$IMAGE")
image_id=$(docker image inspect --format '{{.Id}}' "$IMAGE")
repo_digests=$(docker image inspect --format '{{json .RepoDigests}}' "$IMAGE")

if [[ "$actual_tree" != "$EXPECTED_SOURCE_TREE" || "$actual_lock" != "$expected_lock" ]]; then
    echo "promotion blocked: image labels do not match the reviewed tree and Cargo.lock" >&2
    exit 1
fi
jq -e --arg image "$IMAGE" 'any(.[]; . == $image)' <<<"$repo_digests" >/dev/null || {
    echo "promotion blocked: pulled image does not report the requested immutable digest" >&2
    exit 1
}

printf 'promotion_channel=%s\nimage=%s\nimage_id=%s\nsource_tree=%s\ncargo_lock_sha256=%s\n' \
    "$CHANNEL" "$IMAGE" "$image_id" "$actual_tree" "$actual_lock"
