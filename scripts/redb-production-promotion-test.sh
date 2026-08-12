#!/usr/bin/env bash

set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
FIXTURE=$(mktemp -d /tmp/nora-redb-production-promotion-test.XXXXXXXX)

cleanup() {
    local exit_code=$?
    trap - EXIT INT TERM
    case "$FIXTURE" in
        /tmp/nora-redb-production-promotion-test.*) rm -rf -- "$FIXTURE" ;;
        *) echo "refusing to remove unexpected promotion-test fixture" >&2 ;;
    esac
    exit "$exit_code"
}
trap cleanup EXIT INT TERM

APP_REPO="$FIXTURE/app"
CHART_REPO="$FIXTURE/chart-repo"
CHART="$CHART_REPO/charts/nora"
BIN="$FIXTURE/bin"
LOG="$FIXTURE/calls.log"
DOCKER_CONFIG_DIR="$FIXTURE/docker-config"
SOURCE_TREE=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
IMAGE_DIGEST="sha256:$(printf 'b%.0s' {1..64})"
OTHER_DIGEST="sha256:$(printf 'c%.0s' {1..64})"
CHART_DIGEST="sha256:$(printf 'd%.0s' {1..64})"
IMAGE="docker-hub.just-ai.com/infra/artifact-nora@$IMAGE_DIGEST"
VERSION=0.5.11

mkdir -p \
    "$APP_REPO/scripts" "$APP_REPO/nora-registry" "$CHART" "$BIN" \
    "$DOCKER_CONFIG_DIR"
cp -- "$ROOT/scripts/redb-production-promotion.sh" \
    "$ROOT/scripts/verify-public-release-redb-policy.sh" "$APP_REPO/scripts/"

cat >"$APP_REPO/scripts/redb-production-gate.sh" <<EOF
#!/usr/bin/env bash
set -Eeuo pipefail
[[ "\$#" == 1 && "\$1" == verify ]]
echo "app-verify" >>"\$NORA_PROMOTION_TEST_LOG"
echo "PASS: fixture approval"
echo "image=$IMAGE"
echo "image=$IMAGE"
if [[ \${NORA_PROMOTION_TEST_CONFLICTING_IMAGE:-0} == 1 ]]; then
    echo "image=docker-hub.just-ai.com/infra/artifact-nora@sha256:$(printf 'c%.0s' {1..64})"
fi
if [[ \${NORA_PROMOTION_TEST_MALFORMED_IMAGE:-0} == 1 ]]; then
    echo "image=not-a-digest"
fi
echo "image_source_tree=$SOURCE_TREE"
EOF
cat >"$APP_REPO/nora-registry/Cargo.toml" <<'EOF'
[package]
name = "nora-registry"
version = "1.0.0"

[dependencies]
redb = { git = "https://github.com/cberner/redb", rev = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }
EOF
cat >"$APP_REPO/Cargo.toml" <<'EOF'
[workspace]
members = ["nora-registry"]
resolver = "2"
EOF
chmod +x "$APP_REPO/scripts/"*.sh
cat >"$DOCKER_CONFIG_DIR/config.json" <<'EOF'
{"auths":{"https://docker-hub.just-ai.com/v1/":{"auth":"dXNlcjpwYXNz"}}}
EOF

write_chart() {
    local annotated_digest=$1 source_tree=$2 default_digest=${3:-$1}
    cat >"$CHART/Chart.yaml" <<EOF
apiVersion: v2
name: nora
type: application
version: $VERSION
appVersion: "redb-$SOURCE_TREE"
annotations:
  nora.just-ai.com/image-digest: "$annotated_digest"
  nora.just-ai.com/source-tree: "$source_tree"
  nora.just-ai.com/release-channel: "production"
  nora.just-ai.com/production-approved: "true"
EOF
    cat >"$CHART/values.yaml" <<EOF
replicaCount: 1
image:
  repository: docker-hub.just-ai.com/infra/artifact-nora
  digest: "$default_digest"
  tag: ""
indexPersistence:
  existingClaim: ""
nodeSelector: {}
EOF
    git -C "$CHART_REPO" add charts/nora/Chart.yaml charts/nora/values.yaml
}

git -C "$CHART_REPO" init -q
git -C "$CHART_REPO" config user.name fixture
git -C "$CHART_REPO" config user.email fixture@example.invalid
write_chart "$IMAGE_DIGEST" "$SOURCE_TREE"
git -C "$CHART_REPO" commit -qm fixture

cat >"$BIN/oras" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
echo "oras $*" >>"$NORA_PROMOTION_TEST_LOG"
if [[ "$1 $2" == "repo tags" ]]; then
    [[ "$3" == docker-hub.just-ai.com/helm-charts/nora ]]
    if [[ ${NORA_PROMOTION_TEST_EXISTING_VERSION:-0} == 1 ]]; then
        echo 0.5.11
    else
        echo 0.5.10
    fi
elif [[ "$1 $2 $3" == "manifest fetch --descriptor" ]]; then
    [[ "$4" == docker-hub.just-ai.com/helm-charts/nora:0.5.11 ]]
    digest="sha256:$(printf 'd%.0s' {1..64})"
    if [[ ${NORA_PROMOTION_TEST_BAD_REMOTE_DIGEST:-0} == 1 ]]; then
        digest="sha256:$(printf 'e%.0s' {1..64})"
    fi
    printf '{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"%s"}\n' "$digest"
else
    echo "unexpected mock oras invocation: $*" >&2
    exit 2
fi
EOF

cat >"$BIN/curl" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
echo "curl-policy-read" >>"$NORA_PROMOTION_TEST_LOG"
output= config= url=
while (($#)); do
    case "$1" in
        --disable|--silent|--show-error|--fail|--tlsv1.2) shift ;;
        --proto|--config|--output)
            key=$1
            value=$2
            shift 2
            case "$key" in
                --proto) [[ "$value" == =https ]] ;;
                --config) config=$value ;;
                --output) output=$value ;;
            esac
            ;;
        https://*) url=$1; shift ;;
        *) echo "unexpected mock curl argument: $1" >&2; exit 2 ;;
    esac
done
[[ -f "$config" && -n "$output" ]]
[[ "$url" == https://docker-hub.just-ai.com/api/v2.0/projects/helm-charts/immutabletagrules ]]
grep -Fq 'user = "user:pass"' "$config"
if [[ ${NORA_PROMOTION_TEST_BAD_POLICY:-0} == 1 ]]; then
    printf '[]\n' >"$output"
elif [[ ${NORA_PROMOTION_TEST_DISABLED_POLICY:-0} == 1 ]]; then
    cat >"$output" <<'JSON'
[{"disabled":true,"action":"immutable","scope_selectors":{"repository":[{"kind":"doublestar","decoration":"repoMatches","pattern":"nora"}]},"tag_selectors":[{"kind":"doublestar","decoration":"matches","pattern":"**"}]}]
JSON
elif [[ ${NORA_PROMOTION_TEST_NULL_POLICY:-0} == 1 ]]; then
    cat >"$output" <<'JSON'
[{"disabled":null,"action":"immutable","scope_selectors":{"repository":[{"kind":"doublestar","decoration":"repoMatches","pattern":"nora"}]},"tag_selectors":[{"kind":"doublestar","decoration":"matches","pattern":"**"}]}]
JSON
elif [[ ${NORA_PROMOTION_TEST_MALFORMED_POLICY:-0} == 1 ]]; then
    cat >"$output" <<'JSON'
[{"disabled":"false","action":"immutable","scope_selectors":{"repository":[{"kind":"doublestar","decoration":"repoMatches","pattern":"nora"}]},"tag_selectors":[{"kind":"doublestar","decoration":"matches","pattern":"**"}]}]
JSON
else
    cat >"$output" <<'JSON'
[{"action":"immutable","scope_selectors":{"repository":[{"kind":"doublestar","decoration":"repoMatches","pattern":"nora"}]},"tag_selectors":[{"kind":"doublestar","decoration":"matches","pattern":"**"}]}]
JSON
fi
EOF

cat >"$BIN/helm" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
echo "helm $*" >>"$NORA_PROMOTION_TEST_LOG"
image="docker-hub.just-ai.com/infra/artifact-nora@sha256:$(printf 'b%.0s' {1..64})"
chart_digest="sha256:$(printf 'd%.0s' {1..64})"
immutable="oci://docker-hub.just-ai.com/helm-charts/nora@$chart_digest"

emit_immutable_pull() {
    local pulled=${immutable#oci://} digest=$chart_digest
    if [[ ${NORA_PROMOTION_TEST_BAD_PULL_REF:-0} == 1 ]]; then
        pulled=docker-hub.just-ai.com/helm-charts/other@$chart_digest
    fi
    if [[ ${NORA_PROMOTION_TEST_BAD_PULL_DIGEST:-0} == 1 ]]; then
        digest="sha256:$(printf 'e%.0s' {1..64})"
    fi
    printf 'Pulled: %s\nDigest: %s\n' "$pulled" "$digest"
}

render() {
    local phase=${1:-client}
    local selected_image=$image
    if [[ "$phase" == local-dry-run && ${NORA_PROMOTION_TEST_BAD_DRY_RUN:-0} == 1 ]]; then
        selected_image="docker-hub.just-ai.com/infra/artifact-nora@sha256:$(printf 'c%.0s' {1..64})"
    fi
    if [[ "$phase" == immutable-dry-run \
        && ${NORA_PROMOTION_TEST_BAD_IMMUTABLE_DRY_RUN:-0} == 1 ]]; then
        selected_image="docker-hub.just-ai.com/infra/artifact-nora@sha256:$(printf 'c%.0s' {1..64})"
    fi
    if [[ "$phase" == immutable-client \
        && ${NORA_PROMOTION_TEST_BAD_IMMUTABLE_RENDER:-0} == 1 ]]; then
        selected_image="docker-hub.just-ai.com/infra/artifact-nora@sha256:$(printf 'c%.0s' {1..64})"
    fi
    cat <<YAML
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nora
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app.kubernetes.io/name: nora
  template:
    metadata:
      labels:
        app.kubernetes.io/name: nora
    spec:
      nodeSelector:
        storage.just-ai.com/linstor-csi-node: "true"
      containers:
        - name: nora
          image: $selected_image
      volumes:
        - name: index
          persistentVolumeClaim:
            claimName: nora-index
YAML
}

require_promotion_overrides() {
    local joined=" $* "
    local values= argument previous=
    [[ "$joined" == *" --set-string image.repository=docker-hub.just-ai.com/infra/artifact-nora "* ]]
    [[ "$joined" == *" --set-string image.digest=sha256:$(printf 'b%.0s' {1..64}) "* ]]
    [[ "$joined" == *" --set-string image.tag= "* ]]
    [[ "$joined" == *" --set-string indexPersistence.existingClaim=nora-index "* ]]
    for argument in "$@"; do
        if [[ "$previous" == --values ]]; then
            values=$argument
            break
        fi
        previous=$argument
    done
    [[ -f "$values" ]]
    yq -e \
        '.nodeSelector."storage.just-ai.com/linstor-csi-node" == "true"
         and (.nodeSelector | has("kubernetes.io/hostname") | not)' \
        "$values" >/dev/null
    echo "sanitized-values" >>"$NORA_PROMOTION_TEST_LOG"
}

if [[ "$1" == package ]]; then
    chart=$2
    [[ "$3" == --destination ]]
    [[ "$chart" == /tmp/nora-redb-chart-promotion.*/chart-source/charts/nora ]]
    [[ -f "$chart/Chart.yaml" && -f "$chart/values.yaml" ]]
    grep -Fq 'version: 0.5.11' "$chart/Chart.yaml"
    mkdir -p "$4"
    : >"$4/nora-0.5.11.tgz"
    echo "Successfully packaged chart and saved it to: $4/nora-0.5.11.tgz"
elif [[ "$1" == push ]]; then
    [[ "$2" == */nora-0.5.11.tgz && "$3" == oci://docker-hub.just-ai.com/helm-charts ]]
    echo "Pushed: docker-hub.just-ai.com/helm-charts/nora:0.5.11"
    echo "Digest: $chart_digest"
elif [[ "$1 $2" == "show chart" ]]; then
    [[ "$3" == "$immutable" ]]
    emit_immutable_pull
    cat "$NORA_PROMOTION_TEST_CHART/Chart.yaml"
elif [[ "$1 $2" == "show values" ]]; then
    [[ "$3" == "$immutable" ]]
    emit_immutable_pull
    cat "$NORA_PROMOTION_TEST_CHART/values.yaml"
elif [[ "$1" == --kube-context ]]; then
    [[ "$2 $3 $4 $5 $6 $7 $8" == "testcloud-k8s -n nora get values nora -o" && "$9" == yaml ]]
    cat <<'YAML'
config:
  storage:
    mode: s3
nodeSelector:
  kubernetes.io/hostname: dcd-1
YAML
elif [[ "$1" == template ]]; then
    [[ "$2" == nora && ("$3" == "$immutable" || "$3" == */nora-0.5.11.tgz) ]]
    require_promotion_overrides "$@"
    phase=local-client
    if [[ "$3" == "$immutable" ]]; then
        phase=immutable-client
        emit_immutable_pull
    fi
    render "$phase"
elif [[ "$1" == upgrade ]]; then
    [[ "$2" == nora && ("$3" == "$immutable" || "$3" == */nora-0.5.11.tgz) ]]
    require_promotion_overrides "$@"
    if [[ " $* " == *" --dry-run=server "* ]]; then
        [[ " $* " == *" --reset-values "* ]]
        phase=local-dry-run
        if [[ "$3" == "$immutable" ]]; then
            phase=immutable-dry-run
            emit_immutable_pull
        fi
        manifest=$(render "$phase")
        if [[ "$phase" == immutable-dry-run \
            && ${NORA_PROMOTION_TEST_EXTRA_DRY_RUN_OUTPUT:-0} == 1 ]]; then
            echo unexpected-prefix
        fi
        jq -n --arg manifest "$manifest" '{manifest: $manifest}'
    else
        [[ "$3" == "$immutable" ]]
        [[ " $* " == *" --reset-values "* ]]
        [[ " $* " == *" --rollback-on-failure "* && " $* " == *" --wait "* ]]
        echo "Release nora upgraded"
    fi
else
    echo "unexpected mock helm invocation: $*" >&2
    exit 2
fi
EOF

cat >"$BIN/kubectl" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
echo "kubectl $*" >>"$NORA_PROMOTION_TEST_LOG"
[[ "$1 $2 $3 $4" == "--context testcloud-k8s -n nora" ]]
shift 4
if [[ "$1 $2 $3 $4 $5" == "get deployment nora -o json" ]]; then
    deployed_digest=$(printf 'b%.0s' {1..64})
    if [[ ${NORA_PROMOTION_TEST_BAD_DEPLOYMENT:-0} == 1 ]]; then
        deployed_digest=$(printf 'c%.0s' {1..64})
    fi
    jq -n --arg digest "$deployed_digest" '{
      spec: {
        replicas: 1,
        strategy: {type: "Recreate"},
        selector: {matchLabels: {"app.kubernetes.io/name": "nora"}},
        template: {spec: {
          nodeSelector: {"storage.just-ai.com/linstor-csi-node": "true"},
          containers: [{
            name: "nora",
            image: ("docker-hub.just-ai.com/infra/artifact-nora@sha256:" + $digest)
          }]
        }}
      }
    }'
elif [[ "$1 $2" == "get pods" && "$3" == -l \
    && "$4" == app.kubernetes.io/name=nora && "$5 $6" == "-o json" ]]; then
    image_id="docker-pullable://docker-hub.just-ai.com/infra/artifact-nora@sha256:$(printf 'b%.0s' {1..64})"
    if [[ ${NORA_PROMOTION_TEST_IMAGE_ID_STYLE:-} == containerd ]]; then
        image_id="containerd://sha256:$(printf 'b%.0s' {1..64})"
    fi
    if [[ ${NORA_PROMOTION_TEST_BAD_IMAGE_ID:-0} == 1 ]]; then
        image_id="containerd://sha256:$(printf 'c%.0s' {1..64})"
    fi
    pod_digest=$(printf 'b%.0s' {1..64})
    if [[ $(printenv NORA_PROMOTION_TEST_BAD_POD_SPEC_IMAGE 2>/dev/null || true) == 1 ]]; then
        pod_digest=$(printf 'c%.0s' {1..64})
    fi
    jq -n --arg image_id "$image_id" --arg pod_digest "$pod_digest" '{
      items: [{
        metadata: {deletionTimestamp: null},
        spec: {
          containers: [{
            name: "nora",
            image: ("docker-hub.just-ai.com/infra/artifact-nora@sha256:" + $pod_digest)
          }]
        },
        status: {
          phase: "Running",
          conditions: [{type: "Ready", status: "True"}],
          containerStatuses: [{
            name: "nora",
            ready: true,
            image: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            imageID: $image_id
          }]
        }
      }]
    }'
else
    echo "unexpected mock kubectl invocation: $*" >&2
    exit 2
fi
EOF

chmod +x "$BIN/curl" "$BIN/helm" "$BIN/kubectl" "$BIN/oras"
export PATH="$BIN:$PATH"
export DOCKER_CONFIG="$DOCKER_CONFIG_DIR"
export NORA_PROMOTION_TEST_LOG="$LOG"
export NORA_PROMOTION_TEST_CHART="$CHART"

expect_failure() {
    local expected=$1
    shift
    local output status
    set +e
    output=$("$@" 2>&1)
    status=$?
    set -e
    ((status != 0)) || {
        echo "expected promotion failure: $expected" >&2
        exit 1
    }
    [[ "$output" == *"$expected"* ]] || {
        echo "promotion failed for an unexpected reason" >&2
        echo "$output" >&2
        exit 1
    }
}

require_log_absent() {
    local pattern=$1
    if grep -Eq -- "$pattern" "$LOG"; then
        echo "unexpected mutating preflight call matched: $pattern" >&2
        exit 1
    fi
}

: >"$LOG"
"$APP_REPO/scripts/redb-production-promotion.sh" preflight "$CHART" >"$FIXTURE/preflight.out"
grep -Fxq 'mode=preflight' "$FIXTURE/preflight.out"
grep -Fxq "image=$IMAGE" "$FIXTURE/preflight.out"
grep -Fxq "image_source_tree=$SOURCE_TREE" "$FIXTURE/preflight.out"
grep -Eq '^chart_package_sha256=[0-9a-f]{64}$' "$FIXTURE/preflight.out"
require_log_absent '^helm push '
require_log_absent '^oras manifest fetch '
require_log_absent '^kubectl '
require_log_absent '--rollback-on-failure'
grep -Fq 'helm template nora /tmp/nora-redb-chart-promotion.' "$LOG"
grep -Fq 'helm upgrade nora /tmp/nora-redb-chart-promotion.' "$LOG"
grep -Fq 'sanitized-values' "$LOG"
[[ $(grep -c '^app-verify$' "$LOG") == 1 ]]
[[ $(head -n 1 "$LOG") == app-verify ]]

expect_failure "Harbor must have one enabled immutable rule" \
    env NORA_PROMOTION_TEST_BAD_POLICY=1 \
    "$APP_REPO/scripts/redb-production-promotion.sh" preflight "$CHART"
expect_failure "Harbor must have one enabled immutable rule" \
    env NORA_PROMOTION_TEST_DISABLED_POLICY=1 \
    "$APP_REPO/scripts/redb-production-promotion.sh" preflight "$CHART"
expect_failure "Harbor must have one enabled immutable rule" \
    env NORA_PROMOTION_TEST_NULL_POLICY=1 \
    "$APP_REPO/scripts/redb-production-promotion.sh" preflight "$CHART"
expect_failure "Harbor must have one enabled immutable rule" \
    env NORA_PROMOTION_TEST_MALFORMED_POLICY=1 \
    "$APP_REPO/scripts/redb-production-promotion.sh" preflight "$CHART"
expect_failure "approved image reported conflicting values" \
    env NORA_PROMOTION_TEST_CONFLICTING_IMAGE=1 \
    "$APP_REPO/scripts/redb-production-promotion.sh" preflight "$CHART"
expect_failure "approved image reported a malformed value" \
    env NORA_PROMOTION_TEST_MALFORMED_IMAGE=1 \
    "$APP_REPO/scripts/redb-production-promotion.sh" preflight "$CHART"

printf 'untracked chart input\n' >"$CHART/untracked.txt"
expect_failure "chart directory has untracked inputs" \
    "$APP_REPO/scripts/redb-production-promotion.sh" preflight "$CHART"
mv -- "$CHART/untracked.txt" "$FIXTURE/untracked.txt"

write_chart "$OTHER_DIGEST" "$SOURCE_TREE"
expect_failure "image-digest annotation does not match" \
    "$APP_REPO/scripts/redb-production-promotion.sh" preflight "$CHART"
write_chart "$IMAGE_DIGEST" bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
expect_failure "source-tree annotation does not match" \
    "$APP_REPO/scripts/redb-production-promotion.sh" preflight "$CHART"
write_chart "$IMAGE_DIGEST" "$SOURCE_TREE" "$OTHER_DIGEST"
expect_failure "default image digest does not match" \
    "$APP_REPO/scripts/redb-production-promotion.sh" preflight "$CHART"
write_chart "$IMAGE_DIGEST" "$SOURCE_TREE"

expect_failure "chart version already exists" env NORA_PROMOTION_TEST_EXISTING_VERSION=1 \
    "$APP_REPO/scripts/redb-production-promotion.sh" preflight "$CHART"
expect_failure "local server dry-run Deployment does not preserve" \
    env NORA_PROMOTION_TEST_BAD_DRY_RUN=1 \
    "$APP_REPO/scripts/redb-production-promotion.sh" preflight "$CHART"
expect_failure "remote chart manifest digest does not match" \
    env NORA_PROMOTION_TEST_BAD_REMOTE_DIGEST=1 \
    "$APP_REPO/scripts/redb-production-promotion.sh" apply "$CHART"
expect_failure "immutable chart metadata read-back did not report the exact immutable chart pull" \
    env NORA_PROMOTION_TEST_BAD_PULL_REF=1 \
    "$APP_REPO/scripts/redb-production-promotion.sh" apply "$CHART"
expect_failure "immutable chart metadata read-back did not report the exact immutable chart pull" \
    env NORA_PROMOTION_TEST_BAD_PULL_DIGEST=1 \
    "$APP_REPO/scripts/redb-production-promotion.sh" apply "$CHART"
expect_failure "immutable server dry-run returned an unexpected JSON payload" \
    env NORA_PROMOTION_TEST_EXTRA_DRY_RUN_OUTPUT=1 \
    "$APP_REPO/scripts/redb-production-promotion.sh" apply "$CHART"
expect_failure "immutable server dry-run Deployment does not preserve" \
    env NORA_PROMOTION_TEST_BAD_IMMUTABLE_DRY_RUN=1 \
    "$APP_REPO/scripts/redb-production-promotion.sh" apply "$CHART"
expect_failure "immutable client render Deployment does not preserve" \
    env NORA_PROMOTION_TEST_BAD_IMMUTABLE_RENDER=1 \
    "$APP_REPO/scripts/redb-production-promotion.sh" apply "$CHART"

: >"$LOG"
"$APP_REPO/scripts/redb-production-promotion.sh" apply "$CHART" >"$FIXTURE/apply.out"
grep -Fxq 'deployment=testcloud-k8s/nora/nora' "$FIXTURE/apply.out"
grep -Fxq "chart=oci://docker-hub.just-ai.com/helm-charts/nora@$CHART_DIGEST" "$FIXTURE/apply.out"
grep -Fq 'kubectl --context testcloud-k8s -n nora get pods -l app.kubernetes.io/name=nora -o json' "$LOG"
env NORA_PROMOTION_TEST_IMAGE_ID_STYLE=containerd \
    "$APP_REPO/scripts/redb-production-promotion.sh" apply "$CHART" >/dev/null

expect_failure "deployed Deployment does not match" \
    env NORA_PROMOTION_TEST_BAD_DEPLOYMENT=1 \
    "$APP_REPO/scripts/redb-production-promotion.sh" apply "$CHART"

expect_failure "running Pod does not match the approved image and imageID" \
    env NORA_PROMOTION_TEST_BAD_POD_SPEC_IMAGE=1 \
    "$APP_REPO/scripts/redb-production-promotion.sh" apply "$CHART"

expect_failure "running Pod does not match the approved image and imageID" \
    env NORA_PROMOTION_TEST_BAD_IMAGE_ID=1 \
    "$APP_REPO/scripts/redb-production-promotion.sh" apply "$CHART"

expect_failure "exact-git redb" \
    "$APP_REPO/scripts/verify-public-release-redb-policy.sh"
cat >"$APP_REPO/nora-registry/Cargo.toml" <<'EOF'
[package]
name = "nora-registry"
version = "1.0.0"

[dependencies]
redb = "4.2.0"
EOF
"$APP_REPO/scripts/verify-public-release-redb-policy.sh" \
    | grep -Fq 'PASS: public release uses stable crates.io redb 4.2.0'
cat >"$APP_REPO/nora-registry/Cargo.toml" <<'EOF'
[package]
name = "nora-registry"
version = "1.0.0"

[dependencies]
redb = "4.2.0"

[patch.crates-io]
redb = { git = "https://github.com/cberner/redb", rev = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }
EOF
expect_failure "Cargo patch override" \
    "$APP_REPO/scripts/verify-public-release-redb-policy.sh"
cat >"$APP_REPO/nora-registry/Cargo.toml" <<'EOF'
[package]
name = "nora-registry"
version = "1.0.0"

[dependencies]
redb = "4.2.0"
EOF
cat >"$APP_REPO/Cargo.toml" <<'EOF'
[workspace]
members = ["nora-registry"]
resolver = "2"

[patch.crates-io]
redb = { git = "https://github.com/cberner/redb", rev = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }
EOF
expect_failure "Cargo patch override" \
    "$APP_REPO/scripts/verify-public-release-redb-policy.sh"
cat >"$APP_REPO/Cargo.toml" <<'EOF'
[workspace]
members = ["nora-registry"]
resolver = "2"
EOF
cat >"$APP_REPO/nora-registry/Cargo.toml" <<'EOF'
[package]
name = "nora-registry"
version = "1.0.0"

[dependencies]
redb = "4.2.0"

[target.'cfg(unix)'.dependencies]
redb = { git = "https://github.com/cberner/redb", rev = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }
EOF
expect_failure "target 'cfg(unix)' has a redb dependency override" \
    "$APP_REPO/scripts/verify-public-release-redb-policy.sh"

python3 - "$ROOT/.github/workflows/release.yml" <<'PY'
import pathlib
import sys

workflow = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
checkout = workflow.index("actions/checkout@")
policy = workflow.index("scripts/verify-public-release-redb-policy.sh")
setup = workflow.index("Install ORAS")
first_image_build = workflow.index("docker/build-push-action@")
if not checkout < policy < setup < first_image_build:
    raise SystemExit("public release policy is not before release setup/image build")
PY

echo "PASS: connected app/chart promotion rejects identity, read-back, dry-run, and imageID drift"
