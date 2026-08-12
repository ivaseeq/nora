#!/usr/bin/env bash
# Connected, fail-closed promotion gate for the qualified exact-git redb build.
# It consumes the app-side approval, binds one final chart to that exact image,
# publishes the chart only under a previously unused OCI version, and uses the
# immutable chart manifest digest for every render, dry-run, and apply action.

set -Eeuo pipefail
umask 077

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
APP_GATE="$ROOT/scripts/redb-production-gate.sh"
CHART_REPOSITORY=docker-hub.just-ai.com/helm-charts/nora
IMAGE_REPOSITORY=docker-hub.just-ai.com/infra/artifact-nora
KUBE_CONTEXT=testcloud-k8s
NAMESPACE=nora
RELEASE=nora
DEPLOYMENT=nora
CONTAINER=nora
INDEX_CLAIM=nora-index
CAPABILITY_KEY=storage.just-ai.com/linstor-csi-node
CAPABILITY_VALUE=true
HARBOR_API=https://docker-hub.just-ai.com/api/v2.0
HARBOR_POLICY_CHECKS=0
RUN_ROOT=

usage() {
    cat >&2 <<'EOF'
usage:
  redb-production-promotion.sh preflight <nora-chart-directory>
  redb-production-promotion.sh apply <nora-chart-directory>

preflight is read-only: it verifies the argument-free app approval, requires a
previously unused chart version, packages the frozen staged chart privately,
renders it, and performs a server-side Helm dry-run against testcloud-k8s/nora.

apply performs the same checks, pushes the unique chart version, reads it back
by manifest digest, repeats render and server dry-run by that immutable digest,
then upgrades release nora. It verifies both the Deployment image and the
running Pod container imageID against the approved Harbor digest.
EOF
    exit 2
}

fail() {
    echo "redb production promotion blocked: $*" >&2
    exit 1
}

cleanup() {
    local exit_code=$?
    trap - EXIT INT TERM
    if [[ -n "$RUN_ROOT" ]]; then
        case "$RUN_ROOT" in
            /tmp/nora-redb-chart-promotion.*) rm -rf -- "$RUN_ROOT" ;;
            *) echo "redb production promotion blocked: refusing to remove unexpected temporary path" >&2 ;;
        esac
    fi
    exit "$exit_code"
}
trap cleanup EXIT INT TERM

require_commands() {
    local required
    for required in "$@"; do
        command -v "$required" >/dev/null \
            || fail "required command is missing: $required"
    done
}

require_one_line() {
    local label=$1 pattern=$2 file=$3
    local -a matches
    mapfile -t matches < <(grep -E -- "$pattern" "$file" || true)
    ((${#matches[@]} == 1)) \
        || fail "$label was not reported exactly once"
    printf '%s\n' "${matches[0]}"
}

require_consistent_line() {
    local label=$1 key=$2 pattern=$3 file=$4 candidate
    local -a matches
    mapfile -t matches < <(grep -E -- "^${key}=" "$file" || true)
    ((${#matches[@]} >= 1)) \
        || fail "$label was not reported"
    for candidate in "${matches[@]}"; do
        [[ "$candidate" =~ $pattern ]] \
            || fail "$label reported a malformed value"
    done
    for candidate in "${matches[@]:1}"; do
        [[ "$candidate" == "${matches[0]}" ]] \
            || fail "$label reported conflicting values"
    done
    printf '%s\n' "${matches[0]}"
}

extract_immutable_helm_output() {
    local raw=$1 expected_ref=$2 expected_digest=$3 output=$4 label=$5
    local pulled digest
    pulled=$(sed -n '1p' "$raw")
    digest=$(sed -n '2p' "$raw")
    [[ "$pulled" == "Pulled: ${expected_ref#oci://}" \
        && "$digest" == "Digest: $expected_digest" ]] \
        || fail "$label did not report the exact immutable chart pull"
    sed '1,2d' "$raw" >"$output"
    [[ -s "$output" ]] \
        || fail "$label returned no chart output after its immutable pull identity"
}

require_harbor_immutability() {
    local docker_config_file curl_config response
    HARBOR_POLICY_CHECKS=$((HARBOR_POLICY_CHECKS + 1))
    if [[ -n ${DOCKER_CONFIG:-} ]]; then
        docker_config_file="$DOCKER_CONFIG/config.json"
    else
        docker_config_file=$(python3 -c \
            'import pathlib; print(pathlib.Path.home() / ".docker" / "config.json")')
    fi
    [[ -f "$docker_config_file" && ! -L "$docker_config_file" ]] \
        || fail "inline Docker auth config is required for the Harbor policy check"
    docker_config_file=$(realpath -e -- "$docker_config_file") \
        || fail "Docker auth config cannot be resolved"
    curl_config="$RUN_ROOT/harbor-curl-$HARBOR_POLICY_CHECKS.conf"
    python3 - "$docker_config_file" "$curl_config" <<'PY'
import base64
import binascii
import json
import os
import pathlib
import sys
from urllib.parse import urlsplit

source, destination = sys.argv[1:]
try:
    config = json.loads(pathlib.Path(source).read_text(encoding="utf-8"))
except (OSError, UnicodeError, json.JSONDecodeError) as error:
    raise SystemExit(f"cannot read Docker auth config: {error}")

credentials = set()
for registry, entry in config.get("auths", {}).items():
    if not isinstance(registry, str) or not isinstance(entry, dict):
        continue
    parsed = urlsplit(registry if "://" in registry else f"//{registry}")
    if parsed.hostname != "docker-hub.just-ai.com":
        continue
    encoded = entry.get("auth")
    if not isinstance(encoded, str) or not encoded:
        continue
    try:
        decoded = base64.b64decode(encoded, validate=True).decode("utf-8")
    except (binascii.Error, UnicodeError) as error:
        raise SystemExit(f"invalid inline Docker auth for Harbor: {error}")
    username, separator, password = decoded.partition(":")
    if not separator or not username or not password or any(
        ord(character) < 32 for character in decoded
    ):
        raise SystemExit("invalid inline Docker username/password for Harbor")
    credentials.add(decoded)
if len(credentials) != 1:
    raise SystemExit(
        "exactly one inline Docker username/password is required for Harbor; "
        "credential helpers are not accepted by this gate"
    )
credential = credentials.pop().replace("\\", "\\\\").replace('"', '\\"')
descriptor = os.open(
    destination,
    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
    0o600,
)
with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
    handle.write(f'user = "{credential}"\n')
PY
    response="$RUN_ROOT/harbor-immutability-$HARBOR_POLICY_CHECKS.json"
    curl --disable --silent --show-error --fail \
        --proto '=https' --tlsv1.2 \
        --config "$curl_config" --output "$response" \
        "$HARBOR_API/projects/helm-charts/immutabletagrules" \
        || fail "cannot read the authenticated Harbor tag-immutability policy"
    jq -e '
        type == "array"
        and ([.[]
              # Harbor serializes the active false value with omitempty, so
              # an enabled rule may omit disabled. Reject explicit null and
              # every non-boolean value instead of treating them as enabled.
              | select(((has("disabled") | not) or (.disabled == false))
                       and .action == "immutable")
              | select((.scope_selectors | keys) == ["repository"])
              | select(.scope_selectors.repository | length == 1)
              | select(.scope_selectors.repository[0].kind == "doublestar")
              | select(.scope_selectors.repository[0].decoration == "repoMatches")
              | select(.scope_selectors.repository[0].pattern == "nora")
              | select(.tag_selectors | length == 1)
              | select(.tag_selectors[0].kind == "doublestar")
              | select(.tag_selectors[0].decoration == "matches")
              | select(.tag_selectors[0].pattern == "**")]
             | length == 1)
    ' "$response" >/dev/null \
        || fail "Harbor must have one enabled immutable rule for repository nora and all tags"
}

yaml_scalar() {
    local expression=$1 file=$2
    yq -er "$expression | select(tag != \"!!null\")" "$file"
}

require_chart_identity() {
    local chart_yaml=$1 values_yaml=$2 expected_tree=$3 expected_digest=$4
    local name version source_tree annotated_digest repository default_digest tag
    local channel approved

    name=$(yaml_scalar '.name' "$chart_yaml") \
        || fail "chart name is missing"
    version=$(yaml_scalar '.version' "$chart_yaml") \
        || fail "chart version is missing"
    source_tree=$(yaml_scalar '.annotations."nora.just-ai.com/source-tree"' "$chart_yaml") \
        || fail "chart source-tree annotation is missing"
    annotated_digest=$(yaml_scalar '.annotations."nora.just-ai.com/image-digest"' "$chart_yaml") \
        || fail "chart image-digest annotation is missing"
    channel=$(yaml_scalar '.annotations."nora.just-ai.com/release-channel"' "$chart_yaml") \
        || fail "chart release-channel annotation is missing"
    approved=$(yaml_scalar '.annotations."nora.just-ai.com/production-approved"' "$chart_yaml") \
        || fail "chart production-approved annotation is missing"
    repository=$(yaml_scalar '.image.repository' "$values_yaml") \
        || fail "default image repository is missing"
    default_digest=$(yaml_scalar '.image.digest' "$values_yaml") \
        || fail "default image digest is missing"
    tag=$(yq -er '.image.tag | select(tag == "!!str")' "$values_yaml") \
        || fail "default image tag must be a string"

    [[ "$name" == nora ]] || fail "chart name is not nora"
    [[ "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] \
        || fail "chart version is not one canonical release version"
    [[ "$source_tree" == "$expected_tree" ]] \
        || fail "chart source-tree annotation does not match the approved app tree"
    [[ "$annotated_digest" == "$expected_digest" ]] \
        || fail "chart image-digest annotation does not match the approved image"
    [[ "$repository" == "$IMAGE_REPOSITORY" ]] \
        || fail "chart default image repository does not match the approved repository"
    [[ "$default_digest" == "$expected_digest" ]] \
        || fail "chart default image digest does not match the approved image"
    [[ -z "$tag" ]] \
        || fail "chart default image tag must be empty when a production digest is set"
    [[ "$channel" == production && "$approved" == true ]] \
        || fail "chart is not marked as production-approved"

    printf '%s\n' "$version"
}

require_rendered_deployment() {
    local manifest=$1 expected_image=$2 phase=$3
    local deployments deployment
    deployments=$(yq -o=json -I=0 \
        'select(.kind == "Deployment" and .metadata.name == "nora")' \
        "$manifest") || fail "$phase manifest cannot be parsed"
    [[ -n "$deployments" && $(wc -l <<<"$deployments") -eq 1 ]] \
        || fail "$phase did not contain exactly one nora Deployment"
    deployment=$deployments
    jq -e \
        --arg image "$expected_image" \
        --arg key "$CAPABILITY_KEY" \
        --arg value "$CAPABILITY_VALUE" \
        --arg claim "$INDEX_CLAIM" '
        .spec.replicas == 1
        and .spec.strategy.type == "Recreate"
        and (.spec.template.spec.nodeSelector[$key] == $value)
        and ((.spec.template.spec.nodeSelector | has("kubernetes.io/hostname")) | not)
        and ([.spec.template.spec.containers[] | select(.name == "nora") | .image] == [$image])
        and ([.spec.template.spec.volumes[]?
              | select(.name == "index")
              | .persistentVolumeClaim.claimName] == [$claim])
    ' <<<"$deployment" >/dev/null \
        || fail "$phase Deployment does not preserve the approved image, singleton/index safety, and capability selector"
}

verify_live_result() {
    local expected_image=$1 expected_digest=$2 deployment_json pods_json selector
    local matching

    deployment_json="$RUN_ROOT/deployment.json"
    pods_json="$RUN_ROOT/pods.json"
    kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" \
        get deployment "$DEPLOYMENT" -o json >"$deployment_json" \
        || fail "cannot read the deployed Deployment"
    jq -e \
        --arg image "$expected_image" \
        --arg key "$CAPABILITY_KEY" \
        --arg value "$CAPABILITY_VALUE" '
        .spec.replicas == 1
        and .spec.strategy.type == "Recreate"
        and (.spec.template.spec.nodeSelector[$key] == $value)
        and ((.spec.template.spec.nodeSelector | has("kubernetes.io/hostname")) | not)
        and ([.spec.template.spec.containers[] | select(.name == "nora") | .image] == [$image])
    ' "$deployment_json" >/dev/null \
        || fail "deployed Deployment does not match the approved image and capability selector"
    selector=$(jq -er '
        .spec.selector.matchLabels
        | to_entries
        | sort_by(.key)
        | map(.key + "=" + .value)
        | join(",")
        | select(length > 0)
    ' "$deployment_json") || fail "Deployment selector is missing"
    kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" \
        get pods -l "$selector" -o json >"$pods_json" \
        || fail "cannot read the promoted Pod"
    matching=$(jq -r --arg container "$CONTAINER" \
        '[.items[]
          | select(.metadata.deletionTimestamp == null)
          | .status.containerStatuses[]?
          | select(.name == $container)] | length' "$pods_json") \
        || fail "promoted Pod status cannot be parsed"
    [[ "$matching" == 1 ]] \
        || fail "expected exactly one active promoted Pod container status"
    jq -e \
        --arg container "$CONTAINER" \
        --arg image "$expected_image" \
        --arg digest "$expected_digest" '
        [.items[]
         | select(.metadata.deletionTimestamp == null)
         | select([.spec.containers[]?
                   | select(.name == $container)
                   | .image] == [$image])
         | select(.status.phase == "Running")
         | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))
         | .status.containerStatuses[]?
         | select(.name == $container)
         | select(.ready == true)
         | select(.imageID == $digest
                  or (.imageID | endswith("@" + $digest))
                  or (.imageID | endswith("/" + $digest)))]
        | length == 1
    ' "$pods_json" >/dev/null \
        || fail "running Pod does not match the approved image and imageID"
}

run_promotion() {
    (($# == 2)) || usage
    local mode=$1 requested_chart=$2 chart_dir chart_repo chart_relative
    local chart_source_tree chart_snapshot
    local verify_log image_line tree_line approved_image approved_tree approved_digest
    local chart_yaml values_yaml version tags package_dir package chart_push_log
    local push_line pushed_digest descriptor remote_digest immutable_chart
    local remote_chart_raw remote_chart_yaml remote_values_raw remote_values_yaml
    local live_values promoted_values template_raw template_manifest
    local dry_run_raw dry_run_json dry_run_manifest
    local -a common_values

    [[ "$mode" == preflight || "$mode" == apply ]] || usage
    require_commands curl find git grep helm jq kubectl oras python3 realpath sha256sum tar wc yq
    [[ -x "$APP_GATE" ]] || fail "argument-free app verification gate is missing"

    RUN_ROOT=$(mktemp -d /tmp/nora-redb-chart-promotion.XXXXXXXX)
    verify_log="$RUN_ROOT/app-verify.log"
    "$APP_GATE" verify >"$verify_log" \
        || fail "argument-free app approval verification failed"
    image_line=$(require_consistent_line "approved image" image \
        '^image=docker-hub\.just-ai\.com/infra/artifact-nora@sha256:[0-9a-f]{64}$' \
        "$verify_log")
    tree_line=$(require_one_line "approved app tree" \
        '^image_source_tree=[0-9a-f]{40}$' "$verify_log")
    approved_image=${image_line#image=}
    approved_tree=${tree_line#image_source_tree=}
    approved_digest=${approved_image##*@}
    [[ "$approved_image" == "$IMAGE_REPOSITORY@$approved_digest" ]] \
        || fail "app approval did not derive the canonical Harbor image"

    chart_dir=$(realpath -e -- "$requested_chart") \
        || fail "chart directory does not exist"
    [[ -d "$chart_dir" && ! -L "$requested_chart" ]] \
        || fail "chart input must be one physical directory"
    chart_repo=$(git -C "$chart_dir" rev-parse --show-toplevel 2>/dev/null) \
        || fail "chart directory is not in one Git repository"
    chart_repo=$(realpath -e -- "$chart_repo")
    case "$chart_dir/" in
        "$chart_repo"/*) ;;
        *) fail "chart directory escaped its Git repository" ;;
    esac
    chart_relative=${chart_dir#"$chart_repo"/}
    git -C "$chart_repo" diff --quiet -- "$chart_relative" \
        || fail "chart directory has unstaged inputs"
    [[ -z $(git -C "$chart_repo" ls-files --others --exclude-standard -- "$chart_relative") ]] \
        || fail "chart directory has untracked inputs"
    [[ -z $(git -C "$chart_repo" ls-files --others --ignored --exclude-standard -- "$chart_relative") ]] \
        || fail "chart directory has ignored inputs that Helm could package"
    [[ -z $(git -C "$chart_repo" ls-files --unmerged -- "$chart_relative") ]] \
        || fail "chart directory has unresolved index entries"
    [[ -z $(find "$chart_dir" -type l -print -quit) ]] \
        || fail "chart directory contains a symlink"
    chart_source_tree=$(git -C "$chart_repo" write-tree) \
        || fail "chart staged tree cannot be frozen"
    chart_snapshot="$RUN_ROOT/chart-source"
    mkdir -- "$chart_snapshot"
    git -C "$chart_repo" archive "$chart_source_tree" -- "$chart_relative" \
        | tar -xf - -C "$chart_snapshot" \
        || fail "chart staged tree cannot be archived"
    chart_snapshot="$chart_snapshot/$chart_relative"
    [[ -d "$chart_snapshot" && -z $(find "$chart_snapshot" -type l -print -quit) ]] \
        || fail "archived chart is missing or contains a symlink"
    chart_yaml="$chart_snapshot/Chart.yaml"
    values_yaml="$chart_snapshot/values.yaml"
    [[ -f "$chart_yaml" && ! -L "$chart_yaml" \
        && -f "$values_yaml" && ! -L "$values_yaml" ]] \
        || fail "chart metadata and defaults must be regular files"
    version=$(require_chart_identity \
        "$chart_yaml" "$values_yaml" "$approved_tree" "$approved_digest")
    require_harbor_immutability

    tags="$RUN_ROOT/remote-tags.txt"
    oras repo tags "$CHART_REPOSITORY" >"$tags" \
        || fail "cannot enumerate existing chart versions"
    if grep -Fxq -- "$version" "$tags"; then
        fail "chart version already exists in the OCI repository"
    fi

    package_dir="$RUN_ROOT/package"
    mkdir -- "$package_dir"
    helm package "$chart_snapshot" --destination "$package_dir" >"$RUN_ROOT/package.log" \
        || fail "chart packaging failed"
    package="$package_dir/nora-$version.tgz"
    [[ -f "$package" && ! -L "$package" ]] \
        || fail "chart package name does not match its version"
    [[ $(find "$package_dir" -maxdepth 1 -type f -name '*.tgz' | wc -l) -eq 1 ]] \
        || fail "chart packaging did not produce exactly one archive"
    if ! git -C "$chart_repo" diff --quiet -- "$chart_relative" \
        || [[ $(git -C "$chart_repo" write-tree) != "$chart_source_tree" ]]; then
        fail "chart staged tree changed before publication"
    fi

    live_values="$RUN_ROOT/live-values.yaml"
    helm --kube-context "$KUBE_CONTEXT" -n "$NAMESPACE" \
        get values "$RELEASE" -o yaml >"$live_values" \
        || fail "cannot read the existing release values"
    promoted_values="$RUN_ROOT/promoted-values.yaml"
    CAPABILITY_KEY="$CAPABILITY_KEY" CAPABILITY_VALUE="$CAPABILITY_VALUE" \
        yq -e '
            del(.nodeSelector."kubernetes.io/hostname")
            | .nodeSelector[strenv(CAPABILITY_KEY)] = strenv(CAPABILITY_VALUE)
        ' "$live_values" >"$promoted_values" \
        || fail "cannot create sanitized promotion values"
    yq -e \
        '.nodeSelector."storage.just-ai.com/linstor-csi-node" == "true"
         and (.nodeSelector | has("kubernetes.io/hostname") | not)' \
        "$promoted_values" >/dev/null \
        || fail "promotion values did not remove the old hostname selector"
    common_values=(
        --set-string "image.repository=$IMAGE_REPOSITORY"
        --set-string "image.digest=$approved_digest"
        --set-string 'image.tag='
        --set-string "indexPersistence.existingClaim=$INDEX_CLAIM"
    )

    template_manifest="$RUN_ROOT/template.yaml"
    helm template "$RELEASE" "$package" \
        --kube-context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
        --values "$promoted_values" "${common_values[@]}" >"$template_manifest" \
        || fail "local frozen chart render failed"
    require_rendered_deployment "$template_manifest" "$approved_image" "local client render"

    dry_run_json="$RUN_ROOT/server-dry-run.json"
    helm upgrade "$RELEASE" "$package" \
        --kube-context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
        --dry-run=server --hide-secret --output json \
        --reset-values --values "$promoted_values" \
        "${common_values[@]}" >"$dry_run_json" \
        || fail "local frozen chart server dry-run failed"
    dry_run_manifest="$RUN_ROOT/server-dry-run.yaml"
    jq -er '.manifest | select(type == "string" and length > 0)' \
        "$dry_run_json" >"$dry_run_manifest" \
        || fail "server dry-run did not return a rendered manifest"
    require_rendered_deployment "$dry_run_manifest" "$approved_image" "local server dry-run"

    if [[ "$mode" == preflight ]]; then
        echo "PASS: connected redb app/chart promotion preflight is read-only and green"
        echo "mode=preflight"
        echo "image=$approved_image"
        echo "image_source_tree=$approved_tree"
        echo "chart_source_tree=$chart_source_tree"
        echo "chart_version=$version"
        echo "chart_package_sha256=$(sha256sum "$package" | awk '{print $1}')"
        echo "next=run apply once; it repeats preflight before publishing the unique chart version"
        return
    fi

    require_harbor_immutability
    oras repo tags "$CHART_REPOSITORY" >"$tags" \
        || fail "cannot recheck existing chart versions before publication"
    if grep -Fxq -- "$version" "$tags"; then
        fail "chart version appeared before OCI publication"
    fi

    chart_push_log="$RUN_ROOT/chart-push.log"
    helm push "$package" "oci://docker-hub.just-ai.com/helm-charts" \
        >"$chart_push_log" 2>&1 \
        || fail "chart OCI push failed"
    push_line=$(require_one_line "pushed chart digest" \
        '^Digest: sha256:[0-9a-f]{64}$' "$chart_push_log")
    pushed_digest=${push_line#Digest: }

    descriptor="$RUN_ROOT/remote-chart-descriptor.json"
    oras manifest fetch --descriptor "$CHART_REPOSITORY:$version" >"$descriptor" \
        || fail "cannot read the pushed chart manifest back"
    remote_digest=$(jq -er '
        select(.mediaType == "application/vnd.oci.image.manifest.v1+json")
        | .digest
        | select(test("^sha256:[0-9a-f]{64}$"))
    ' "$descriptor") || fail "remote chart descriptor is malformed"
    [[ "$remote_digest" == "$pushed_digest" ]] \
        || fail "remote chart manifest digest does not match the pushed digest"
    immutable_chart="oci://$CHART_REPOSITORY@$remote_digest"

    remote_chart_raw="$RUN_ROOT/remote-Chart.raw"
    remote_chart_yaml="$RUN_ROOT/remote-Chart.yaml"
    remote_values_raw="$RUN_ROOT/remote-values.raw"
    remote_values_yaml="$RUN_ROOT/remote-values.yaml"
    helm show chart "$immutable_chart" >"$remote_chart_raw" \
        || fail "cannot read chart metadata by immutable digest"
    extract_immutable_helm_output "$remote_chart_raw" "$immutable_chart" \
        "$remote_digest" "$remote_chart_yaml" "immutable chart metadata read-back"
    helm show values "$immutable_chart" >"$remote_values_raw" \
        || fail "cannot read chart defaults by immutable digest"
    extract_immutable_helm_output "$remote_values_raw" "$immutable_chart" \
        "$remote_digest" "$remote_values_yaml" "immutable chart defaults read-back"
    [[ $(require_chart_identity \
        "$remote_chart_yaml" "$remote_values_yaml" \
        "$approved_tree" "$approved_digest") == "$version" ]] \
        || fail "immutable chart read-back changed the chart version"

    template_raw="$RUN_ROOT/immutable-template.raw"
    template_manifest="$RUN_ROOT/immutable-template.yaml"
    helm template "$RELEASE" "$immutable_chart" \
        --kube-context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
        --values "$promoted_values" "${common_values[@]}" >"$template_raw" \
        || fail "immutable chart render failed"
    extract_immutable_helm_output "$template_raw" "$immutable_chart" \
        "$remote_digest" "$template_manifest" "immutable client render"
    [[ $(sed -n '1p' "$template_manifest") == --- ]] \
        || fail "immutable client render returned an unexpected payload"
    require_rendered_deployment "$template_manifest" "$approved_image" "immutable client render"

    dry_run_raw="$RUN_ROOT/immutable-server-dry-run.raw"
    dry_run_json="$RUN_ROOT/immutable-server-dry-run.json"
    helm upgrade "$RELEASE" "$immutable_chart" \
        --kube-context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
        --dry-run=server --hide-secret --output json \
        --reset-values --values "$promoted_values" \
        "${common_values[@]}" >"$dry_run_raw" \
        || fail "immutable chart server dry-run failed"
    extract_immutable_helm_output "$dry_run_raw" "$immutable_chart" \
        "$remote_digest" "$dry_run_json" "immutable server dry-run"
    jq -e -s 'length == 1 and (.[0] | type == "object")' \
        "$dry_run_json" >/dev/null \
        || fail "immutable server dry-run returned an unexpected JSON payload"
    dry_run_manifest="$RUN_ROOT/immutable-server-dry-run.yaml"
    jq -er '.manifest | select(type == "string" and length > 0)' \
        "$dry_run_json" >"$dry_run_manifest" \
        || fail "immutable server dry-run did not return a rendered manifest"
    require_rendered_deployment "$dry_run_manifest" "$approved_image" "immutable server dry-run"

    helm upgrade "$RELEASE" "$immutable_chart" \
        --kube-context "$KUBE_CONTEXT" --namespace "$NAMESPACE" \
        --reset-values --values "$promoted_values" "${common_values[@]}" \
        --rollback-on-failure --wait --timeout 30m --history-max 10 \
        >"$RUN_ROOT/upgrade.log" \
        || fail "immutable chart Helm upgrade failed"
    verify_live_result "$approved_image" "$approved_digest"

    echo "PASS: connected redb app/chart promotion contract is verified"
    echo "mode=$mode"
    echo "image=$approved_image"
    echo "image_source_tree=$approved_tree"
    echo "chart_source_tree=$chart_source_tree"
    echo "chart_version=$version"
    echo "chart=$immutable_chart"
    echo "deployment=$KUBE_CONTEXT/$NAMESPACE/$DEPLOYMENT"
}

run_promotion "$@"
