#!/usr/bin/env bash
# Isolated Maven/npm + S3 acceptance for NORA's persistent derived redb index.
#
# The harness creates its own Docker network, MinIO server, bucket and bind
# directories. It never accepts an external S3 endpoint. Artifact bytes remain
# authoritative in MinIO; every index-loss/corruption phase compares the exact
# bucket manifest before and after recovery.
#
# By default this is a development-candidate integration suite. Production
# matrix mode additionally proves that a deliberately undersized index volume
# fails closed while Maven/npm reads remain S3-backed. Exact redb ENOSPC,
# torn-write and double-crash engine regressions run in the separate production
# matrix wrapper; neither component alone authorizes a production allowlist row.

set -Eeuo pipefail

NORA_IMAGE=${NORA_REDB_E2E_IMAGE:-nora-redb-e2e:local}
EXPECTED_SOURCE_TREE=${NORA_REDB_E2E_SOURCE_TREE:-}
MINIO_IMAGE=${NORA_REDB_E2E_MINIO_IMAGE:-minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e}
MC_IMAGE=${NORA_REDB_E2E_MC_IMAGE:-minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727}
KEEP=${NORA_REDB_E2E_KEEP:-0}
EVIDENCE_OUTPUT=${NORA_REDB_E2E_EVIDENCE_DIR:-}
PRODUCTION_MATRIX=${NORA_REDB_E2E_PRODUCTION_MATRIX:-0}
IO_TIMEOUT_SECS=${NORA_REDB_E2E_IO_TIMEOUT_SECS:-120}

for command in docker curl jq openssl tar base64 sha256sum dd python3 timeout; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 2
    }
done
[[ "$EXPECTED_SOURCE_TREE" =~ ^[0-9a-f]{40}$ ]] || {
    echo "NORA_REDB_E2E_SOURCE_TREE must be the exact 40-hex git tree in the image label" >&2
    exit 2
}
[[ "$PRODUCTION_MATRIX" == 0 || "$PRODUCTION_MATRIX" == 1 ]] || {
    echo "NORA_REDB_E2E_PRODUCTION_MATRIX must be 0 or 1" >&2
    exit 2
}
if [[ ! "$IO_TIMEOUT_SECS" =~ ^[0-9]+$ ]] \
    || ((IO_TIMEOUT_SECS < 10 || IO_TIMEOUT_SECS > 600)); then
    echo "NORA_REDB_E2E_IO_TIMEOUT_SECS must be between 10 and 600" >&2
    exit 2
fi

RUN_ROOT=$(mktemp -d /tmp/nora-redb-e2e.XXXXXXXX)
RUN_TOKEN=$(basename "$RUN_ROOT" | tr -cd 'a-zA-Z0-9_.-')
NETWORK="${RUN_TOKEN}-network"
MINIO_NAME="${RUN_TOKEN}-minio"
BUCKET="${RUN_TOKEN,,}"
BUCKET=${BUCKET//_/-}
BUCKET=${BUCKET//./-}
ENV_FILE="$RUN_ROOT/runtime.env"
CONFIG_FILE="$RUN_ROOT/config.toml"
EVIDENCE_DIR="$RUN_ROOT/evidence"
mkdir -p "$EVIDENCE_DIR"

declare -a CONTAINERS=()
CURRENT_NORA=
CURRENT_BASE_URL=
CURRENT_PHASE=

cleanup() {
    local exit_code=$?
    trap - EXIT INT TERM
    for container in "${CONTAINERS[@]}"; do
        timeout -s TERM -k 5s 30s docker rm -f "$container" \
            >/dev/null 2>&1 || true
    done
    timeout -s TERM -k 5s 30s docker network rm "$NETWORK" \
        >/dev/null 2>&1 || true
    if [[ -f "$ENV_FILE" ]]; then
        : >"$ENV_FILE"
        chmod 600 "$ENV_FILE"
    fi
    if [[ "$KEEP" == 1 ]]; then
        echo "redb MinIO E2E workspace retained at $RUN_ROOT" >&2
    else
        case "$RUN_ROOT" in
            /tmp/nora-redb-e2e.*) rm -rf -- "$RUN_ROOT" ;;
            *) echo "refusing to remove unexpected run root: $RUN_ROOT" >&2 ;;
        esac
    fi
    exit "$exit_code"
}
trap cleanup EXIT INT TERM

fail() {
    echo "FAIL: $*" >&2
    if [[ -n "$CURRENT_NORA" ]]; then
        docker logs --tail 120 "$CURRENT_NORA" >&2 2>/dev/null || true
    fi
    return 1
}

mark_phase() {
    local phase=$1
    printf 'pass\n' >"$EVIDENCE_DIR/phase-$phase.ok"
}

image_id=$(docker image inspect --format '{{.Id}}' "$NORA_IMAGE")
image_source_tree=$(docker image inspect \
    --format '{{index .Config.Labels "io.nora.source-tree"}}' "$NORA_IMAGE")
[[ "$image_source_tree" == "$EXPECTED_SOURCE_TREE" ]] \
    || fail "image source-tree label does not match the reviewed tree"
image_cargo_lock_sha256=$(docker image inspect \
    --format '{{index .Config.Labels "io.nora.cargo-lock-sha256"}}' "$NORA_IMAGE")
image_repo_digests=$(docker image inspect --format '{{json .RepoDigests}}' "$NORA_IMAGE")
SOURCE_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cargo_lock_sha256=$(sha256sum "$SOURCE_ROOT/Cargo.lock" | awk '{print $1}')
[[ "$image_cargo_lock_sha256" == "$cargo_lock_sha256" ]] \
    || fail "image Cargo.lock label does not match the reviewed lockfile"
engine_revision=$(sed -nE 's/^const ENGINE_REVISION: &str = "([^"]+)";/\1/p' \
    "$SOURCE_ROOT/nora-registry/src/repo_index/redb_store.rs")
[[ -n "$engine_revision" ]] || fail "cannot read redb engine identity from the reviewed source"
schema_version=$(sed -nE 's/^const SCHEMA_VERSION: u32 = ([0-9]+);/\1/p' \
    "$SOURCE_ROOT/nora-registry/src/repo_index/redb_store.rs")
[[ -n "$schema_version" ]] || fail "cannot read NORA redb schema identity from the reviewed source"
cat >"$EVIDENCE_DIR/provenance.env" <<EOF
nora_image=$NORA_IMAGE
nora_image_id=$image_id
nora_image_repo_digests=$image_repo_digests
source_tree=$image_source_tree
cargo_lock_sha256=$cargo_lock_sha256
engine_revision=$engine_revision
schema_version=$schema_version
minio_image=$MINIO_IMAGE
mc_image=$MC_IMAGE
EOF

retry() {
    local attempts=$1
    local delay=$2
    shift 2
    local attempt
    for ((attempt = 1; attempt <= attempts; attempt++)); do
        if "$@"; then
            return 0
        fi
        sleep "$delay"
    done
    return 1
}

umask 077
MINIO_USER="nora-e2e-$(openssl rand -hex 8)"
MINIO_PASSWORD=$(openssl rand -hex 24)
cat >"$ENV_FILE" <<EOF
MINIO_ROOT_USER=$MINIO_USER
MINIO_ROOT_PASSWORD=$MINIO_PASSWORD
NORA_STORAGE_S3_ACCESS_KEY=$MINIO_USER
NORA_STORAGE_S3_SECRET_KEY=$MINIO_PASSWORD
EOF
chmod 600 "$ENV_FILE"

cat >"$CONFIG_FILE" <<EOF
[server]
port = 4000
public_url = "http://127.0.0.1:4000"

[storage]
mode = "s3"
s3_url = "http://$MINIO_NAME:9000"
bucket = "$BUCKET"
s3_region = "us-east-1"
s3_virtual_hosted = false
request_timeout_secs = 30
retry_timeout_secs = 60
max_retries = 2
health_probe_timeout_secs = 5

[index]
path = "/data/index/nora.redb"
reconcile_interval_secs = 3600

[auth]
enabled = false

[rate_limit]
enabled = false

[gc]
enabled = false

[retention]
enabled = false

[proxy_cache_cleanup]
enabled = false

[maven]
enabled = true
proxies = []
default_repository = "maven-hosted"

[[maven.repositories]]
kind = "hosted"
name = "maven-hosted"
version_policy = "release"
write_policy = "allow_once"

[npm]
enabled = true
default_repository = "npm-hosted"

[[npm.repositories]]
kind = "hosted"
name = "npm-hosted"
write_policy = "allow_once"

[registries]
enable = ["maven", "npm"]
EOF
chmod 600 "$CONFIG_FILE"

docker network create "$NETWORK" >/dev/null
docker run -d \
    --name "$MINIO_NAME" \
    --network "$NETWORK" \
    --env-file "$ENV_FILE" \
    "$MINIO_IMAGE" server /data --console-address :9001 >/dev/null
CONTAINERS+=("$MINIO_NAME")

mc_run() {
    # The credential variables expand inside the ephemeral container only.
    # shellcheck disable=SC2016
    timeout -s TERM -k 10s "${IO_TIMEOUT_SECS}s" docker run --rm \
        --network "$NETWORK" \
        --env-file "$ENV_FILE" \
        --entrypoint /bin/sh \
        "$MC_IMAGE" \
        -c 'mc alias set local http://'"$MINIO_NAME"':9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && exec mc "$@"' \
        sh "$@"
}

retry 60 1 mc_run ready local >/dev/null || fail "isolated MinIO did not become ready"
mc_run mb --ignore-existing "local/$BUCKET" >/dev/null

start_nora() {
    local phase=$1
    local data_dir=$2
    local name="${RUN_TOKEN}-nora-${phase}"
    mkdir -p "$data_dir/index"
    docker run -d \
        --name "$name" \
        --network "$NETWORK" \
        --user "$(id -u):$(id -g)" \
        --env-file "$ENV_FILE" \
        -e NORA_HOST=0.0.0.0 \
        -e NORA_REGISTRIES_ENABLE=maven,npm \
        -e RUST_LOG=info \
        -p 127.0.0.1::4000 \
        -v "$CONFIG_FILE:/etc/nora/config.toml:ro" \
        -v "$data_dir:/data" \
        "$NORA_IMAGE" >/dev/null
    CONTAINERS+=("$name")
    CURRENT_NORA=$name
    CURRENT_PHASE=$phase
    local port
    if ! port=$(docker port "$name" 4000/tcp 2>/dev/null | awk -F: 'NR == 1 {print $NF}'); then
        fail "$phase exited before publishing its HTTP port"
    fi
    [[ "$port" =~ ^[0-9]+$ ]] || fail "cannot resolve published NORA port for $phase"
    CURRENT_BASE_URL="http://127.0.0.1:$port"
}

start_nora_tmpfs() {
    local phase=$1
    local size=$2
    local name="${RUN_TOKEN}-nora-${phase}"
    docker run -d \
        --name "$name" \
        --network "$NETWORK" \
        --user "$(id -u):$(id -g)" \
        --env-file "$ENV_FILE" \
        -e NORA_HOST=0.0.0.0 \
        -e NORA_REGISTRIES_ENABLE=maven,npm \
        -e RUST_LOG=info \
        -p 127.0.0.1::4000 \
        -v "$CONFIG_FILE:/etc/nora/config.toml:ro" \
        --tmpfs "/data:rw,size=$size,mode=0770,uid=$(id -u),gid=$(id -g)" \
        "$NORA_IMAGE" >/dev/null
    CONTAINERS+=("$name")
    CURRENT_NORA=$name
    CURRENT_PHASE=$phase
    local port
    if ! port=$(docker port "$name" 4000/tcp 2>/dev/null | awk -F: 'NR == 1 {print $NF}'); then
        fail "$phase exited before publishing its HTTP port"
    fi
    [[ "$port" =~ ^[0-9]+$ ]] || fail "cannot resolve published NORA port for $phase"
    CURRENT_BASE_URL="http://127.0.0.1:$port"
}

http_status_is() {
    local expected=$1
    local path=$2
    [[ $(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
        --max-time 5 "$CURRENT_BASE_URL$path" 2>/dev/null || true) == "$expected" ]]
}

wait_live() {
    retry 120 1 http_status_is 200 /health || fail "$CURRENT_NORA did not become live"
}

wait_storage_ready() {
    retry 120 1 http_status_is 200 /ready || fail "$CURRENT_NORA did not reach storage readiness"
}

wait_index_ready() {
    retry 180 1 http_status_is 200 /ready/index || fail "$CURRENT_NORA index did not become ready"
}

stop_nora() {
    local name=$CURRENT_NORA
    local phase=$CURRENT_PHASE
    docker stop --time 35 "$name" >/dev/null
    local exit_code
    exit_code=$(docker inspect "$name" --format '{{.State.ExitCode}}')
    [[ "$exit_code" == 0 ]] || fail "$name graceful shutdown exit=$exit_code"
    docker logs "$name" >"$EVIDENCE_DIR/nora-$phase.log" 2>&1
    if [[ "$CURRENT_NORA" == "$name" ]]; then
        CURRENT_NORA=
        CURRENT_BASE_URL=
        CURRENT_PHASE=
    fi
}

container_stopped() {
    [[ $(docker inspect "$1" --format '{{.State.Running}}') == false ]]
}

metric_value() {
    local metric=$1
    curl --silent --show-error --max-time 5 "$CURRENT_BASE_URL/metrics" \
        | awk -v metric="$metric" '$1 == metric {print $2; found=1} END {if (!found) print 0}'
}

generation() {
    metric_value nora_index_generation
}

reconcile_successes() {
    curl --silent --show-error --max-time 5 "$CURRENT_BASE_URL/metrics" \
        | awk '/^nora_index_reconcile_total\{[^}]*result="success"[^}]*\}/ {sum += $2} END {print sum + 0}'
}

reconcile_errors_present() {
    curl --silent --show-error --max-time 5 "$CURRENT_BASE_URL/metrics" \
        | awk '/^nora_index_reconcile_total\{[^}]*result="error"[^}]*\}/ {sum += $2} END {exit !(sum > 0)}'
}

wait_generation_gt() {
    local before=$1
    local attempt current
    for ((attempt = 1; attempt <= 120; attempt++)); do
        current=$(generation)
        if [[ "$current" =~ ^[0-9]+$ ]] && ((current > before)); then
            return 0
        fi
        sleep 1
    done
    fail "index generation did not advance past $before"
}

publish_maven() {
    local artifact=$1
    local version=$2
    local payload=$3
    local response="$RUN_ROOT/maven-response"
    local status
    status=$(curl --silent --show-error --connect-timeout 5 \
        --max-time "$IO_TIMEOUT_SECS" --output "$response" --write-out '%{http_code}' \
        --request PUT --header 'Content-Type: application/java-archive' \
        --data-binary "@$payload" \
        "$CURRENT_BASE_URL/repository/maven-hosted/com/acme/$artifact/$version/$artifact-$version.jar")
    [[ "$status" == 200 || "$status" == 201 ]] || fail "Maven publish returned HTTP $status"
}

make_npm_payload() {
    local package=$1
    local version=$2
    local output=$3
    local package_root="$RUN_ROOT/npm-package-$package-$version"
    local tarball="$RUN_ROOT/$package-$version.tgz"
    mkdir -p "$package_root/package"
    jq -cn --arg name "$package" --arg version "$version" \
        '{name:$name,version:$version}' >"$package_root/package/package.json"
    tar -C "$package_root" -czf "$tarball" package/package.json
    local encoded length filename
    encoded=$(base64 -w0 "$tarball")
    length=$(stat -c '%s' "$tarball")
    filename="$package-$version.tgz"
    jq -cn \
        --arg name "$package" \
        --arg version "$version" \
        --arg filename "$filename" \
        --arg data "$encoded" \
        --argjson length "$length" \
        '{name:$name,versions:{($version):{name:$name,version:$version,dist:{}}},_attachments:{($filename):{data:$data,length:$length}},"dist-tags":{latest:$version}}' \
        >"$output"
}

publish_npm() {
    local package=$1
    local version=$2
    local payload="$RUN_ROOT/$package-$version.json"
    local response="$RUN_ROOT/npm-response"
    make_npm_payload "$package" "$version" "$payload"
    local status
    status=$(curl --silent --show-error --connect-timeout 5 \
        --max-time "$IO_TIMEOUT_SECS" --output "$response" --write-out '%{http_code}' \
        --request PUT --header 'Content-Type: application/json' \
        --data-binary "@$payload" \
        "$CURRENT_BASE_URL/repository/npm-hosted/$package")
    [[ "$status" == 200 || "$status" == 201 ]] || fail "npm publish returned HTTP $status"
}

assert_maven_visible() {
    local artifact=$1
    local version=$2
    local expected=$3
    local downloaded="$RUN_ROOT/download-$artifact-$version.jar"
    curl --fail --silent --show-error --max-time 10 \
        "$CURRENT_BASE_URL/repository/maven-hosted/com/acme/$artifact/$version/$artifact-$version.jar" \
        >"$downloaded"
    cmp -s "$expected" "$downloaded" || fail "Maven artifact bytes differ for $artifact:$version"
    curl --fail --silent --show-error --max-time 10 \
        "$CURRENT_BASE_URL/api/ui/maven/list?q=$artifact&limit=100" \
        | jq -e --arg needle "$artifact/$version" \
            '.items | any(.name | contains($needle))' >/dev/null \
        || fail "Maven redb UI projection is missing $artifact:$version"
}

assert_npm_visible() {
    local package=$1
    local version=$2
    curl --fail --silent --show-error --max-time 10 \
        "$CURRENT_BASE_URL/repository/npm-hosted/$package" \
        | jq -e --arg name "$package" --arg version "$version" \
            '.name == $name and .versions[$version].version == $version' >/dev/null \
        || fail "npm hosted packument is missing $package@$version"
    curl --fail --silent --show-error --max-time 10 \
        "$CURRENT_BASE_URL/api/ui/npm/list?q=$package&limit=100" \
        | jq -e --arg needle "$package" '.items | any(.name | contains($needle))' >/dev/null \
        || fail "npm redb UI projection is missing $package@$version"
}

bucket_manifest() {
    local label=$1
    local snapshot="$EVIDENCE_DIR/$label"
    mkdir -p "$snapshot"
    docker run --rm \
        --network "$NETWORK" \
        --user "$(id -u):$(id -g)" \
        --env-file "$ENV_FILE" \
        -e HOME=/tmp \
        --entrypoint /bin/sh \
        -v "$snapshot:/evidence" \
        "$MC_IMAGE" \
        -c 'mc alias set local http://'"$MINIO_NAME"':9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc mirror --overwrite local/'"$BUCKET"' /evidence >/dev/null'
    (
        cd "$snapshot"
        find . -type f -print0 | sort -z | xargs -0 -r sha256sum
    ) >"$EVIDENCE_DIR/$label.sha256"
    [[ -s "$EVIDENCE_DIR/$label.sha256" ]] || fail "bucket manifest $label is empty"
}

assert_bucket_unchanged() {
    local baseline=$1
    local label=$2
    bucket_manifest "$label"
    cmp -s "$EVIDENCE_DIR/$baseline.sha256" "$EVIDENCE_DIR/$label.sha256" \
        || fail "S3 bytes changed between $baseline and $label"
}

MAVEN_ONE="$RUN_ROOT/demo-1.0.jar"
MAVEN_TWO="$RUN_ROOT/demo-next-2.0.jar"
MAVEN_CRASH="$RUN_ROOT/demo-crash-3.0.jar"
printf '%s' 'maven-redb-e2e-v1' >"$MAVEN_ONE"
printf '%s' 'maven-redb-e2e-v2' >"$MAVEN_TWO"
printf '%s' 'maven-redb-e2e-crash-v3' >"$MAVEN_CRASH"

echo "phase=seed"
SEED_DATA="$RUN_ROOT/seed-data"
start_nora seed "$SEED_DATA"
wait_live
wait_storage_ready
wait_index_ready
seed_generation=$(generation)
publish_maven demo 1.0 "$MAVEN_ONE"
publish_npm redb-pkg 1.0.0
wait_generation_gt "$seed_generation"
wait_index_ready
assert_maven_visible demo 1.0 "$MAVEN_ONE"
assert_npm_visible redb-pkg 1.0.0
bucket_manifest seeded
stop_nora
seed_db_bytes=$(stat -c '%s' "$SEED_DATA/index/nora.redb")
((seed_db_bytes > 0)) || fail "seed redb file is empty"

echo "phase=cold-rebuild"
COLD_DATA="$RUN_ROOT/cold-data"
start_nora cold "$COLD_DATA"
wait_live
wait_storage_ready
wait_index_ready
assert_maven_visible demo 1.0 "$MAVEN_ONE"
assert_npm_visible redb-pkg 1.0.0
cold_generation=$(generation)
cold_db_bytes=$(stat -c '%s' "$COLD_DATA/index/nora.redb")
((cold_db_bytes > 0)) || fail "cold-rebuilt redb file is empty"
stop_nora
assert_bucket_unchanged seeded after-cold-rebuild
mark_phase cold_rebuild

echo "phase=warm-incremental-and-single-writer"
start_nora warm "$COLD_DATA"
wait_live
# Last-good pages are served from redb independently of the startup index gate.
assert_maven_visible demo 1.0 "$MAVEN_ONE"
assert_npm_visible redb-pkg 1.0.0
wait_storage_ready
wait_index_ready
warm_generation=$(generation)
((warm_generation >= cold_generation)) || fail "warm reopen regressed the published generation"
docker logs "$CURRENT_NORA" 2>&1 | grep -Eq 'full_integrity[^[:alnum:]]+false' \
    || fail "clean warm reopen did not use the bounded open/schema preflight"
mark_phase warm_reopen

CONTENDER="${RUN_TOKEN}-nora-contender"
docker run -d \
    --name "$CONTENDER" \
    --network "$NETWORK" \
    --user "$(id -u):$(id -g)" \
    --env-file "$ENV_FILE" \
    -e NORA_HOST=0.0.0.0 \
    -e NORA_REGISTRIES_ENABLE=maven,npm \
    -v "$CONFIG_FILE:/etc/nora/config.toml:ro" \
    -v "$COLD_DATA:/data" \
    "$NORA_IMAGE" >/dev/null
CONTAINERS+=("$CONTENDER")
retry 30 1 container_stopped "$CONTENDER" \
    || fail "second writer stayed running against the same redb file"
contender_exit=$(docker inspect "$CONTENDER" --format '{{.State.ExitCode}}')
[[ "$contender_exit" != 0 ]] || fail "second writer unexpectedly exited successfully"
docker logs "$CONTENDER" 2>&1 | grep -qi 'already open' \
    || fail "second-writer rejection did not report the file-lock conflict"
mark_phase single_writer

reconcile_before=$(reconcile_successes)
incremental_generation=$(generation)
publish_maven demo-next 2.0 "$MAVEN_TWO"
publish_npm redb-next 2.0.0
wait_generation_gt "$incremental_generation"
wait_index_ready
assert_maven_visible demo-next 2.0 "$MAVEN_TWO"
assert_npm_visible redb-next 2.0.0
reconcile_after=$(reconcile_successes)
[[ "$reconcile_after" == "$reconcile_before" ]] \
    || fail "incremental Maven/npm updates triggered a full S3 reconciliation"
mark_phase incremental

echo "phase=acknowledged-write-sigkill-recovery"
publish_maven demo-crash 3.0 "$MAVEN_CRASH"
docker kill --signal KILL "$CURRENT_NORA" >/dev/null
docker wait "$CURRENT_NORA" >/dev/null
docker logs "$CURRENT_NORA" >"$EVIDENCE_DIR/nora-$CURRENT_PHASE-sigkill.log" 2>&1 || true
CURRENT_NORA=
CURRENT_BASE_URL=
CURRENT_PHASE=
start_nora crash-reopen "$COLD_DATA"
wait_live
wait_storage_ready
wait_index_ready
assert_maven_visible demo-crash 3.0 "$MAVEN_CRASH"
assert_npm_visible redb-next 2.0.0
docker logs "$CURRENT_NORA" 2>&1 | grep -Eq 'full_integrity[^[:alnum:]]+true' \
    || fail "SIGKILL recovery did not run the isolated full-integrity preflight"
bucket_manifest post-crash
docker stats --no-stream --format '{{.MemUsage}}' "$CURRENT_NORA" \
    >"$EVIDENCE_DIR/peak-sample-memory.txt"
stop_nora
mark_phase crash

echo "phase=typed-corruption-quarantine-reseed"
# A normal clean restart intentionally skips the expensive full-file scan.
# Reopen and kill the writer first so the copied fixture carries the durable
# unclean marker and exercises the isolated corruption-check path.
start_nora corruption-source "$COLD_DATA"
wait_live
wait_storage_ready
wait_index_ready
docker kill --signal KILL "$CURRENT_NORA" >/dev/null
docker wait "$CURRENT_NORA" >/dev/null
docker logs "$CURRENT_NORA" >"$EVIDENCE_DIR/nora-$CURRENT_PHASE-sigkill.log" 2>&1 || true
CURRENT_NORA=
CURRENT_BASE_URL=
CURRENT_PHASE=
CORRUPT_DATA="$RUN_ROOT/corrupt-data"
cp -a "$COLD_DATA" "$CORRUPT_DATA"
CORRUPT_DB="$CORRUPT_DATA/index/nora.redb"
before_corrupt=$(sha256sum "$CORRUPT_DB" | awk '{print $1}')
# This is deliberately only a generic corruption test. A legitimate previous
# ENGINE_REVISION/SCHEMA_VERSION fixture must be produced by the real previous
# binary; byte substitution cannot prove an upgrade transition. Corrupt a
# known inventory value rather than the redb file header: the pinned MinIO
# single-part ETag is the artifact MD5, and the existing unit test proves that
# a value-page checksum failure is returned as typed Corrupted. An abort/signal
# or generic I/O outcome must remain non-destructive.
CORRUPTION_MARKER=$(openssl dgst -md5 "$MAVEN_CRASH" | awk '{print $NF}')
mapfile -t marker_matches \
    < <(LC_ALL=C grep -aob -F "$CORRUPTION_MARKER" "$CORRUPT_DB")
((${#marker_matches[@]} > 0)) \
    || fail "cannot locate the controlled redb inventory marker"
marker_offset=${marker_matches[0]%%:*}
corrupt_offset=$((marker_offset + ${#CORRUPTION_MARKER} / 2))
original_byte=$(dd if="$CORRUPT_DB" bs=1 skip="$corrupt_offset" count=1 status=none)
replacement_byte=0
[[ "$original_byte" != 0 ]] || replacement_byte=1
printf '%s' "$replacement_byte" \
    | dd of="$CORRUPT_DB" bs=1 seek="$corrupt_offset" count=1 \
    conv=notrunc status=none
after_corrupt=$(sha256sum "$CORRUPT_DB" | awk '{print $1}')
[[ "$before_corrupt" != "$after_corrupt" ]] || fail "could not inject deterministic redb corruption"
start_nora corrupt "$CORRUPT_DATA"
wait_live
wait_storage_ready
wait_index_ready
docker logs "$CURRENT_NORA" 2>&1 | grep -q 'quarantined unusable derived index' \
    || fail "typed redb corruption was not quarantined by the preflight child"
compgen -G "$CORRUPT_DB.quarantine.*" >/dev/null \
    || fail "quarantined redb file was not preserved"
assert_maven_visible demo-crash 3.0 "$MAVEN_CRASH"
assert_npm_visible redb-next 2.0.0
stop_nora
assert_bucket_unchanged post-crash after-quarantine
mark_phase corruption

echo "phase=index-loss-reseed"
LOST_DATA="$RUN_ROOT/lost-data"
start_nora lost "$LOST_DATA"
wait_live
wait_storage_ready
wait_index_ready
assert_maven_visible demo-crash 3.0 "$MAVEN_CRASH"
assert_npm_visible redb-next 2.0.0
lost_db_bytes=$(stat -c '%s' "$LOST_DATA/index/nora.redb")
((lost_db_bytes > 0)) || fail "lost-index reseed did not create redb"
stop_nora
assert_bucket_unchanged post-crash after-index-loss
mark_phase index_loss
mark_phase recovery

if [[ "$PRODUCTION_MATRIX" == 1 ]]; then
    echo "phase=disk-full-admission-and-protocol-survival"
    # The derived index reserves at least 64 MiB for a safe shadow build. A 2
    # MiB tmpfs therefore deterministically exercises the low-space/ENOSPC
    # boundary before redb can damage the active generation.
    start_nora_tmpfs disk-full 2m
    wait_live
    wait_storage_ready
    retry 120 1 reconcile_errors_present \
        || fail "undersized index volume did not report a failed reconciliation"
    http_status_is 503 /ready/index \
        || fail "undersized index volume unexpectedly reported index readiness"
    curl --fail --silent --show-error --max-time 10 \
        "$CURRENT_BASE_URL/repository/maven-hosted/com/acme/demo-crash/3.0/demo-crash-3.0.jar" \
        | cmp -s "$MAVEN_CRASH" - \
        || fail "Maven S3 read failed while the derived index was disk-full"
    curl --fail --silent --show-error --max-time 10 \
        "$CURRENT_BASE_URL/repository/npm-hosted/redb-next" \
        | jq -e '.name == "redb-next" and .versions["2.0.0"].version == "2.0.0"' >/dev/null \
        || fail "npm S3 read failed while the derived index was disk-full"
    stop_nora

    DISK_RECOVERY_DATA="$RUN_ROOT/disk-recovery-data"
    start_nora disk-recovery "$DISK_RECOVERY_DATA"
    wait_live
    wait_storage_ready
    wait_index_ready
    assert_maven_visible demo-crash 3.0 "$MAVEN_CRASH"
    assert_npm_visible redb-next 2.0.0
    stop_nora
    assert_bucket_unchanged post-crash after-disk-full-recovery
    mark_phase disk_full_admission
fi

if grep -RFl -- "$MINIO_USER" "$EVIDENCE_DIR" | grep -q . \
    || grep -RFl -- "$MINIO_PASSWORD" "$EVIDENCE_DIR" | grep -q .; then
    fail "ephemeral MinIO credentials appeared in the evidence payload"
fi
artifacts_json="$RUN_ROOT/redb-minio-e2e-artifacts.json"
python3 - "$EVIDENCE_DIR" >"$artifacts_json" <<'PY'
import hashlib
import json
import pathlib
import stat
import sys

root = pathlib.Path(sys.argv[1])
artifacts = {}
for path in sorted(root.rglob("*")):
    relative = path.relative_to(root).as_posix()
    mode = path.lstat().st_mode
    if stat.S_ISDIR(mode):
        continue
    if not stat.S_ISREG(mode):
        raise SystemExit(f"non-regular MinIO evidence member: {relative}")
    artifacts[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
if not artifacts:
    raise SystemExit("MinIO evidence payload is empty")
json.dump(artifacts, sys.stdout, sort_keys=True, separators=(",", ":"))
sys.stdout.write("\n")
PY
mapfile -t artifact_paths < <(jq -er 'keys[]' "$artifacts_json")
evidence_bundle="$RUN_ROOT/redb-minio-e2e-evidence.tar"
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
    -C "$EVIDENCE_DIR" -cf "$evidence_bundle" -- "${artifact_paths[@]}"
evidence_bundle_sha256=$(sha256sum "$evidence_bundle" | awk '{print $1}')
jq -n \
    --arg source_tree "$image_source_tree" \
    --arg cargo_lock_sha256 "$cargo_lock_sha256" \
    --arg image "$NORA_IMAGE" \
    --arg image_id "$image_id" \
    --arg image_repo_digests "$image_repo_digests" \
    --arg engine_revision "$engine_revision" \
    --argjson schema_version "$schema_version" \
    --arg minio_image "$MINIO_IMAGE" \
    --arg mc_image "$MC_IMAGE" \
    --arg bundle_sha256 "$evidence_bundle_sha256" \
    --slurpfile artifacts "$artifacts_json" \
    --argjson production_matrix_component "$PRODUCTION_MATRIX" \
    '{
        schema: 1,
        source_tree: $source_tree,
        cargo_lock_sha256: $cargo_lock_sha256,
        image: $image,
        image_id: $image_id,
        image_repo_digests: $image_repo_digests,
        engine_revision: $engine_revision,
        nora_schema_version: $schema_version,
        minio_image: $minio_image,
        mc_image: $mc_image,
        bundle_sha256: $bundle_sha256,
        artifacts: $artifacts[0],
        phase_artifacts: ({
            cold_rebuild: "phase-cold_rebuild.ok",
            warm_reopen: "phase-warm_reopen.ok",
            incremental: "phase-incremental.ok",
            single_writer: "phase-single_writer.ok",
            crash: "phase-crash.ok",
            corruption: "phase-corruption.ok",
            index_loss: "phase-index_loss.ok",
            recovery: "phase-recovery.ok"
        } + (if $production_matrix_component == 1 then {
            disk_full_admission: "phase-disk_full_admission.ok"
        } else {} end)),
        verified_phases: ([
            "cold_rebuild",
            "warm_reopen",
            "incremental",
            "single_writer",
            "crash",
            "corruption",
            "index_loss",
            "recovery"
        ] + (if $production_matrix_component == 1 then ["disk_full_admission"] else [] end)),
        production_matrix_component_complete: ($production_matrix_component == 1),
        production_matrix_complete: false
    }' >"$RUN_ROOT/redb-minio-e2e-evidence.json"
evidence_manifest_sha256=$(sha256sum "$RUN_ROOT/redb-minio-e2e-evidence.json" | awk '{print $1}')

if [[ -n "$EVIDENCE_OUTPUT" ]]; then
    evidence_destination="$EVIDENCE_OUTPUT/$RUN_TOKEN"
    [[ ! -e "$evidence_destination" ]] \
        || fail "evidence destination already exists: $evidence_destination"
    mkdir -p "$evidence_destination"
    cp "$evidence_bundle" "$evidence_destination/"
    cp "$RUN_ROOT/redb-minio-e2e-evidence.json" "$evidence_destination/"
    echo "evidence_dir=$evidence_destination"
fi

echo "PASS: isolated redb MinIO E2E"
echo "nora_image=$NORA_IMAGE"
echo "nora_image_id=$image_id"
echo "source_tree=$image_source_tree"
echo "cargo_lock_sha256=$cargo_lock_sha256"
echo "minio_image=$MINIO_IMAGE"
echo "mc_image=$MC_IMAGE"
echo "engine_revision=$engine_revision"
echo "schema_version=$schema_version"
echo "evidence_bundle_sha256=$evidence_bundle_sha256"
echo "evidence_manifest_sha256=$evidence_manifest_sha256"
echo "seed_db_bytes=$seed_db_bytes"
echo "cold_db_bytes=$cold_db_bytes"
echo "lost_db_bytes=$lost_db_bytes"
