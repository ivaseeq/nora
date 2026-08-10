#!/usr/bin/env bash

set -euo pipefail

readonly K6_IMAGE='docker.io/grafana/k6:2.1.0@sha256:65c920dc067d5e2e00befbf982af6ad6ad0117034e8b1c65817c7975c52d4669'
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR

require_env() {
    local name="$1"
    if [[ -z "${!name:-}" ]]; then
        printf 'error: %s is required\n' "$name" >&2
        exit 2
    fi
}

for name in BASE_URL MAVEN_PATH NPM_PACKAGE_PATH NPM_VERSION; do
    require_env "$name"
done

exec docker run --rm -i \
    --env BASE_URL \
    --env MAVEN_PATH \
    --env NPM_PACKAGE_PATH \
    --env NPM_VERSION \
    --env MAVEN_VUS \
    --env MAVEN_ITERATIONS \
    --env NPM_VUS \
    --env NPM_ITERATIONS \
    --env EXPECTED_MAVEN_SHA256 \
    --env EXPECTED_NPM_PACKUMENT_SHA256 \
    --env EXPECTED_NPM_TARBALL_SHA256 \
    "$K6_IMAGE" run - < "$SCRIPT_DIR/load-test.js"
