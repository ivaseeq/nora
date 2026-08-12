#!/usr/bin/env bash
# Public tags must not rebuild or publish a second, unqualified image while the
# production engine is an exact Git revision. The qualified Harbor/chart path
# is the only release path until Cargo.toml returns to a stable crates.io redb.

set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
MANIFEST="$ROOT/nora-registry/Cargo.toml"
WORKSPACE_MANIFEST="$ROOT/Cargo.toml"

(($# == 0)) || {
    echo "public release policy accepts no caller-supplied dependency identity" >&2
    exit 2
}
[[ -f "$MANIFEST" && ! -L "$MANIFEST" ]] || {
    echo "public release blocked: Cargo manifest is not a regular file" >&2
    exit 1
}
[[ -f "$WORKSPACE_MANIFEST" && ! -L "$WORKSPACE_MANIFEST" ]] || {
    echo "public release blocked: workspace Cargo manifest is not a regular file" >&2
    exit 1
}

python3 - "$MANIFEST" "$WORKSPACE_MANIFEST" <<'PY'
import re
import sys
import tomllib

with open(sys.argv[1], "rb") as handle:
    manifest = tomllib.load(handle)
with open(sys.argv[2], "rb") as handle:
    workspace_manifest = tomllib.load(handle)
dependency = manifest.get("dependencies", {}).get("redb")

patched_redb = [
    source
    for candidate in (manifest, workspace_manifest)
    for registry in candidate.get("patch", {}).values()
    if isinstance(registry, dict)
    for name, source in registry.items()
    if name == "redb"
]
if patched_redb:
    raise SystemExit("public release blocked: redb has a Cargo patch override")
if any(
    str(name).split(":", 1)[0] == "redb"
    for candidate in (manifest, workspace_manifest)
    for name in candidate.get("replace", {})
):
    raise SystemExit("public release blocked: redb has a Cargo replace override")
for target, table in manifest.get("target", {}).items():
    if not isinstance(table, dict):
        continue
    for section in ("dependencies", "dev-dependencies", "build-dependencies"):
        if isinstance(table.get(section), dict) and "redb" in table[section]:
            raise SystemExit(
                f"public release blocked: target {target!r} has a redb dependency override"
            )

if isinstance(dependency, dict) and any(
    dependency.get(field) for field in ("git", "rev", "branch", "tag")
):
    raise SystemExit(
        "public release blocked: exact-git redb must consume the qualified "
        "Harbor image; public release.yml may not rebuild or publish another image"
    )
if isinstance(dependency, str):
    version = dependency
elif isinstance(dependency, dict):
    for forbidden in ("path", "workspace", "registry", "package"):
        if dependency.get(forbidden):
            raise SystemExit(
                f"public release blocked: redb dependency uses unsupported {forbidden} indirection"
            )
    version = dependency.get("version", "")
else:
    raise SystemExit("public release blocked: redb dependency is missing or malformed")
if not isinstance(version, str) or not re.fullmatch(r"=?\d+\.\d+\.\d+", version.strip()):
    raise SystemExit("public release blocked: redb is not a stable crates.io version")
print(f"PASS: public release uses stable crates.io redb {version}")
PY
