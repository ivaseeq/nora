# NORA

**The artifact registry that grows with you.** Starts with `docker run`, scales with your needs.

```bash
docker run -d -p 4000:4000 -v nora-data:/data getnora/nora:latest
```

Open [http://localhost:4000/ui/](http://localhost:4000/ui/) — your registry is ready.

<p align="center">
  <img src=".github/assets/dashboard.png" alt="NORA Dashboard" width="960" />
</p>

## Why NORA

- **Zero-config** — single binary, no database, no dependencies. `docker run` and it works.
- **15 registries** — Docker, Maven, npm, PyPI, Cargo, Go, Raw, RubyGems, Terraform, Ansible Galaxy, NuGet, Pub (Dart/Flutter), Conan (C/C++), RPM (yum/dnf), Debian/APT.
- **Secure by default** — [OpenSSF Scorecard](https://scorecard.dev/viewer/?uri=github.com/getnora-io/nora), signed releases, SBOM, fuzz testing, 1200+ tests.

[![Release](https://img.shields.io/github/v/release/getnora-io/nora)](https://github.com/getnora-io/nora/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/nora)](https://artifacthub.io/packages/helm/nora/nora)
[![Docker Pulls](https://img.shields.io/docker/pulls/getnora/nora)](https://hub.docker.com/r/getnora/nora)

**< 30 MB** binary | **< 50 MB** RAM idle | **3s** startup | **15** registries

## Supported Registries

All endpoints require authentication. Anonymous read is opt-in via `anonymous_read: true`.

| Format | Pull (proxy/cache) | Push/Publish | Default Upstream | Notes |
|--------|:---:|:---:|---|---|
| Docker Registry v2 | ✅ | ✅ | `registry-1.docker.io` | hosted + proxy; cache on when `docker.upstreams` non-empty (Docker Hub by default) |
| Maven | ✅ | ✅ | `repo1.maven.org/maven2` | named hosted/proxy/group at `/repository/{name}/`; `/maven2/` alias |
| npm | ✅ | ✅ | `registry.npmjs.org` | named hosted/proxy/group at `/repository/{name}/`; `/npm/` alias |
| Cargo | ✅ | ✅ | `crates.io` (sparse index) | hosted + proxy (sparse index) |
| PyPI | ✅ | ✅ | `pypi.org/simple/` | hosted + proxy |
| Go Modules | ✅ | — | `proxy.golang.org` | proxy only (modules immutable, push not in protocol) |
| Raw files | ❌ | ✅ | — (no upstream) | hosted only; conditional `PUT` (ETag/`If-Match` — local backend only; `If-None-Match: *` works on any backend) |
| RubyGems | ✅ | ❌ | `rubygems.org` | proxy only — `gem push` not implemented in NORA v1.1.0 |
| Terraform | ✅ | — | `registry.terraform.io` | proxy only; client configuration notes in COMPAT.md |
| Ansible Galaxy | ✅ | ❌ | `galaxy.ansible.com` | proxy only — `ansible-galaxy collection publish` not implemented |
| NuGet | ✅ | ❌ | `api.nuget.org` | proxy only — `dotnet nuget push` not implemented |
| Pub (Dart/Flutter) | ✅ | ❌ | `pub.dev` | proxy only — `dart pub publish` not implemented |
| Conan (C/C++) | ⚠️ | ❌ | `center2.conan.io` | proxy only; Conan client compatibility tracked in COMPAT.md |
| RPM (yum/dnf) | ⚠️ | ✅ | — (none by default) | hosted; pull-through via `config.registries.rpm.proxies` (off by default); auto-generates `repodata/` |
| Debian/APT | ⚠️ | ✅ | — (none by default) | hosted; pull-through via `config.registries.deb.proxies` (off by default); flat & structured layouts; auto-generates `Packages`/`Release`/`InRelease` |

> **Helm charts** work via the Docker/OCI endpoint — `helm push`/`pull` with `--plain-http` or behind TLS reverse proxy.

> **Pull/Push legend:** ✅ supported · ⚠️ partial (pull-through available but off by default, or client compatibility issue) · ❌ not implemented in NORA v1.1.0 · — not applicable (protocol has no push). Per-format details and cache strategy in [COMPAT.md](COMPAT.md).

## Quick Start

### Docker (Recommended)

```bash
docker run -d -p 4000:4000 -v nora-data:/data getnora/nora:latest
```

### Binary

```bash
# x86_64
curl -fsSL https://github.com/getnora-io/nora/releases/latest/download/nora-linux-amd64 -o nora

# ARM64 (Raspberry Pi, Graviton, Apple Silicon VMs)
curl -fsSL https://github.com/getnora-io/nora/releases/latest/download/nora-linux-arm64 -o nora

chmod +x nora && ./nora
```

`./nora` listens on `127.0.0.1:4000`. To expose it on a network, set the bind
address and the public URL clients should use for download links:

```bash
NORA_HOST=0.0.0.0 NORA_PUBLIC_URL=https://registry.example.com ./nora
```

### Kubernetes (Helm)

```bash
helm repo add nora https://getnora-io.github.io/helm-charts
helm install nora nora/nora
```

### From Source

```bash
cargo install nora-registry
nora
```

## Usage

```bash
# Docker
docker tag myapp:latest localhost:4000/myapp:latest
docker push localhost:4000/myapp:latest

# Nexus-compatible npm topology: publish to hosted, install through the group
export NORA_NPM_REPOSITORIES_JSON='[{"kind":"hosted","name":"npm-private","write_policy":"allow"},{"kind":"proxy","name":"npm-registry","url":"https://registry.npmjs.org"},{"kind":"group","name":"npm-group","members":["npm-private","npm-registry"]}]'
export NORA_NPM_DEFAULT_REPOSITORY=npm-group
npm config set registry http://localhost:4000/repository/npm-group/
npm publish --registry http://localhost:4000/repository/npm-private/

# Go
GOPROXY=http://localhost:4000/go go get golang.org/x/text@latest
```

See [full documentation](https://getnora.dev) for all registries.

For production, Maven and npm use Nexus-style named `hosted`, `proxy`, and
`group` repositories under one `/repository/{name}/` namespace. Repository
names are globally unique across both formats, groups own no storage, and one
NORA process is the supported writer topology. The legacy `/maven2/` and
`/npm/` routes remain compatibility aliases, not the recommended deployment
model.

## Features

- **Web UI** — dashboard with search, browse, i18n (EN/RU)
- **Proxy & Cache** — transparent proxy to upstream registries with local cache
- **Curation** — blocklist, allowlist, namespace isolation, integrity verification, min-release-age filter, digest quarantine
- **Token RBAC** — read/write/admin roles, expiry tracking, deferred last_used flush
- **Mirror CLI** — offline sync for air-gapped environments (`nora mirror`)
- **Backup & Restore** — `nora backup` / `nora restore`
- **S3 Storage** — AWS S3, Ceph RGW, any S3-compatible backend
- **Persistent Maven/npm index** — bounded redb-backed browse/search pages,
  incremental repair after NORA writes, and background S3 reconciliation
- **Prometheus Metrics** — `/metrics` endpoint, [Grafana dashboard](MONITORING.md)
- **Rate Limiting** — configurable per-endpoint rate limits

## Configuration

NORA works out of the box. For advanced setup — auth, S3, retention, curation — see [getnora.dev/configuration](https://getnora.dev/configuration/settings/).

For Maven/npm on S3, keep the derived index on persistent local storage:

```toml
[index]
path = "/var/lib/nora/index/nora.redb"
reconcile_interval_secs = 3600
```

S3 remains authoritative; deleting the redb file only forces a background
rebuild. Run exactly one NORA process. A PVC improves warm restart and UI/search
availability but is not an HA or distributed-locking mechanism. `/ready` is the
storage gate and `/ready/index` is the independent Maven/npm projection gate.
Until the first usable generation is published, Maven/npm browser pages show
the current indexing phase and exact committed object/package counts; this
progress display does not issue additional storage requests, and artifact API
reads remain independent.
After a durably clean shutdown, an exact-topology generation whose accepted
changes are fully covered by its active watermark is published immediately;
dirty or unclean state still reconciles from S3 before `/ready/index` becomes
ready. The clean-proof contract is versioned: a database last closed by a
binary without that proof performs one fail-closed S3 reconciliation before
warm reuse. Periodic reconciliation remains the anti-entropy path for
out-of-band changes.
Local and GCS storage keep their existing in-memory index path. The current
implementation pins one full upstream redb revision containing required
post-4.1 crash/recovery fixes. Production promotion accepts only that canonical
repository and revision after it is bound to a new application schema and a
saved crash/recovery evidence manifest. The qualification path is deliberately
split: `redb-production-matrix.sh` tests one immutable Harbor image;
`publish-redb-production-evidence.sh` publishes the resulting tar plus matrix
as an OCI artifact and reads it back by the registry manifest digest; the
checked-in approval records that OCI digest separately from the tar SHA-256.
The matrix re-executes from an immutable archive and independently recomputes
the reviewed Git tree inside that snapshot. It uses the dependency-complete
redb runner only by its read-back Harbor digest, enforces bounded deadlines,
and archives only a canonical, hash-enumerated evidence member set. The
publisher derives a content-addressed staging tag from the tree and bundle
hash; the approval always names the immutable OCI manifest digest.
The production source gate then pulls and hashes the actual artifact, verifies
the embedded matrix, dependency, Cargo.lock, canonical load-bearing source and
all four exact harnesses, and binds the tested Harbor image digest. The Helm
chart package/render has its own later digest gate because it does not exist at
application build time.

```bash
# Auth
docker run -d -p 4000:4000 \
  -v nora-data:/data \
  -v ./users.htpasswd:/data/users.htpasswd \
  -e NORA_AUTH_ENABLED=true \
  getnora/nora:latest
```

```bash
# Curation — block packages younger than 7 days
docker run -d -p 4000:4000 \
  -v nora-data:/data \
  -e NORA_CURATION_MODE=enforce \
  -e NORA_CURATION_MIN_RELEASE_AGE=7d \
  -e NORA_CURATION_ALLOWLIST_PATH=/data/allowlist.json \
  getnora/nora:latest
```

## Performance

| Metric | NORA | Nexus | JFrog |
|--------|------|-------|-------|
| Startup | < 3s | 30-60s | 30-60s |
| Memory | < 50 MB idle | 2-4 GB | 2-4 GB |
| Binary | < 30 MB | 600+ MB | 1+ GB |

## Roadmap

- ~~Mirror CLI~~ ✅ v0.4.0
- ~~Garbage Collection & Retention~~ ✅ v0.6.0
- ~~Helm Chart~~ ✅ v0.6.1
- ~~Signed releases & SBOM~~ ✅ v0.6.4
- ~~Curation layer & 13 registry formats~~ ✅ v0.7.0
- ~~Min Release Age~~ ✅ v0.7.1
- ~~Hash Pin Store, auth rate limiting, Cache-Control~~ ✅ v0.8.0
- ~~Outbound proxy, structured audit log~~ ✅ v0.8.3
- ~~Circuit breaker, OIDC, hot reload, arm64, streaming uploads~~ ✅ v0.9.0
- ~~NuGet V3 stabilization, Cargo ETag, 1049 tests~~ ✅ v0.9.1
- ~~Prometheus metrics, Ansible Galaxy v3, security fixes, 1086 tests~~ ✅ v0.9.2
- ~~Security hardening, null byte protection, config refactor, 1204 tests~~ ✅ v0.9.3
- ~~Multi-upstream PyPI, conditional-request revalidation, single-flight coalescing, per-registry metrics~~ ✅ v0.9.4
- ~~Digest quarantine across all registries, trusted upstream dates, token access-control hardening~~ ✅ v0.9.5
- **Image Signing Policy** — cosign verification on upstream pulls
- **Semver contract** — stable API, configuration format, and storage layout

See [ROADMAP.md](ROADMAP.md) for the full roadmap and [CHANGELOG.md](CHANGELOG.md) for release history.

## Security & Trust

[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/getnora-io/nora/badge)](https://scorecard.dev/viewer/?uri=github.com/getnora-io/nora)
[![CII Best Practices](https://www.bestpractices.dev/projects/12207/badge)](https://www.bestpractices.dev/projects/12207)
[![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/devitway/0f0538f1ed16d5d9951e4f2d3f79b699/raw/nora-coverage.json)](https://github.com/getnora-io/nora/actions/workflows/ci.yml)
[![CI](https://img.shields.io/github/actions/workflow/status/getnora-io/nora/ci.yml?label=CI)](https://github.com/getnora-io/nora/actions)

See [SECURITY.md](SECURITY.md) for vulnerability reporting.

## Documentation

Full documentation: **https://getnora.dev**

## Author

Created and maintained by [Pavel Volkov](https://github.com/devitway)

[![Docs](https://img.shields.io/badge/docs-getnora.dev-green?logo=gitbook)](https://getnora.dev)
[![Telegram](https://img.shields.io/badge/Telegram-Community-blue?logo=telegram)](https://t.me/getnora)
[![GitHub Stars](https://img.shields.io/github/stars/getnora-io/nora?style=flat&logo=github)](https://github.com/getnora-io/nora/stargazers)

## Contributing

NORA welcomes contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License — see [LICENSE](LICENSE)

Copyright (c) 2026 The NORA Authors
