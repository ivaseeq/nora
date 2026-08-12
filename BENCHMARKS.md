# Benchmarks

Reproducible performance figures for NORA.

> **Scope.** The figures below cover what CI measures today on each release:
> binary size, cold-start time, and idle memory, plus Criterion micro-benchmarks.
> Load-test figures — throughput, request latency, and memory under concurrent
> load — are **not yet measured**. A read-only Maven/npm k6 harness is available,
> but no accepted run establishes those figures; Docker/raw coverage and CI
> integration remain pending in #693. The rows under "Targets" are goals only.

## Measured (v0.9.0)

| Metric | Value |
|--------|-------|
| Cold start | < 3 s |
| RAM (idle) | < 50 MB |
| Binary size | ~23 MB |

> Measured in CI on GitHub Actions (`ubuntu-latest`, 2 vCPU, 4 GB RAM) for v0.9.0;
> current per-release values are attached as release artifacts. See methodology below.

## Targets (not yet measured — see #693)

These remain unverified. The Maven/npm harness does not by itself establish that
any target has been met, and Docker coverage is still pending:

| Metric | Target |
|--------|--------|
| RAM (100 concurrent pulls) | < 100 MB |
| Docker pull p95 (cached) | < 50 ms |
| npm install p95 (cached) | < 30 ms |

## Methodology

On each release, the `benchmarks.yml` workflow measures:

- **Cold start** — wall-clock from `nora serve` to the first successful `/health` response.
- **Idle memory** — `VmRSS` from `/proc/PID/status` after startup, with no requests.
- **Binary size** — `stat` of the stripped release binary.
- **Micro-benchmarks** — `cargo bench -p nora-registry` (parsing and validation).

Hardware: 2 vCPU, 4 GB RAM (GitHub Actions `ubuntu-latest` or equivalent). Results
are attached as release artifacts.

### Throughput and latency (partial harness — #693)

Current harness coverage:

| Operation | Scenario | Status |
|-----------|----------|--------|
| Docker pull (manifest) | GET `/v2/{name}/manifests/{tag}` — cached images | Pending |
| Docker pull (blob) | GET `/v2/{name}/blobs/{digest}` — 10 MB layer | Pending |
| npm install | GET packument + requested-version tarball through the ingress | Implemented |
| Maven resolve | GET an artifact path through the ingress | Implemented |
| Raw upload | PUT `/raw/{file}` — 1–100 KB files | Pending |

The Maven/npm harness reports request rate plus p50, p95, and p99 latency. It has
correctness thresholds only; it intentionally defines no latency SLO.

### Storage overhead

An architectural property, not a load measurement: NORA stores raw, content-addressable
files, so identical blobs are stored once.

| Solution | 1000 Docker images | Overhead |
|----------|-------------------|----------|
| NORA (local) | Raw files on disk | ~0% (content-addressable dedup) |
| NORA (S3) | S3 objects | ~0% |
| DB-backed registry | DB + filesystem | indexes, WAL, metadata tables |

### Backup / restore

NORA's data is plain files, so backup and restore are a file copy — no database dumps,
no index rebuilds, no multi-step procedures.

| Operation | Command |
|-----------|---------|
| Backup | `cp -r /data/ backup/` or `nora backup` |
| Restore | `cp -r backup/ /data/` or `nora restore` |

## Running benchmarks locally

### Micro-benchmarks (Criterion)

```bash
cargo bench -p nora-registry
```

Runs parsing and validation benchmarks. Results in `target/criterion/`.

### Load tests

The current harness performs GET requests only. It uses a digest-pinned k6 image,
defaults to 10 Maven VUs × 20 iterations and 10 npm VUs × 10 iterations, and sets
each scenario's `maxDuration` to five minutes:

```bash
BASE_URL=https://nora.example.test \
MAVEN_PATH=/repository/maven-group/com/example/app/1.0/app-1.0.jar \
NPM_PACKAGE_PATH=/repository/npm-group/example-package \
NPM_VERSION=1.0.0 \
./scripts/load-test.sh
```

Override the finite workload with `MAVEN_VUS`, `MAVEN_ITERATIONS`, `NPM_VUS`,
and `NPM_ITERATIONS`. Optional `EXPECTED_MAVEN_SHA256`,
`EXPECTED_NPM_PACKUMENT_SHA256`, and `EXPECTED_NPM_TARBALL_SHA256` values verify
the three response bodies; the artifact and tarball checks are binary-safe.

Docker/raw scenarios and baseline regression comparison are not implemented yet.

## CI integration

The `benchmarks.yml` workflow runs on each release:

1. Builds the release binary
2. Records binary size, cold-start time, and idle memory
3. Runs the Criterion micro-benchmarks
4. Uploads a JSON report as a release artifact

Load testing is not part of this workflow. Maven/npm can be exercised manually with
the read-only harness above; Docker/raw coverage and baseline regression comparison
remain pending in #693. No performance target is currently recorded as met.

## Historical results

Release reports are published as GitHub Release artifacts:

```bash
gh release download <tag> --pattern 'bench-*.json' --dir /tmp/<tag>
```
