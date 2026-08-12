# NORA UI audit

This suite is a read-only browser audit of the shared shell plus Maven and npm UI. It never
publishes artifacts or seeds S3/MinIO; prepare deterministic data before the run.

## Install and inspect

```sh
npm ci
npx playwright install chromium firefox webkit
npm run test:list
npm run check:css
```

The lock pins Playwright 1.58.2 and axe-core 4.13.0. Functional coverage runs in Chromium,
Firefox and WebKit at 1920x1080. Chromium additionally checks 1536x864, 390x844, phone
landscape at 844x390 and WCAG reflow at 320x800. `check:css` regenerates Tailwind into a
temporary directory and fails on drift without overwriting the embedded asset; after intentional
template changes, refresh it explicitly with `npm run build:css` and review the diff.

`test:list` proves discovery and TypeScript loading only. The executable local
Chromium smoke used by CI additionally requires the NORA binary and browser:

```sh
cargo build --locked -p nora-registry
cd tests/e2e
npx playwright install chromium
npm run test:smoke:local
```

It starts an isolated local-storage NORA on `127.0.0.1:14080`, executes the
Swagger/OpenAPI/favicon contracts, then terminates the server and removes its
temporary data. Override a local port conflict with `NORA_UI_SMOKE_PORT`.

## Live ingress smoke

```sh
NORA_URL=https://nora.example.test npm run test:functional
NORA_URL=https://nora.example.test npm run test:responsive
NORA_URL=https://nora.example.test npm run test:headed
```

`test:headed` opens the primary 1920x1080 Chromium project in a visible browser window.

For repeatable detail-page selection, set `NORA_E2E_NPM_DETAIL_PATH` and
`NORA_E2E_MAVEN_DETAIL_PATH` to existing `/ui/...` paths. Without them, the functional audit
discovers read-only candidates from the current index. A live run gates same-origin console
warnings/errors, page errors, failed requests and HTTP 4xx/5xx responses; any exception must be
an exact per-test allowlist entry. axe-core covers WCAG 2.2 AA rules, but manual keyboard,
screen-reader, zoom, forced-colors and touch checks remain release gates.

Seeded edge-state gates are opt-in: `NORA_E2E_NPM_PAGINATED_QUERY` must match at least two
packages, `NORA_E2E_NPM_PRERELEASE_DETAIL_PATH` must contain stable and pre-release versions,
`NORA_E2E_NPM_LONG_SCOPED_DETAIL_PATH` must be a long scoped package, and
`NORA_E2E_EXPECT_INDEX_LOADING=1` is only for a deliberately cold index that will become ready.

The historical protocol specs are not part of this read-only audit. They publish packages,
upload artifacts and trigger reindexing, so they require a disposable approved target and the
explicit command `NORA_ALLOW_E2E_WRITES=1 npm run test:protocol:write`.

## Deterministic visual lane

Visual baselines are valid only against a pinned browser/OS and a pre-seeded, immutable target:

```sh
NORA_URL=https://nora-seeded.example.test \
NORA_VISUAL_SEEDED=1 \
NORA_E2E_NPM_QUERY=e2e-ui-npm \
NORA_E2E_NPM_DETAIL_PATH=/ui/npm/repositories/npm-private/e2e-ui-npm \
NORA_E2E_MAVEN_DETAIL_PATH=/ui/maven/maven-releases/com/e2e/ui-test/1.0 \
npm run test:visual -- --update-snapshots
```

Review generated images before committing them, then rerun without `--update-snapshots`.
Until reviewed snapshot files exist, a skipped visual project is not a passed visual gate.
Snapshots use strict zero-diff comparison only in pinned Chromium FullHD. Masks are limited to
proven volatile fields: dashboard uptime/activity timestamps and the relative Updated cells in the
npm result list. Live data, counters, timestamps and index progress change legitimately, so live
screenshots are diagnostics, never goldens. CI retries once, records a trace on the first retry,
and captures screenshots only on failure.
