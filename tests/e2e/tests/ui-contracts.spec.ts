import AxeBuilder from '@axe-core/playwright';
import type { Locator, Page } from '@playwright/test';
import { expect, test } from './fixtures/ui-test';
import {
  INDEX_READY_TIMEOUT,
  envPath,
  openSearchableList,
  openUi,
  visibleDataRows,
} from './helpers/ui';

const npmDetailPath = envPath('NORA_E2E_NPM_DETAIL_PATH');
const mavenDetailPath = envPath('NORA_E2E_MAVEN_DETAIL_PATH');
const npmPrereleasePath = envPath('NORA_E2E_NPM_PRERELEASE_DETAIL_PATH');
const npmPaginatedQuery = process.env.NORA_E2E_NPM_PAGINATED_QUERY?.trim();
const expectIndexLoading = process.env.NORA_E2E_EXPECT_INDEX_LOADING === '1';

type Registry = 'maven' | 'npm';

async function expectSearchResultContract(page: Page, query: string): Promise<void> {
  const results = page.locator('#repo-results');
  const visibleStatus = page.locator('#repo-search-status');
  const announcement = page.locator('#repo-search-announcement');
  await expect(results).toHaveAttribute('aria-busy', 'false');
  await expect(visibleStatus).toBeVisible();
  await expect(announcement).toHaveAttribute('role', 'status');
  await expect(announcement).toHaveAttribute('aria-live', 'polite');

  const rowCount = await visibleDataRows(page).count();
  const countPattern = new RegExp(`\\b${rowCount}\\b`);
  await expect(visibleStatus).toContainText(countPattern);
  await expect(announcement).toContainText(countPattern);

  const nextLinks = results.getByRole('link', { name: /next/i });
  for (let index = 0; index < (await nextLinks.count()); index += 1) {
    const href = await nextLinks.nth(index).getAttribute('href');
    expect(href, 'search pagination must retain the active query').toBeTruthy();
    expect(new URL(href!, 'http://nora.invalid').searchParams.get('q')).toBe(query);
  }
}

async function waitForSearch(
  page: Page,
  registry: Registry,
  query: string,
  action: () => Promise<void>,
): Promise<void> {
  const responsePromise = page.waitForResponse((response) => {
    const url = new URL(response.url());
    return (
      response.request().method() === 'GET' &&
      url.pathname === `/api/ui/${registry}/search` &&
      (url.searchParams.get('q') ?? '') === query
    );
  });
  await action();
  const response = await responsePromise;
  expect(response.ok(), `search request failed with ${response.status()}`).toBe(true);
  await expectSearchResultContract(page, query);
}

async function searchCandidate(page: Page): Promise<string> {
  const link = visibleDataRows(page).first().getByRole('link').first();
  await expect(link).toBeVisible({ timeout: INDEX_READY_TIMEOUT });
  const label = (await link.innerText()).trim();
  expect(label, 'seeded UI target must contain a searchable row').not.toBe('');
  return label;
}

async function openNpmDetail(page: Page): Promise<void> {
  if (npmDetailPath) {
    await openUi(page, npmDetailPath);
  } else {
    await openSearchableList(page, '/ui/npm');
    const firstPackage = visibleDataRows(page).first().getByRole('link').first();
    await expect(firstPackage).toBeVisible({ timeout: INDEX_READY_TIMEOUT });
    await firstPackage.click();
  }
  await expect(page.locator('#install-cmd')).toBeVisible({ timeout: INDEX_READY_TIMEOUT });
}

async function openNamedMavenDetail(page: Page): Promise<Locator> {
  if (mavenDetailPath) {
    await openUi(page, mavenDetailPath);
    const configuredLinks = page.locator('main a[href^="/repository/"]');
    await expect(configuredLinks.first()).toBeVisible({ timeout: INDEX_READY_TIMEOUT });
    return configuredLinks;
  }

  await openSearchableList(page, '/ui/maven');
  const pending = await page
    .locator('main tbody a[href^="/ui/maven/"]')
    .evaluateAll((links) => links.map((link) => (link as HTMLAnchorElement).href));
  const visited = new Set<string>();

  while (pending.length > 0 && visited.size < 30) {
    const next = pending.pop()!;
    if (visited.has(next)) continue;
    visited.add(next);
    await page.goto(next, { waitUntil: 'load' });
    await expect(page.locator('main')).toBeVisible();

    const downloads = page.locator('main a[href^="/repository/"]');
    if ((await downloads.count()) > 0) return downloads;

    const children = await page
      .locator('main tbody a[href^="/ui/maven/"]')
      .evaluateAll((links) => links.map((link) => (link as HTMLAnchorElement).href));
    pending.push(...children.filter((child) => !visited.has(child)));
  }

  throw new Error(
    'No named Maven artifact page found; seed one or set NORA_E2E_MAVEN_DETAIL_PATH',
  );
}

function safeAxeSummary(
  violations: Awaited<ReturnType<AxeBuilder['analyze']>>['violations'],
): Array<{ id: string; impact: string | null; targets: string[][] }> {
  return violations.map((violation) => ({
    id: violation.id,
    impact: violation.impact,
    targets: violation.nodes.map((node) => node.target.map(String)),
  }));
}

test.describe('shared shell', () => {
  test('language selection is exposed and survives reload', async ({ page }) => {
    await openUi(page, '/ui/');
    const language = page.locator('header [role="group"]');
    await expect(language).toHaveAccessibleName(/\S/);
    const english = language.getByRole('button', { name: 'EN', exact: true });
    const russian = language.getByRole('button', { name: 'RU', exact: true });

    await expect(english).toHaveAttribute('aria-pressed', 'true');
    await Promise.all([page.waitForEvent('load'), russian.click()]);
    await expect(page.locator('html')).toHaveAttribute('lang', 'ru');
    await expect(language).toHaveAccessibleName(/\S/);
    await expect(language.getByRole('button', { name: 'RU', exact: true })).toHaveAttribute(
      'aria-pressed',
      'true',
    );

    await Promise.all([
      page.waitForEvent('load'),
      language.getByRole('button', { name: 'EN', exact: true }).click(),
    ]);
    await expect(page.locator('html')).toHaveAttribute('lang', 'en');
  });

  test('icon-only destinations have user-facing names', async ({ page }) => {
    await openUi(page, '/ui/');
    await expect(page.getByRole('link', { name: 'NORA on GitHub' })).toBeVisible();
    await expect(page.getByRole('link', { name: 'API documentation' })).toBeVisible();
  });

  test('screen-reader labels are visually clipped but remain programmatically named', async ({
    page,
  }) => {
    const searchbox = await openSearchableList(page, '/ui/npm');
    await expect(searchbox).toHaveAccessibleName(/search/i);
    const label = page.locator('label[for="repository-search"]');
    const presentation = await label.evaluate((element) => {
      const style = getComputedStyle(element);
      const box = element.getBoundingClientRect();
      return {
        position: style.position,
        overflow: style.overflow,
        width: box.width,
        height: box.height,
      };
    });
    expect(presentation.position).toBe('absolute');
    expect(presentation.overflow).toBe('hidden');
    expect(presentation.width).toBeLessThanOrEqual(1);
    expect(presentation.height).toBeLessThanOrEqual(1);
  });
});

for (const registry of ['maven', 'npm'] as const) {
  test.describe(`${registry} search`, () => {
    test('typing, fill and clear replace rows, status and pagination atomically', async ({
      page,
    }) => {
      const searchbox = await openSearchableList(page, `/ui/${registry}`);
      const query = await searchCandidate(page);

      await searchbox.focus();
      await waitForSearch(page, registry, query, () => searchbox.pressSequentially(query));
      await expect(searchbox).toBeFocused();

      await searchbox.press('ControlOrMeta+A');
      await waitForSearch(page, registry, '', () => searchbox.press('Backspace'));
      await expect(searchbox).toBeFocused();
      if (registry === 'maven') {
        await expect(page.locator('#repo-results thead th').nth(1)).toHaveText(/items/i);
        const itemCounts = await page
          .locator('#repo-results tbody tr:has(a) td:nth-child(2)')
          .allTextContents();
        expect(itemCounts.length).toBeGreaterThan(0);
        expect(itemCounts.every((value) => /^(?:—|0|[1-9]\d*)$/.test(value.trim()))).toBe(true);
      }

      await waitForSearch(page, registry, query, () => searchbox.fill(query));
      await expect(searchbox).toBeFocused();

      const missing = '__nora_e2e_known_missing_7e6bd214__';
      await waitForSearch(page, registry, missing, () => searchbox.fill(missing));
      await expect(visibleDataRows(page)).toHaveCount(0);
      await expect(searchbox).toBeFocused();

      await waitForSearch(page, registry, '', () => searchbox.fill(''));
      await expect(searchbox).toBeFocused();
      const nextLinks = page.locator('#repo-results').getByRole('link', { name: /next/i });
      for (let index = 0; index < (await nextLinks.count()); index += 1) {
        const href = await nextLinks.nth(index).getAttribute('href');
        expect(new URL(href!, 'http://nora.invalid').searchParams.get('q') ?? '').toBe('');
      }
    });

    test('paste triggers the same search flow', async ({ page, context, browserName }) => {
      test.skip(browserName !== 'chromium', 'Clipboard permissions are covered in Chromium');
      const searchbox = await openSearchableList(page, `/ui/${registry}`);
      const query = await searchCandidate(page);
      const origin = new URL(page.url()).origin;
      await context.grantPermissions(['clipboard-read', 'clipboard-write'], { origin });
      await page.evaluate((value) => navigator.clipboard.writeText(value), query);
      await searchbox.focus();
      await waitForSearch(page, registry, query, () => searchbox.press('ControlOrMeta+V'));
      await expect(searchbox).toBeFocused();
    });
  });
}

test.describe('search failure state', () => {
  test.use({
    browserIssueAllowlist: [
      [
        {
          kind: 'console',
          level: 'error',
          message: 'Response Status Error Code 503 from /api/ui/npm/search?[REDACTED]',
          source: '/ui/static/htmx.min.js',
        },
        {
          kind: 'console',
          level: 'error',
          message: 'Failed to load resource: the server responded with a status of 503 (Service Unavailable)',
          source: '/api/ui/npm/search',
        },
        { kind: 'http', method: 'GET', path: '/api/ui/npm/search', status: 503 },
      ],
      { scope: 'test' },
    ],
  });

  test('HTML error response replaces stale results and is announced', async ({ page }) => {
    await page.route('**/api/ui/npm/search**', async (route) => {
      await route.fulfill({
        status: 503,
        contentType: 'text/html; charset=utf-8',
        body: `<div id="repo-results" data-search-announcement="Search temporarily unavailable" aria-busy="false">
          <div role="alert"><p id="repo-search-status">Search temporarily unavailable</p>
          <a href="/ui/npm?q=failure&amp;limit=50&amp;lang=en">Reload results</a></div>
        </div>`,
      });
    });
    const searchbox = await openSearchableList(page, '/ui/npm');
    const responsePromise = page.waitForResponse(
      (response) =>
        new URL(response.url()).pathname === '/api/ui/npm/search' && response.status() === 503,
    );
    await searchbox.fill('failure');
    await responsePromise;

    await expect(page.getByRole('alert')).toContainText('Search temporarily unavailable');
    await expect(page.locator('#repo-search-status')).toHaveText('Search temporarily unavailable');
    await expect(page.locator('#repo-search-announcement')).toHaveText(
      'Search temporarily unavailable',
    );
    await expect(page.locator('#repo-results')).toHaveAttribute('aria-busy', 'false');
    await expect(searchbox).toBeFocused();
    const reload = page.getByRole('link', { name: 'Reload results' });
    await expect(reload).toHaveAttribute('href', /q=failure/);
  });
});

test('npm search Next uses normal navigation and Back restores the first page', async ({
  page,
}) => {
  test.skip(
    !npmPaginatedQuery,
    'Set NORA_E2E_NPM_PAGINATED_QUERY to a seeded query with at least two matches',
  );
  const firstPageUrl = `/ui/npm?q=${encodeURIComponent(npmPaginatedQuery!)}&limit=1`;
  const searchbox = await openSearchableList(page, firstPageUrl);
  await expect(searchbox).toHaveValue(npmPaginatedQuery!);
  const firstPage = await visibleDataRows(page).allTextContents();
  const next = page.locator('#repo-results').getByRole('link', { name: /next/i });
  await expect(next).toBeVisible();
  await next.focus();
  await expect(next).toBeFocused();

  await next.click();
  await expect(page).toHaveURL((url) => {
    return (
      url.pathname === '/ui/npm' &&
      url.searchParams.get('q') === npmPaginatedQuery &&
      url.searchParams.has('continuation_token')
    );
  });
  await expect(page.getByRole('searchbox')).toHaveValue(npmPaginatedQuery!);
  expect(await visibleDataRows(page).allTextContents()).not.toEqual(firstPage);

  await page.goBack();
  await expect(page).toHaveURL((url) =>
    url.pathname === '/ui/npm' &&
    url.searchParams.get('q') === npmPaginatedQuery &&
    !url.searchParams.has('continuation_token'),
  );
  await expect(page.getByRole('searchbox')).toHaveValue(npmPaginatedQuery!);
  await expect(visibleDataRows(page)).toHaveCount(firstPage.length);
});

test('index warm-up announces exact progress and refreshes when ready', async ({
  page,
  browserName,
}) => {
  test.skip(
    browserName !== 'chromium' || !expectIndexLoading,
    'Run on a cold target with NORA_E2E_EXPECT_INDEX_LOADING=1',
  );
  test.setTimeout(300_000);
  await openUi(page, '/ui/maven');
  const loading = page.locator('#index-loading-status');
  await expect(loading).toBeVisible();
  await expect(loading).toHaveAttribute('aria-busy', 'false');
  await expect(page.getByRole('status')).toHaveAttribute('aria-live', 'polite');
  await expect(loading.locator('dl dd')).toHaveCount(3);
  for (const counter of await loading.locator('dl dd').allTextContents()) {
    expect(counter.trim()).toMatch(/^\d+$/);
  }

  await page.evaluate(() => {
    const status = document.getElementById('index-loading-status');
    (window as Window & { __noraBusySeen?: string[] }).__noraBusySeen = [];
    if (!status) return;
    new MutationObserver(() => {
      const value = status.getAttribute('aria-busy') ?? '';
      (window as Window & { __noraBusySeen?: string[] }).__noraBusySeen?.push(value);
      if (value === 'true') sessionStorage.setItem('nora-e2e-index-busy-seen', 'true');
    }).observe(status, { attributes: true, attributeFilter: ['aria-busy'] });
  });
  await expect
    .poll(
      () =>
        page.evaluate(
          () => sessionStorage.getItem('nora-e2e-index-busy-seen') === 'true',
        ),
      { message: 'index polling must expose aria-busy=true while a request is active' },
    )
    .toBe(true);

  await expect(page.getByRole('searchbox')).toBeVisible({ timeout: 280_000 });
  await expect(page.locator('#index-loading-status')).toHaveCount(0);
});

test.describe('Maven detail', () => {
  test('filtered logical path crosses ingress with encoded slashes', async ({ page }) => {
    const searchbox = await openSearchableList(page, '/ui/maven');
    await waitForSearch(page, 'maven', '/', () => searchbox.fill('/'));
    let encodedDetail: Locator | undefined;

    for (let scanWindow = 0; scanWindow < 20; scanWindow += 1) {
      const candidate = page
        .locator('#repo-results a[href^="/ui/maven/"][href*="%2F" i]')
        .first();
      if ((await candidate.count()) > 0) {
        encodedDetail = candidate;
        break;
      }

      const next = page.locator('#repo-results').getByRole('link', { name: /next/i });
      if ((await next.count()) === 0) break;
      await Promise.all([page.waitForEvent('load'), next.click()]);
    }

    expect(
      encodedDetail,
      'live Maven seed needs an indexed nested path within 20 scan windows',
    ).toBeDefined();
    await expect(encodedDetail!).toBeVisible();
    const href = await encodedDetail!.getAttribute('href');
    expect(href, 'Maven logical names must remain one encoded path segment').toMatch(/%2f/i);

    const navigationResponse = page.waitForResponse(
      (response) =>
        response.request().isNavigationRequest() &&
        response.request().frame() === page.mainFrame(),
    );
    await encodedDetail!.click();
    const response = await navigationResponse;
    expect(response.ok(), `encoded Maven navigation failed with ${response.status()}`).toBe(true);
    await expect(page.locator('main')).toBeVisible();
    await expect(
      page.locator('main a[href^="/ui/maven/"], main a[href^="/repository/"]').first(),
    ).toBeVisible({ timeout: INDEX_READY_TIMEOUT });
  });

  test('artifact downloads use an explicit named repository', async ({ page }) => {
    const links = await openNamedMavenDetail(page);
    expect(await links.count()).toBeGreaterThan(0);
    for (let index = 0; index < (await links.count()); index += 1) {
      const href = await links.nth(index).getAttribute('href');
      expect(href).toMatch(/^\/repository\/[^/]+\/.+/);
      expect(href).not.toMatch(/^\/maven2\//);
    }
  });
});

test.describe('npm detail', () => {
  test('copy action exposes success feedback', async ({ page, context, browserName }) => {
    await openNpmDetail(page);
    if (browserName === 'chromium') {
      await context.grantPermissions(['clipboard-read', 'clipboard-write'], {
        origin: new URL(page.url()).origin,
      });
    }
    await page.getByRole('button', { name: /copy/i }).click();
    const status = page.getByRole('status');
    await expect(status).toHaveText(/\S+/);
    if (browserName === 'chromium') {
      await expect(status).toContainText(/copied|скопирован|已复制/i);
    }
  });

  test('version rows do not pretend to be interactive', async ({ page }) => {
    await openNpmDetail(page);
    let row = page.locator('.version-row').first();
    if ((await row.count()) === 0) {
      const showPrereleases = page.getByRole('link', { name: /pre-release/i });
      await expect(
        showPrereleases,
        'a package without stable rows must expose its indexed pre-release versions',
      ).toBeVisible();
      await showPrereleases.click();
      await expect(page).toHaveURL((url) => url.searchParams.get('prerelease') === 'true');
      row = page.locator('.version-row').first();
    }
    await expect(row).toBeVisible();
    const initialUrl = page.url();
    const initialCommand = await page.locator('#install-cmd').innerText();
    await row.click();
    await expect(page).toHaveURL(initialUrl);
    await expect(page.locator('#install-cmd')).toHaveText(initialCommand);
    await expect(row).not.toHaveAttribute('role', 'button');
    await expect(row).not.toHaveAttribute('tabindex', '0');
  });

  test('named package install command retains the repository', async ({ page }) => {
    await openNpmDetail(page);
    const command = await page.locator('#install-cmd').innerText();
    const path = new URL(page.url()).pathname;
    if (path.includes('/repositories/')) {
      expect(command).toMatch(/--registry\s+https?:\/\/[^\s]+\/repository\/[^\s]+/);
      expect(command).not.toMatch(/--registry\s+https?:\/\/[^\s]+\/npm(?:\s|$)/);
    }
  });

  test('pre-release view can return to stable versions', async ({ page }) => {
    test.skip(
      !npmPrereleasePath,
      'Set NORA_E2E_NPM_PRERELEASE_DETAIL_PATH to a seeded package with pre-releases',
    );
    await openUi(page, npmPrereleasePath!);
    const showPrereleases = page.getByRole('link', { name: /pre-release/i });
    await expect(showPrereleases).toBeVisible();
    await showPrereleases.click();
    await expect(page).toHaveURL((url) => url.searchParams.get('prerelease') === 'true');

    const showStable = page.getByRole('link', { name: /show stable versions/i });
    await expect(showStable).toBeVisible();
    await showStable.click();
    await expect(page).toHaveURL((url) => !url.searchParams.has('prerelease'));
    await expect(page.getByRole('link', { name: /pre-release/i })).toBeVisible();
  });
});

for (const path of ['/ui/', '/ui/maven', '/ui/npm']) {
  test(`WCAG 2.2 AA automated audit: ${path}`, async ({ page }) => {
    if (path === '/ui/') await openUi(page, path);
    else await openSearchableList(page, path);
    if (path !== '/ui/') {
      const firstLinkedRow = page
        .locator('main tbody tr')
        .filter({ has: page.getByRole('link') })
        .first();
      test.skip(
        (await firstLinkedRow.count()) === 0,
        'A seeded Maven/npm row is required for the hover-contrast audit',
      );
      await firstLinkedRow.locator('td').nth(1).hover();
      await expect(firstLinkedRow).toHaveCSS('background-color', 'rgba(51, 65, 85, 0.5)');
    }
    const results = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa'])
      .analyze();
    expect(safeAxeSummary(results.violations)).toEqual([]);
  });
}
