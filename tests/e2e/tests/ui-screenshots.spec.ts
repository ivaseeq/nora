import { expect, test } from './fixtures/ui-test';
import { envPath, openSearchableList, openUi } from './helpers/ui';

const seeded = process.env.NORA_VISUAL_SEEDED === '1';
const npmDetail = envPath('NORA_E2E_NPM_DETAIL_PATH');
const mavenDetail = envPath('NORA_E2E_MAVEN_DETAIL_PATH');
const npmQuery = process.env.NORA_E2E_NPM_QUERY?.trim();

test.describe('deterministic FullHD visual contract', () => {
  test.skip(
    !seeded,
    'Visual goldens require NORA_VISUAL_SEEDED=1 and a pinned, deterministic dataset',
  );

  test.beforeEach(async ({ context, request }) => {
    await expect
      .poll(async () => (await request.get('/ready/index')).status(), {
        message: 'persistent Maven/npm index must be ready before visual capture',
        timeout: 90_000,
      })
      .toBe(200);
    await context.addCookies([
      {
        name: 'nora_lang',
        value: 'en',
        url: process.env.NORA_URL || 'http://localhost:4000',
      },
    ]);
  });

  test('dashboard', async ({ page }) => {
    await openUi(page, '/ui/');
    await expect(page.locator('#stat-downloads')).toBeVisible();
    await expect(page).toHaveScreenshot('dashboard-fullhd.png', {
      animations: 'disabled',
      caret: 'hide',
      mask: [
        page.locator('#uptime'),
        page.locator('#activity-log tbody tr td:first-child'),
      ],
      maskColor: '#1e293b',
      maxDiffPixels: 0,
    });
  });

  test('npm filtered list', async ({ page }) => {
    test.skip(!npmQuery, 'Set NORA_E2E_NPM_QUERY to a deterministic seeded package');
    const searchbox = await openSearchableList(page, '/ui/npm');
    const response = page.waitForResponse((candidate) => {
      const url = new URL(candidate.url());
      return url.pathname === '/api/ui/npm/search' && url.searchParams.get('q') === npmQuery;
    });
    await searchbox.fill(npmQuery!);
    await response;
    await expect(page.locator('#repo-results')).toHaveAttribute('aria-busy', 'false');
    await expect(page).toHaveScreenshot('npm-filtered-fullhd.png', {
      animations: 'disabled',
      caret: 'hide',
      mask: [page.locator('#repo-results tbody td:nth-child(4)')],
      maskColor: '#1e293b',
      maxDiffPixels: 0,
    });
  });

  test('npm detail', async ({ page }) => {
    test.skip(!npmDetail, 'Set NORA_E2E_NPM_DETAIL_PATH for the seeded package');
    await openUi(page, npmDetail!);
    await expect(page.locator('#install-cmd')).toBeVisible();
    await expect(page).toHaveScreenshot('npm-detail-fullhd.png', {
      animations: 'disabled',
      caret: 'hide',
      maxDiffPixels: 0,
    });
  });

  test('Maven artifact detail', async ({ page }) => {
    test.skip(!mavenDetail, 'Set NORA_E2E_MAVEN_DETAIL_PATH for the seeded coordinate');
    await openUi(page, mavenDetail!);
    await expect(page.locator('main a[href^="/repository/"]').first()).toBeVisible();
    await expect(page).toHaveScreenshot('maven-detail-fullhd.png', {
      animations: 'disabled',
      caret: 'hide',
      maxDiffPixels: 0,
    });
  });
});
