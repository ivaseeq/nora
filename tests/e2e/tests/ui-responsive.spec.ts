import AxeBuilder from '@axe-core/playwright';
import { expect, test } from './fixtures/ui-test';
import {
  INDEX_READY_TIMEOUT,
  envPath,
  expectAccessibleHorizontalScrollers,
  expectNoDocumentOverflow,
  openSearchableList,
  openUi,
  visibleDataRows,
} from './helpers/ui';

const longScopedNpmPath = process.env.NORA_E2E_NPM_LONG_SCOPED_DETAIL_PATH?.trim();
const mavenDetailPath = envPath('NORA_E2E_MAVEN_DETAIL_PATH');

for (const path of ['/ui/', '/ui/maven', '/ui/npm']) {
  test(`@responsive ${path} reflows without document-level horizontal scrolling`, async ({
    page,
  }) => {
    if (path === '/ui/') await openUi(page, path);
    else await openSearchableList(page, path);
    await expectNoDocumentOverflow(page);
    await expectAccessibleHorizontalScrollers(page);
  });
}

test('@responsive seeded Maven artifact detail preserves the overflow contract', async ({
  page,
}) => {
  test.skip(!mavenDetailPath, 'Set NORA_E2E_MAVEN_DETAIL_PATH to a seeded artifact detail');
  await openUi(page, mavenDetailPath!);
  await expect(page.locator('main a[href^="/repository/"]').first()).toBeVisible({
    timeout: INDEX_READY_TIMEOUT,
  });
  await expectNoDocumentOverflow(page);
  await expectAccessibleHorizontalScrollers(page);
});

test('@responsive npm command and copy control remain in the viewport', async ({ page }) => {
  await openSearchableList(page, '/ui/npm');
  const firstPackage = visibleDataRows(page).first().getByRole('link').first();
  await expect(firstPackage).toBeVisible({ timeout: INDEX_READY_TIMEOUT });
  await firstPackage.click();

  const command = page.locator('#install-cmd');
  const copy = page.getByRole('button', { name: /copy/i });
  await expect(command).toBeVisible({ timeout: INDEX_READY_TIMEOUT });
  await expect(copy).toBeInViewport();
  await expectNoDocumentOverflow(page);
  await expectAccessibleHorizontalScrollers(page);
});

test('@responsive mobile navigation is modal, keyboard operable and restores focus', async ({
  page,
}) => {
  test.skip((page.viewportSize()?.width ?? 1920) >= 768, 'mobile navigation is hidden on desktop');
  await openUi(page, '/ui/');

  const opener = page.locator('button[data-sidebar-control]:not([data-sidebar-close])');
  const sidebar = page.locator('#sidebar');
  const appContent = page.locator('#app-content');
  await expect(opener).toHaveAccessibleName('Open navigation menu');
  await expect(opener).toHaveAttribute('aria-expanded', 'false');
  await expect(opener).toHaveAttribute('aria-controls', 'sidebar');
  await expect(sidebar).toHaveAttribute('inert', '');
  await expect(sidebar).toHaveAttribute('aria-hidden', 'true');
  await expect(sidebar).not.toHaveAttribute('role', 'dialog');
  await expect(sidebar).not.toHaveAttribute('aria-modal', 'true');

  await opener.click();
  const close = sidebar.getByRole('button', { name: 'Close navigation menu' });
  await expect(close).toBeFocused();
  await expect(opener).toHaveAttribute('aria-expanded', 'true');
  await expect(sidebar).not.toHaveAttribute('inert', '');
  await expect(sidebar).not.toHaveAttribute('aria-hidden', 'true');
  await expect(sidebar).toHaveAttribute('role', 'dialog');
  await expect(sidebar).toHaveAttribute('aria-modal', 'true');
  await expect(appContent).toHaveAttribute('inert', '');
  await expect(appContent).toHaveAttribute('aria-hidden', 'true');

  const tabbableCount = await sidebar
    .locator(
      [
        'a[href]',
        'button:not([disabled])',
        'input:not([disabled])',
        'select:not([disabled])',
        'textarea:not([disabled])',
        '[tabindex]:not([tabindex="-1"])',
      ].join(','),
    )
    .evaluateAll(
      (elements) =>
        elements.filter((element) => {
          const node = element as HTMLElement;
          const style = getComputedStyle(node);
          const rect = node.getBoundingClientRect();
          return (
            !node.closest('[inert]') &&
            style.visibility !== 'hidden' &&
            style.display !== 'none' &&
            rect.width > 0 &&
            rect.height > 0
          );
        }).length,
    );
  expect(tabbableCount, 'the open sidebar needs a non-empty keyboard focus cycle').toBeGreaterThan(
    1,
  );

  for (const key of ['Tab', 'Shift+Tab']) {
    for (let step = 0; step < tabbableCount; step += 1) {
      await page.keyboard.press(key);
      const focus = await page.evaluate(() => ({
        insideSidebar: document.activeElement?.closest('#sidebar') !== null,
        tag: document.activeElement?.tagName.toLowerCase() ?? 'none',
        id: document.activeElement?.id ?? '',
      }));
      expect(
        focus.insideSidebar,
        `${key} step ${step + 1}/${tabbableCount} focused ${focus.tag}#${focus.id || '[no-id]'} outside the open sidebar`,
      ).toBe(true);
    }
  }

  const axe = await new AxeBuilder({ page })
    .withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa'])
    .analyze();
  expect(
    axe.violations.map((violation) => ({
      id: violation.id,
      impact: violation.impact,
      targets: violation.nodes.map((node) => node.target.map(String)),
    })),
  ).toEqual([]);

  await page.keyboard.press('Escape');
  await expect(opener).toBeFocused();
  await expect(opener).toHaveAttribute('aria-expanded', 'false');
  await expect(sidebar).toHaveAttribute('inert', '');
  await expect(sidebar).toHaveAttribute('aria-hidden', 'true');
  await expect(sidebar).not.toHaveAttribute('role', 'dialog');
  await expect(sidebar).not.toHaveAttribute('aria-modal', 'true');
  await expect(appContent).not.toHaveAttribute('inert', '');
  await expect(appContent).not.toHaveAttribute('aria-hidden', 'true');
});

test('@responsive mobile navigation preserves focus across breakpoint changes', async ({
  page,
}) => {
  const mobileViewport = page.viewportSize();
  test.skip((mobileViewport?.width ?? 1920) >= 768, 'mobile navigation is hidden on desktop');
  await openUi(page, '/ui/');

  const opener = page.locator('button[data-sidebar-control]:not([data-sidebar-close])');
  const sidebar = page.locator('#sidebar');
  await opener.click();
  await expect(sidebar.getByRole('button', { name: 'Close navigation menu' })).toBeFocused();

  await page.setViewportSize({ width: 1024, height: 800 });
  const desktopTarget = sidebar.locator('a[aria-current="page"]');
  await expect(sidebar).not.toHaveAttribute('role', 'dialog');
  await expect(sidebar).not.toHaveAttribute('aria-modal', 'true');
  await expect(sidebar).not.toHaveAttribute('inert', '');
  await expect(desktopTarget).toBeVisible();
  await expect(desktopTarget).toBeFocused();

  await page.setViewportSize(mobileViewport!);
  await expect(opener).toBeVisible();
  await expect(opener).toBeFocused();
  await expect(sidebar).toHaveAttribute('inert', '');
  await expect(sidebar).toHaveAttribute('aria-hidden', 'true');
});

test('@responsive long scoped npm details reflow at narrow widths', async ({ page }) => {
  test.skip(
    (page.viewportSize()?.width ?? 1920) > 390 || !longScopedNpmPath,
    'Set NORA_E2E_NPM_LONG_SCOPED_DETAIL_PATH to a seeded long @scope/package detail',
  );
  await openUi(
    page,
    longScopedNpmPath!.startsWith('/') ? longScopedNpmPath! : `/${longScopedNpmPath}`,
  );
  await expect(page.locator('#install-cmd')).toContainText('@');
  await expect(page.locator('#install-cmd')).toHaveCSS('white-space', 'pre-wrap');
  await expect(page.getByRole('button', { name: /copy/i })).toBeInViewport();
  await expectNoDocumentOverflow(page);
  await expectAccessibleHorizontalScrollers(page);
});
