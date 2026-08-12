import type { Locator, Page } from '@playwright/test';
import { expect } from '../fixtures/ui-test';

export const INDEX_READY_TIMEOUT = 90_000;

export async function openUi(page: Page, path: string): Promise<void> {
  await page.goto(path, { waitUntil: 'load' });
  await expect(page.locator('main')).toBeVisible();
}

export async function openSearchableList(page: Page, path: string): Promise<Locator> {
  await openUi(page, path);
  const searchbox = page.getByRole('searchbox');
  await expect(searchbox).toBeVisible({ timeout: INDEX_READY_TIMEOUT });
  return searchbox;
}

export function visibleDataRows(page: Page): Locator {
  return page.locator('#repo-results tbody tr').filter({ has: page.getByRole('link') });
}

export async function expectNoDocumentOverflow(page: Page): Promise<void> {
  await expect
    .poll(() =>
      page.evaluate(() => {
        const regions = [
          ['html', document.documentElement],
          ['body', document.body],
          ['#app-content', document.getElementById('app-content')],
          ['main', document.querySelector('main')],
        ] as const;
        return regions.flatMap(([name, element]) => {
          if (!(element instanceof HTMLElement) || element.scrollWidth <= element.clientWidth + 1) {
            return [];
          }
          return [`${name}(${element.scrollWidth}px > ${element.clientWidth}px)`];
        });
      }),
    )
    .toEqual([]);
}

export async function expectAccessibleHorizontalScrollers(page: Page): Promise<void> {
  const unnamedScrollableRegions = await page.locator('main').evaluate((main) =>
    [main, ...Array.from(main.querySelectorAll<HTMLElement>('*'))]
      .filter(
        (element) =>
          element.scrollWidth > element.clientWidth + 1 &&
          ['auto', 'scroll'].includes(getComputedStyle(element).overflowX),
      )
      .filter(
        (element) =>
          !element.getAttribute('aria-label') && !element.getAttribute('aria-labelledby'),
      )
      .map((element) => `${element.tagName.toLowerCase()}#${element.id || '[no-id]'}`),
  );
  expect(unnamedScrollableRegions, 'local horizontal scrollers need an accessible name').toEqual(
    [],
  );
}

export function envPath(name: string): string | undefined {
  const value = process.env[name]?.trim();
  if (!value) return undefined;
  return value.startsWith('/') ? value : `/${value}`;
}
