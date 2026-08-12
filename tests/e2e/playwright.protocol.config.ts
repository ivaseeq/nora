import { defineConfig } from '@playwright/test';

const uiAudit = /ui-(contracts|openapi|responsive|screenshots)\.spec\.ts/;

if (process.env.NORA_ALLOW_E2E_WRITES !== '1') {
  throw new Error(
    'Protocol tests mutate their target. Set NORA_ALLOW_E2E_WRITES=1 only for an approved disposable environment.',
  );
}

/**
 * Explicit opt-in for the historical protocol suites. These tests publish and
 * reindex data, so they are deliberately excluded from the read-only UI config.
 */
export default defineConfig({
  testDir: './tests',
  testIgnore: uiAudit,
  timeout: 30_000,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 1 : 0,
  use: {
    baseURL: process.env.NORA_URL || 'http://localhost:4000',
    ignoreHTTPSErrors: true,
    screenshot: 'only-on-failure',
    trace: 'on-first-retry',
  },
  projects: [{ name: 'protocol-chromium', use: { browserName: 'chromium' } }],
});
