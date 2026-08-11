import { defineConfig } from '@playwright/test';

const uiFunctional = /ui-(contracts|openapi)\.spec\.ts/;
const uiResponsive = /ui-responsive\.spec\.ts/;
const uiVisual = /ui-screenshots\.spec\.ts/;

export default defineConfig({
  testDir: './tests',
  timeout: 45_000,
  expect: { timeout: 10_000 },
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI
    ? [['line'], ['html', { open: 'never' }]]
    : [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL: process.env.NORA_URL || 'http://localhost:4000',
    ignoreHTTPSErrors: true,
    locale: 'en-US',
    timezoneId: 'UTC',
    screenshot: 'only-on-failure',
    trace: 'on-first-retry',
  },
  projects: [
    {
      name: 'chromium-fullhd',
      testMatch: uiFunctional,
      use: {
        browserName: 'chromium',
        viewport: { width: 1920, height: 1080 },
      },
    },
    {
      name: 'chromium-1536',
      testMatch: uiResponsive,
      use: {
        browserName: 'chromium',
        viewport: { width: 1536, height: 864 },
      },
    },
    {
      name: 'chromium-mobile-390',
      testMatch: uiResponsive,
      use: {
        browserName: 'chromium',
        viewport: { width: 390, height: 844 },
        hasTouch: true,
        isMobile: true,
      },
    },
    {
      name: 'chromium-landscape-844',
      testMatch: uiResponsive,
      use: {
        browserName: 'chromium',
        viewport: { width: 844, height: 390 },
        hasTouch: true,
        isMobile: true,
      },
    },
    {
      name: 'chromium-reflow-320',
      testMatch: uiResponsive,
      use: {
        browserName: 'chromium',
        viewport: { width: 320, height: 800 },
        hasTouch: true,
        isMobile: true,
      },
    },
    {
      name: 'firefox-functional',
      testMatch: uiFunctional,
      use: {
        browserName: 'firefox',
        viewport: { width: 1920, height: 1080 },
      },
    },
    {
      name: 'webkit-functional',
      testMatch: uiFunctional,
      use: {
        browserName: 'webkit',
        viewport: { width: 1920, height: 1080 },
      },
    },
    {
      name: 'visual-chromium-fullhd',
      testMatch: uiVisual,
      use: {
        browserName: 'chromium',
        viewport: { width: 1920, height: 1080 },
        colorScheme: 'dark',
        reducedMotion: 'reduce',
      },
    },
  ],
});
