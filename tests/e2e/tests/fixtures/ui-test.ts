import {
  expect,
  test as base,
  type ConsoleMessage,
  type Page,
  type Request,
  type Response,
} from '@playwright/test';

type BrowserIssue =
  | {
      kind: 'console';
      level: 'warning' | 'error';
      message: string;
      source: string;
    }
  | { kind: 'pageerror'; message: string }
  | { kind: 'requestfailed'; method: string; path: string; error: string }
  | { kind: 'http'; method: string; path: string; status: number };

type UiFixtures = {
  /**
   * An exact, per-test allowlist. Query strings, headers and bodies never enter
   * collected diagnostics, so failures can be attached without leaking tokens.
   */
  browserIssueAllowlist: BrowserIssue[];
  _browserIssueGate: void;
};

const REDACTIONS: RegExp[] = [
  /\bBearer\s+[A-Za-z0-9._~+/-]+=*/gi,
  /\b(Basic)\s+[A-Za-z0-9+/]+=*/gi,
  /\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b/g,
  /\b(?:github_pat_|gh[pousr]_)[A-Za-z0-9_]{20,}\b/gi,
  /\b(?:AKIA|ASIA)[A-Z0-9]{16}\b/g,
  /\b(authorization|cookie|password|secret|token|api[-_]?key)\s*[:=]\s*[^\s,;]+/gi,
];

function safeText(value: string): string {
  let output = value.replace(
    /https?:\/\/[^\s"')]+/gi,
    (raw) => {
      try {
        const url = new URL(raw);
        return `${url.origin}${url.pathname}`;
      } catch {
        return '[URL]';
      }
    },
  );
  output = output.replace(
    /(\/[A-Za-z0-9._~!$&'()*+,;=:@%/-]+)\?[^\s"')]+/g,
    '$1?[REDACTED]',
  );
  for (const pattern of REDACTIONS) output = output.replace(pattern, '[REDACTED]');
  return output.slice(0, 500);
}

function safePath(value: string): string {
  try {
    return new URL(value).pathname;
  } catch {
    return value.startsWith('/') ? value.split(/[?#]/, 1)[0] : '[unknown]';
  }
}

function isSameOrigin(value: string, origin: string): boolean {
  try {
    return new URL(value).origin === origin;
  } catch {
    return false;
  }
}

function issueKey(issue: BrowserIssue): string {
  return JSON.stringify(issue);
}

function recordConsole(
  issues: BrowserIssue[],
  message: ConsoleMessage,
  origin: string,
): void {
  const level = message.type();
  if (level !== 'warning' && level !== 'error') return;
  const sourceUrl = message.location().url;
  if (sourceUrl && !isSameOrigin(sourceUrl, origin)) return;
  issues.push({
    kind: 'console',
    level,
    message: safeText(message.text()),
    source: sourceUrl ? safePath(sourceUrl) : '[document]',
  });
}

function recordFailedRequest(
  issues: BrowserIssue[],
  request: Request,
  origin: string,
): void {
  if (!isSameOrigin(request.url(), origin)) return;
  issues.push({
    kind: 'requestfailed',
    method: request.method(),
    path: safePath(request.url()),
    error: safeText(request.failure()?.errorText ?? 'unknown request failure'),
  });
}

function recordErrorResponse(
  issues: BrowserIssue[],
  response: Response,
  origin: string,
): void {
  if (response.status() < 400 || !isSameOrigin(response.url(), origin)) return;
  issues.push({
    kind: 'http',
    method: response.request().method(),
    path: safePath(response.url()),
    status: response.status(),
  });
}

function installIssueGate(page: Page, origin: string, issues: BrowserIssue[]): void {
  page.on('console', (message) => recordConsole(issues, message, origin));
  page.on('pageerror', (error) => {
    issues.push({ kind: 'pageerror', message: safeText(error.message) });
  });
  page.on('requestfailed', (request) => recordFailedRequest(issues, request, origin));
  page.on('response', (response) => recordErrorResponse(issues, response, origin));
}

export const test = base.extend<UiFixtures>({
  browserIssueAllowlist: [[], { option: true }],
  _browserIssueGate: [
    async ({ page, baseURL, browserIssueAllowlist }, use, testInfo) => {
      if (!baseURL) throw new Error('UI audit requires Playwright use.baseURL');
      const origin = new URL(baseURL).origin;
      const issues: BrowserIssue[] = [];
      installIssueGate(page, origin, issues);

      await use();

      const allowed = new Set(browserIssueAllowlist.map(issueKey));
      const unexpected = issues.filter((issue) => !allowed.has(issueKey(issue)));
      if (unexpected.length > 0) {
        await testInfo.attach('browser-issues.json', {
          body: Buffer.from(JSON.stringify(unexpected, null, 2)),
          contentType: 'application/json',
        });
      }
      expect(unexpected, 'unexpected same-origin browser/network issues').toEqual([]);
    },
    { auto: true },
  ],
});

export { expect } from '@playwright/test';
export type { BrowserIssue };
