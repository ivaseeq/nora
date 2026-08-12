import type { APIRequestContext } from '@playwright/test';
import { expect, test } from './fixtures/ui-test';
import { openUi } from './helpers/ui';

type Operation = {
  tags?: string[];
  deprecated?: boolean;
  requestBody?: {
    required?: boolean;
    content?: Record<string, { schema?: { type?: string; format?: string } }>;
  };
  responses?: Record<string, unknown>;
};

type OpenApiDocument = {
  tags?: Array<{ name?: string }>;
  paths: Record<string, Record<string, Operation>>;
};

async function openApiDocument(request: APIRequestContext): Promise<OpenApiDocument> {
  const response = await request.get('/api-docs/openapi.json');
  expect(response.ok()).toBe(true);
  return (await response.json()) as OpenApiDocument;
}

function operation(
  document: OpenApiDocument,
  path: string,
  method: string,
): Operation {
  const value = document.paths[path]?.[method];
  expect(value, `${method.toUpperCase()} ${path} must be documented`).toBeTruthy();
  return value!;
}

test('Maven and npm upload bodies are usable from generated clients', async ({ request }) => {
  const document = await openApiDocument(request);
  const legacyMaven = operation(document, '/maven2/{path}', 'put');
  const legacyNpm = operation(document, '/npm/{name}', 'put');
  const namedPut = operation(document, '/repository/{repository}/{path}', 'put');
  const namedPost = operation(document, '/repository/{repository}/{path}', 'post');

  expect(legacyMaven.requestBody?.required).toBe(true);
  expect(legacyMaven.requestBody?.content?.['application/octet-stream']?.schema).toMatchObject({
    type: 'string',
    format: 'binary',
  });
  expect(Object.keys(legacyNpm.requestBody?.content ?? {})).toEqual(['application/json']);
  expect(legacyNpm.requestBody?.required).toBe(true);

  expect(namedPut.requestBody?.required).toBe(true);
  expect(Object.keys(namedPut.requestBody?.content ?? {}).sort()).toEqual([
    'application/json',
    'application/octet-stream',
  ]);
  expect(namedPut.requestBody?.content?.['application/octet-stream']?.schema).toMatchObject({
    type: 'string',
    format: 'binary',
  });
  expect(namedPost.requestBody?.required).toBe(true);
  expect(Object.keys(namedPost.requestBody?.content ?? {})).toEqual(['application/json']);
});

test('named and compatibility operations have unambiguous tags and lifecycle', async ({
  request,
}) => {
  const document = await openApiDocument(request);
  expect(document.tags?.some((tag) => tag.name === 'repository')).toBe(false);

  expect(operation(document, '/repository/{repository}/{path}', 'get').tags).toEqual([
    'maven',
    'npm',
  ]);
  expect(operation(document, '/repository/{repository}/{path}', 'put').tags).toEqual([
    'maven',
    'npm',
  ]);
  expect(operation(document, '/repository/{repository}/{path}', 'post').tags).toEqual(['npm']);
  expect(operation(document, '/repository/{repository}/{path}', 'delete').tags).toEqual(['npm']);

  for (const [path, method] of [
    ['/maven2/{path}', 'get'],
    ['/maven2/{path}', 'put'],
    ['/npm/{name}', 'get'],
    ['/npm/{name}', 'put'],
  ] as const) {
    expect(operation(document, path, method).deprecated).toBe(true);
  }
});

test('protocol and persistent-index readiness are documented separately', async ({ request }) => {
  const document = await openApiDocument(request);
  expect(operation(document, '/health', 'get')).toBeTruthy();
  expect(operation(document, '/ready', 'get')).toBeTruthy();
  const indexReadiness = operation(document, '/ready/index', 'get');
  expect(indexReadiness.tags).toEqual(['health']);
  expect(indexReadiness.responses).toHaveProperty('200');
  expect(indexReadiness.responses).toHaveProperty('503');
});

test('Swagger initializes without contacting the public validator', async ({ page }) => {
  const validatorRequests: string[] = [];
  page.on('request', (request) => {
    if (new URL(request.url()).hostname === 'validator.swagger.io') {
      validatorRequests.push('/validator');
    }
  });

  await page.goto('/api-docs', { waitUntil: 'load' });
  await expect(page.locator('section.swagger-ui.swagger-container')).toBeVisible();
  await expect(page.locator('.opblock').first()).toBeVisible();
  expect(validatorRequests).toEqual([]);
});

test('Swagger renders the Maven binary body as a file chooser', async ({ page }) => {
  await page.goto('/api-docs', { waitUntil: 'load' });
  const summary = page
    .locator('button.opblock-summary-control')
    .filter({ hasText: 'PUT' })
    .filter({ hasText: '/maven2/{path}' });
  const operation = page
    .locator('.opblock')
    .filter({ has: summary })
    .first();
  await summary.click();
  await operation.getByRole('button', { name: /try it out/i }).click();
  await expect(operation.locator('input[type="file"]')).toBeVisible();
});

test('declared versioned favicon is a loadable SVG resource', async ({ page, request }) => {
  await openUi(page, '/ui/');
  const icon = page.locator('link[rel="icon"]');
  await expect(icon).toHaveAttribute('type', 'image/svg+xml');
  await expect(icon).toHaveAttribute('sizes', 'any');
  const href = await icon.getAttribute('href');
  expect(href).toMatch(/\/favicon\.svg\?v=.+/);

  const url = new URL(href!, page.url());
  const response = await request.get(url.toString());
  expect(response.ok()).toBe(true);
  expect(response.headers()['content-type']).toContain('image/svg+xml');
  expect((await response.body()).subarray(0, 200).toString('utf8')).toContain('<svg');
});
