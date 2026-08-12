import { check, fail } from 'k6';
import crypto from 'k6/crypto';
import http from 'k6/http';
import { Trend } from 'k6/metrics';

const MAX_DURATION = '5m';

function requiredEnv(name) {
  const value = __ENV[name];
  if (value === undefined || value.trim() === '') {
    throw new Error(`${name} is required`);
  }
  return value.trim();
}

function positiveIntegerEnv(name, fallback) {
  const value = __ENV[name];
  if (value === undefined || value === '') {
    return fallback;
  }
  if (!/^[1-9][0-9]*$/.test(value)) {
    throw new Error(`${name} must be a positive integer`);
  }

  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`${name} is too large`);
  }
  return parsed;
}

function expectedIterationCount(name, vus, iterations) {
  const count = vus * iterations;
  if (!Number.isSafeInteger(count)) {
    throw new Error(`${name} VUs multiplied by iterations is too large`);
  }
  return count;
}

function expectedSha256Env(name) {
  const value = __ENV[name];
  if (value === undefined || value === '') {
    return null;
  }
  if (!/^[0-9a-fA-F]{64}$/.test(value)) {
    throw new Error(`${name} must contain exactly 64 hexadecimal characters`);
  }
  return value.toLowerCase();
}

function originOf(url, name) {
  const match = /^(https?):\/\/(\[[0-9a-f:.]+\]|[^/?#:]+)(?::([0-9]{1,5}))?(?:[/?#]|$)/i.exec(url);
  if (match === null) {
    throw new Error(`${name} must be an absolute HTTP(S) URL without credentials`);
  }

  const scheme = match[1].toLowerCase();
  const host = match[2].toLowerCase();
  if (host.includes('@')) {
    throw new Error(`${name} must be an absolute HTTP(S) URL without credentials`);
  }
  const port = match[3] === undefined ? null : Number(match[3]);
  if (port !== null && (port < 1 || port > 65535)) {
    throw new Error(`${name} contains an invalid port`);
  }

  const isDefaultPort = (scheme === 'http' && port === 80) || (scheme === 'https' && port === 443);
  return `${scheme}://${host}${port !== null && !isDefaultPort ? `:${port}` : ''}`;
}

function requestPath(name) {
  const value = requiredEnv(name);
  if (/^[a-z][a-z0-9+.-]*:\/\//i.test(value) || value.includes('?') || value.includes('#')) {
    throw new Error(`${name} must be a path without a query or fragment`);
  }

  const normalized = `/${value.replace(/^\/+/, '')}`;
  if (normalized === '/') {
    throw new Error(`${name} must not be empty`);
  }
  return normalized;
}

const baseUrlInput = requiredEnv('BASE_URL');
if (baseUrlInput.includes('?') || baseUrlInput.includes('#')) {
  throw new Error('BASE_URL must not contain a query or fragment');
}

const baseUrl = baseUrlInput.replace(/\/+$/, '');
const baseOrigin = originOf(baseUrl, 'BASE_URL');
const mavenUrl = `${baseUrl}${requestPath('MAVEN_PATH')}`;
const npmPackumentUrl = `${baseUrl}${requestPath('NPM_PACKAGE_PATH')}`;
const npmVersion = requiredEnv('NPM_VERSION');

const mavenVus = positiveIntegerEnv('MAVEN_VUS', 10);
const mavenIterations = positiveIntegerEnv('MAVEN_ITERATIONS', 20);
const npmVus = positiveIntegerEnv('NPM_VUS', 10);
const npmIterations = positiveIntegerEnv('NPM_ITERATIONS', 10);
const expectedMavenIterations = expectedIterationCount('Maven', mavenVus, mavenIterations);
const expectedNpmIterations = expectedIterationCount('npm', npmVus, npmIterations);

const expectedMavenSha256 = expectedSha256Env('EXPECTED_MAVEN_SHA256');
const expectedNpmPackumentSha256 = expectedSha256Env('EXPECTED_NPM_PACKUMENT_SHA256');
const expectedNpmTarballSha256 = expectedSha256Env('EXPECTED_NPM_TARBALL_SHA256');

const mavenDuration = new Trend('maven_get_duration', true);
const npmPackumentDuration = new Trend('npm_packument_get_duration', true);
const npmTarballDuration = new Trend('npm_tarball_get_duration', true);

export const options = {
  scenarios: {
    maven: {
      executor: 'per-vu-iterations',
      exec: 'mavenGet',
      vus: mavenVus,
      iterations: mavenIterations,
      maxDuration: MAX_DURATION,
    },
    npm: {
      executor: 'per-vu-iterations',
      exec: 'npmGet',
      vus: npmVus,
      iterations: npmIterations,
      maxDuration: MAX_DURATION,
    },
  },
  thresholds: {
    checks: ['rate==1'],
    'http_req_failed{operation:maven}': ['rate==0'],
    'http_req_failed{operation:npm_packument}': ['rate==0'],
    'http_req_failed{operation:npm_tarball}': ['rate==0'],
    'iterations{scenario:maven}': [`count==${expectedMavenIterations}`],
    'iterations{scenario:npm}': [`count==${expectedNpmIterations}`],
  },
  summaryTrendStats: ['p(50)', 'p(95)', 'p(99)'],
};

function responseIsValid(response, statusCheck, digestCheck, expectedSha256) {
  const validations = {
    [statusCheck]: (candidate) => candidate.status === 200,
  };
  if (expectedSha256 !== null) {
    validations[digestCheck] = (candidate) =>
      candidate.body !== null && crypto.sha256(candidate.body, 'hex') === expectedSha256;
  }
  return check(response, validations);
}

function resolveTarballUrl(value) {
  if (typeof value !== 'string' || value.trim() === '') {
    fail('npm setup: requested version has no tarball URL');
  }

  const tarball = value.trim();
  let resolved;
  if (/^https?:\/\//i.test(tarball)) {
    resolved = tarball;
  } else if (tarball.startsWith('//')) {
    fail('npm setup: protocol-relative tarball URLs are not allowed');
  } else if (tarball.startsWith('/')) {
    resolved = `${baseOrigin}${tarball}`;
  } else {
    resolved = `${baseUrl}/${tarball.replace(/^\.\//, '')}`;
  }

  if (originOf(resolved, 'npm tarball URL') !== baseOrigin) {
    fail('npm setup: tarball URL points outside BASE_URL origin');
  }
  return resolved;
}

export function setup() {
  const response = http.get(npmPackumentUrl, {
    redirects: 0,
    responseType: 'text',
    tags: { operation: 'npm_packument' },
  });
  if (
    !responseIsValid(
      response,
      'npm setup packument status is 200',
      'npm setup packument SHA256 matches',
      expectedNpmPackumentSha256,
    )
  ) {
    fail('npm setup: packument validation failed');
  }

  let packument;
  try {
    packument = response.json();
  } catch (_error) {
    fail('npm setup: packument is not valid JSON');
  }

  const version = packument && packument.versions && packument.versions[npmVersion];
  if (!version || !version.dist) {
    fail('npm setup: requested version is absent from the packument');
  }

  return { npmTarballUrl: resolveTarballUrl(version.dist.tarball) };
}

export function mavenGet() {
  const response = http.get(mavenUrl, {
    redirects: 0,
    responseType: 'binary',
    tags: { operation: 'maven' },
  });
  mavenDuration.add(response.timings.duration);
  responseIsValid(
    response,
    'Maven artifact status is 200',
    'Maven artifact SHA256 matches',
    expectedMavenSha256,
  );
}

export function npmGet(data) {
  const packumentResponse = http.get(npmPackumentUrl, {
    redirects: 0,
    responseType: 'text',
    tags: { operation: 'npm_packument' },
  });
  npmPackumentDuration.add(packumentResponse.timings.duration);
  responseIsValid(
    packumentResponse,
    'npm packument status is 200',
    'npm packument SHA256 matches',
    expectedNpmPackumentSha256,
  );

  const tarballResponse = http.get(data.npmTarballUrl, {
    redirects: 0,
    responseType: 'binary',
    tags: { operation: 'npm_tarball' },
  });
  npmTarballDuration.add(tarballResponse.timings.duration);
  responseIsValid(
    tarballResponse,
    'npm tarball status is 200',
    'npm tarball SHA256 matches',
    expectedNpmTarballSha256,
  );
}
