import { spawnSync } from 'node:child_process';
import { readFileSync, rmSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptsDir = dirname(fileURLToPath(import.meta.url));
const e2eRoot = resolve(scriptsDir, '..');
const checkedIn = resolve(e2eRoot, '../../nora-registry/src/ui/static/tailwind.css');
const temporary = mkdtempSync(join(tmpdir(), 'nora-tailwind-'));
const generated = join(temporary, 'tailwind.css');
const cli = resolve(e2eRoot, 'node_modules/tailwindcss/lib/cli.js');

try {
  const build = spawnSync(
    process.execPath,
    [
      cli,
      '-c',
      resolve(e2eRoot, 'tailwind.config.cjs'),
      '-i',
      resolve(e2eRoot, 'input.css'),
      '-o',
      generated,
      '--minify',
    ],
    { cwd: e2eRoot, stdio: 'inherit' },
  );
  if (build.status !== 0) process.exit(build.status ?? 1);

  if (!readFileSync(generated).equals(readFileSync(checkedIn))) {
    console.error('Embedded Tailwind CSS is stale. Run: npm run build:css');
    process.exitCode = 1;
  }
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
