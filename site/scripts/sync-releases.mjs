// Builds the Releases page: each version's CHANGELOG.md entry followed by
// the extension binaries its ext-v<version> GitHub release carries, by
// DuckDB version and platform. Runs at build time; the output is gitignored.
import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const changelogPath = path.resolve(scriptDir, '../../CHANGELOG.md');
const outputPath = path.resolve(scriptDir, '../src/content/docs/releases.md');

const releaseHeading = /^## \[(\d+\.\d+\.\d+)\](?:\(([^)]+)\))? - (\d{4}-\d{2}-\d{2})$/;

// One entry per released version; the Unreleased section and the preamble
// above the first release are dropped.
export function parseChangelog(source) {
  const entries = [];
  let current = null;

  for (const line of source.split('\n')) {
    const match = line.match(releaseHeading);
    if (match) {
      current = { version: match[1], compareUrl: match[2] ?? null, date: match[3], lines: [] };
      entries.push(current);
      continue;
    }
    if (line.startsWith('## ')) {
      current = null;
      continue;
    }
    if (current) current.lines.push(line);
  }

  return entries.map(({ lines, ...entry }) => ({ ...entry, body: lines.join('\n').trim() }));
}

export { changelogPath, outputPath };
