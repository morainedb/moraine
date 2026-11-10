import assert from 'node:assert/strict';
import { test } from 'node:test';
import { parseChangelog } from './sync-releases.mjs';

const changelog = [
  '# Changelog',
  '',
  '## [Unreleased]',
  '',
  '## [0.8.1](https://github.com/morainedb/moraine/compare/v0.8.0...v0.8.1) - 2026-10-27',
  '',
  '### Fixed',
  '',
  '- *(memory)* charge each structure ([#373](https://github.com/morainedb/moraine/pull/373))',
  '',
  '## [0.3.2](https://github.com/alexpropp/moraine/compare/v0.3.1...v0.3.2) - 2026-07-27',
  '',
  '### Other',
  '',
  '- x',
  '',
].join('\n');

test('each released version becomes an entry; Unreleased is dropped', () => {
  const entries = parseChangelog(changelog);
  assert.deepEqual(
    entries.map((e) => e.version),
    ['0.8.1', '0.3.2'],
  );
  assert.equal(entries[0].date, '2026-10-27');
  assert.equal(
    entries[0].compareUrl,
    'https://github.com/morainedb/moraine/compare/v0.8.0...v0.8.1',
  );
  assert.equal(
    entries[1].compareUrl,
    'https://github.com/alexpropp/moraine/compare/v0.3.1...v0.3.2',
  );
  assert.ok(entries[0].body.startsWith('### Fixed'));
  assert.ok(entries[0].body.endsWith('/pull/373))'));
});
