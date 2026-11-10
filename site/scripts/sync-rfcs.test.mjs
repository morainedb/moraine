import assert from 'node:assert/strict';
import { test } from 'node:test';
import { isPublishedRfc, isRfcFile, transformReadme, transformRfc, transformRoadmap } from './sync-rfcs.mjs';

const source = [
  '# RFC 0009: Equality and range indexes',
  '',
  '## Summary',
  '',
  'Verbs live on the [public API](0002-public-api.md) surface, retried',
  'under [the commit protocol](0003-commit-protocol.md#retry). Encoding is in',
  '[the storage format](0001-storage-format.md). Start from the',
  '[template](0000-template.md) or the [core crate](../../crates/moraine).',
  'See [SlateDB](https://slatedb.io) and [below](#summary).',
].join('\n');

const result = transformRfc('0009-equality-indexes.md', source);

test('frontmatter carries the topic as title and label, with numeric order', () => {
  assert.match(result, /^---\ntitle: "Equality and range indexes"\n/);
  assert.match(result, /sidebar: \{ label: "Equality and range indexes", order: 9 \}\n---\n/);
});

test('the H1 is stripped from the body', () => {
  assert.ok(!result.includes('# RFC 0009:'));
  assert.ok(result.startsWith('---\n'));
  assert.ok(result.includes('\n## Summary\n'));
});

test('sibling RFC links become site links, anchors preserved', () => {
  assert.ok(result.includes('](/rfcs/0002-public-api/)'));
  assert.ok(result.includes('](/rfcs/0003-commit-protocol/#retry)'));
  assert.ok(result.includes('](/rfcs/0001-storage-format/)'));
});

test('excluded and out-of-dir targets fall back to GitHub URLs', () => {
  assert.ok(
    result.includes(
      '](https://github.com/morainedb/moraine/blob/main/docs/rfcs/0000-template.md)',
    ),
  );
  assert.ok(
    result.includes('](https://github.com/morainedb/moraine/blob/main/crates/moraine)'),
  );
});

test('external links and pure anchors are untouched', () => {
  assert.ok(result.includes('](https://slatedb.io)'));
  assert.ok(result.includes('](#summary)'));
});

test('a source with no H1 is an error', () => {
  assert.throws(() => transformRfc('0099-broken.md', 'no title here'), /no H1/);
});

test('only numbered RFC files are synced', () => {
  assert.ok(isRfcFile('0011-commit-log-seam.md'));
  assert.ok(!isRfcFile('tasks.md'));
  assert.ok(!isRfcFile('README.md'));
  assert.ok(!isRfcFile('0000-template.md'));
});

const readme = [
  '# RFCs',
  '',
  'Design records for moraine. Each one states the current design.',
  '',
  '## Process',
  '',
  '1. Copy `0000-template.md` to the next free number.',
  '',
  '## Index',
  '',
  'Start with the storage format.',
  '',
  '### Core',
  '',
  '- [Storage format](0001-storage-format.md) — key layout',
  '',
  '### Internal',
  '',
  '- [Commit-log seam](0011-commit-log-seam.md) — the boundary an external',
  '  commit log plugs into; not published on the site',
  '',
].join('\n');

test('the README becomes the section overview: intro and grouped index, no process', () => {
  const index = transformReadme(readme);
  assert.match(index, /^---\ntitle: "Design"\nsidebar: \{ label: "Overview", order: 0 \}\n---\n/);
  assert.ok(index.includes('Design records for moraine.'));
  assert.ok(index.includes('\nStart with the storage format.\n'));
  assert.ok(index.includes('\n## Core\n'));
  assert.ok(index.includes('](/rfcs/0001-storage-format/)'));
  assert.ok(!index.includes('Process'));
  assert.ok(!index.includes('0000-template'));
  assert.ok(!index.includes('# RFCs'));
  assert.ok(!index.includes('## Index'));
});

test('an RFC withheld from the site is neither synced nor listed, nor its emptied group', () => {
  assert.ok(!isPublishedRfc('0011-commit-log-seam.md'));
  assert.ok(isPublishedRfc('0010-maintenance.md'));

  const index = transformReadme(readme);
  assert.ok(!index.includes('0011'));
  assert.ok(!index.includes('commit log plugs into'));
  assert.ok(!index.includes('## Internal'));
});

test('passages marked site:omit are dropped, inline and whole-line', () => {
  const source = [
    '# RFC 0003: Commit protocol',
    '',
    'The single-writer path is complete.',
    '',
    '<!-- site:omit -->',
    'A store may also keep a commit log.',
    '<!-- /site:omit -->',
    '',
    '| Case | Result |',
    '|---|---|',
    '| Format in range | open |',
    '| <!-- site:omit -->Commit-log format | refuse  <!-- /site:omit -->|',
    '',
    'Other fields tune flushing, caches<!-- site:omit -->, and the',
    'seam<!-- /site:omit -->.',
  ].join('\n');
  const page = transformRfc('0003-commit-protocol.md', source);
  assert.ok(!page.includes('commit log'));
  assert.ok(!page.includes('Commit-log'));
  assert.ok(!page.includes('site:omit'));
  assert.ok(page.includes('The single-writer path is complete.\n\n| Case | Result |'));
  assert.ok(page.endsWith('| Format in range | open |\n\nOther fields tune flushing, caches.'));
});

test('a closing site:omit marker may carry the text the site shows instead', () => {
  const page = transformRfc(
    '0006-extension-surface.md',
    '# RFC 0006: Extension surface\n\nForwarded through <!-- site:omit -->`moraine_attach_with_commit_log`<!-- /site:omit the attach call -->.\n',
  );
  assert.ok(page.endsWith('Forwarded through the attach call.\n'));
  assert.ok(!page.includes('commit_log'));
});

test('the roadmap becomes a page with RFC links resolved from the repository root', () => {
  const page = transformRoadmap(
    [
      '# Roadmap',
      '',
      'Done ([storage format](docs/rfcs/0001-storage-format.md)); see',
      '[measurement](docs/benchmarks.md#where-the-time-goes).',
      '',
      '<!-- site:omit -->',
      '### Multi-process writers',
      '<!-- /site:omit -->',
    ].join('\n'),
  );
  assert.match(page, /^---\ntitle: "Roadmap"\n---\n/);
  assert.ok(page.includes('](/rfcs/0001-storage-format/)'));
  assert.ok(
    page.includes('](https://github.com/morainedb/moraine/blob/main/docs/benchmarks.md#where-the-time-goes)'),
  );
  assert.ok(!page.includes('Multi-process'));
  assert.ok(!page.includes('# Roadmap'));
});
