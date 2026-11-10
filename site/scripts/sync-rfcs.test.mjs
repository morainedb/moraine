import assert from 'node:assert/strict';
import { test } from 'node:test';
import { isRfcFile, transformReadme, transformRfc } from './sync-rfcs.mjs';

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

test('frontmatter carries the H1 title, a topic label, and numeric order', () => {
  assert.match(result, /^---\ntitle: "RFC 0009: Equality and range indexes"\n/);
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

test('the README becomes the section index with its links rewritten', () => {
  const index = transformReadme(
    '# RFCs\n\n| RFC | Topic |\n|---|---|\n| [0001](0001-storage-format.md) | Storage format |\n',
  );
  assert.match(index, /^---\ntitle: "Design documents"\nsidebar: \{ label: "About", order: 0 \}\n---\n/);
  assert.ok(index.includes('](/rfcs/0001-storage-format/)'));
  assert.ok(!index.includes('# RFCs'));
});
