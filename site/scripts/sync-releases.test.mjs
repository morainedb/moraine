import assert from 'node:assert/strict';
import { test } from 'node:test';
import { compatibilityRows, downloadsByVersion, fetchReleases, parseChangelog, parseVersionConstants, renderReleases } from './sync-releases.mjs';

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

const asset = (name, url) => ({ name, browser_download_url: url });
const releases = [
  {
    tag_name: 'ext-v0.8.1',
    draft: false,
    prerelease: false,
    assets: [
      asset('moraine.v2.0.0.linux_amd64.duckdb_extension', 'https://x/0.8.1/la'),
      asset('moraine.v2.0.0.osx_arm64.duckdb_extension', 'https://x/0.8.1/oa'),
      asset('moraine.v2.0.0.osx_arm64.duckdb_extension.sha256', 'https://x/junk'),
    ],
  },
  {
    tag_name: 'ext-v0.7.0',
    draft: false,
    prerelease: false,
    assets: [
      asset('moraine.v1.5.4.linux_amd64.duckdb_extension', 'https://x/154'),
      asset('moraine.v1.5.5.linux_amd64.duckdb_extension', 'https://x/155'),
    ],
  },
  {
    tag_name: 'ext-v0.9.0-build.2',
    draft: false,
    prerelease: false,
    assets: [asset('moraine.v2.0.0.linux_amd64.duckdb_extension', 'https://x/dev')],
  },
  {
    tag_name: 'ext-v0.8.1',
    draft: true,
    prerelease: false,
    assets: [asset('moraine.v2.0.0.linux_arm64.duckdb_extension', 'https://x/draft')],
  },
  { tag_name: 'v0.8.1', draft: false, prerelease: false, assets: [] },
];

test('downloads are keyed by moraine version, DuckDB version, platform', () => {
  const d = downloadsByVersion(releases);
  assert.deepEqual([...d.keys()].sort(), ['0.7.0', '0.8.1']);
  assert.equal(d.get('0.8.1').get('v2.0.0').get('linux_amd64'), 'https://x/0.8.1/la');
  assert.equal(d.get('0.8.1').get('v2.0.0').has('linux_arm64'), false);
  assert.deepEqual([...d.get('0.7.0').keys()].sort(), ['v1.5.4', 'v1.5.5']);
});

test('dev builds, drafts, crate releases, and stray assets are ignored', () => {
  const d = downloadsByVersion(releases);
  assert.equal(d.has('0.9.0'), false);
  assert.equal(d.get('0.8.1').get('v2.0.0').size, 2);
});

test('the page carries each version, its notes, and a download table', () => {
  const page = renderReleases(parseChangelog(changelog), downloadsByVersion(releases));
  assert.match(page, /^---\ntitle: "Releases"\n/);
  assert.ok(
    page.includes(
      '## [0.8.1](https://github.com/morainedb/moraine/compare/v0.8.0...v0.8.1) — 2026-10-27',
    ),
  );
  assert.ok(page.includes('### Fixed'));
  assert.ok(page.includes('| DuckDB | Linux x86-64 | Linux arm64 | macOS x86-64 | macOS arm64 |'));
  assert.ok(
    page.includes('| v2.0.0 | [download](https://x/0.8.1/la) |  |  | [download](https://x/0.8.1/oa) |'),
  );
});

test('a version without an extension release has no download table', () => {
  const page = renderReleases(parseChangelog(changelog), downloadsByVersion(releases));
  const section = page.slice(page.indexOf('## [0.3.2]'));
  assert.ok(!section.includes('### Download'));
});

test('DuckDB versions are listed newest first', () => {
  const entries = [{ version: '0.7.0', date: '2026-09-21', compareUrl: null, body: '' }];
  const page = renderReleases(entries, downloadsByVersion(releases));
  assert.ok(page.indexOf('| v1.5.5 |') < page.indexOf('| v1.5.4 |'));
  assert.ok(page.includes('## 0.7.0 — 2026-09-21'));
});

function fakeFetch(pages) {
  const calls = [];
  const impl = async (url, init) => {
    calls.push({ url, init });
    const page = pages[calls.length - 1];
    return {
      ok: page.status === undefined,
      status: page.status ?? 200,
      headers: new Headers(page.next ? { link: `<${page.next}>; rel="next"` } : {}),
      json: async () => page.body,
      text: async () => JSON.stringify(page.body),
    };
  };
  return { impl, calls };
}

test('fetchReleases follows pagination and sends the token', async () => {
  const { impl, calls } = fakeFetch([
    { body: [{ tag_name: 'a' }], next: 'https://api.github.com/x?page=2' },
    { body: [{ tag_name: 'b' }] },
  ]);
  const all = await fetchReleases(impl, 'tok');
  assert.deepEqual(
    all.map((r) => r.tag_name),
    ['a', 'b'],
  );
  assert.equal(calls[1].url, 'https://api.github.com/x?page=2');
  assert.equal(calls[0].init.headers.Authorization, 'Bearer tok');
});

test('fetchReleases omits Authorization without a token', async () => {
  const { impl, calls } = fakeFetch([{ body: [] }]);
  await fetchReleases(impl, undefined);
  assert.equal('Authorization' in calls[0].init.headers, false);
});

test('a failed response names the escape hatches', async () => {
  const { impl } = fakeFetch([{ status: 403, body: { message: 'rate limited' } }]);
  await assert.rejects(
    fetchReleases(impl, undefined),
    /403.*GITHUB_TOKEN or MORAINE_SITE_OFFLINE=1/s,
  );
});

test('legacy unversioned assets take their DuckDB version from the release notes', () => {
  const d = downloadsByVersion([
    {
      tag_name: 'ext-v0.4.0',
      draft: false,
      prerelease: false,
      body: 'Unsigned moraine extension built against DuckDB v1.5.4. Signed install: ...',
      assets: [asset('moraine.osx_arm64.duckdb_extension', 'https://x/0.4.0/oa')],
    },
    {
      tag_name: 'ext-v0.2.0',
      draft: false,
      prerelease: false,
      body: 'no version here',
      assets: [asset('moraine.linux_amd64.duckdb_extension', 'https://x/0.2.0/la')],
    },
  ]);
  assert.equal(d.get('0.4.0').get('v1.5.4').get('osx_arm64'), 'https://x/0.4.0/oa');
  assert.equal(d.get('0.2.0').get('unstated').get('linux_amd64'), 'https://x/0.2.0/la');
});

test('a network failure names the escape hatches too', async () => {
  const impl = async () => {
    throw new TypeError('fetch failed');
  };
  await assert.rejects(fetchReleases(impl, undefined), /fetch failed.*GITHUB_TOKEN or MORAINE_SITE_OFFLINE=1/s);
});

test('the table of contents stops at version headings', () => {
  const page = renderReleases([], new Map());
  assert.ok(page.includes('\ntableOfContents: { maxHeadingLevel: 2 }\n'));
});

const constants = [
  'pub const FORMAT_VERSION: u64 = 1;',
  'pub const FORMAT_WITH_INLINE_TOMBSTONE_HISTORY: u64 = 9;',
  'pub(crate) const MAX_FORMAT_VERSION: u64 = FORMAT_WITH_INLINE_TOMBSTONE_HISTORY;',
  'pub(crate) const MIN_FORMAT_VERSION: u64 = FORMAT_WITH_INLINE_TOMBSTONE_HISTORY;',
  'pub const CATALOG_VERSION: &str = "1.0";',
  'pub const MAX_CATALOG_VERSION: &str = "1.1";',
].join('\n');

test('version constants resolve names to numbers, with old-tag defaults', () => {
  assert.deepEqual(parseVersionConstants(constants), {
    formatMin: 9,
    formatMax: 9,
    catalogMin: '1.0',
    catalogMax: '1.1',
  });
  assert.deepEqual(
    parseVersionConstants('const FORMAT_WITH_STAGED_INDEX: u64 = 3;\nconst MAX_FORMAT_VERSION: u64 = FORMAT_WITH_STAGED_INDEX;'),
    { formatMin: 1, formatMax: 3, catalogMin: '1.0', catalogMax: '1.0' },
  );
  assert.equal(parseVersionConstants(''), null);
});

test('compatibility rows merge consecutive releases that agree', () => {
  const entries = ['0.8.1', '0.8.0', '0.7.0', '0.6.5'].map((version) => ({ version }));
  const downloads = new Map([
    ['0.8.1', new Map([['v2.0.0', new Map()]])],
    ['0.8.0', new Map([['v2.0.0', new Map()]])],
    ['0.7.0', new Map([['v1.5.5', new Map()], ['v1.5.4', new Map()]])],
  ]);
  const byTag = {
    'v0.8.1': constants,
    'v0.8.0': constants,
    'v0.7.0': constants.replace('"1.1"', '"1.0"'),
    'v0.6.5': '',
  };
  const rows = compatibilityRows(entries, downloads, (tag) => byTag[tag]);
  assert.deepEqual(rows, [
    { versions: '0.8.0 – 0.8.1', duckdb: 'v2.0.0', format: '9', catalog: '1.0 – 1.1' },
    { versions: '0.7.0', duckdb: 'v1.5.4, v1.5.5', format: '9', catalog: '1.0' },
    { versions: '0.6.5', duckdb: '—', format: '—', catalog: '—' },
  ]);
});

test('the page opens with the compatibility table', () => {
  const page = renderReleases(
    [{ version: '0.8.1', date: '2026-10-27', compareUrl: null, body: '' }],
    new Map(),
    [{ versions: '0.8.1', duckdb: 'v2.0.0', format: '9', catalog: '1.0 – 1.1' }],
  );
  assert.ok(page.indexOf('## Compatibility') < page.indexOf('## 0.8.1'));
  assert.ok(page.includes('| Moraine | DuckDB | Catalog store format | DuckLake catalog version |'));
  assert.ok(page.includes('| 0.8.1 | v2.0.0 | 9 | 1.0 – 1.1 |'));
});

test('without release assets, DuckDB versions come from the tag manifest', () => {
  const manifest = '# comment\nv1.5.5 duckdb=abc\nv1.5.4\n';
  const rows = compatibilityRows([{ version: '0.6.4' }], new Map(), () => constants, () => manifest);
  assert.equal(rows[0].duckdb, 'v1.5.4, v1.5.5');
  const none = compatibilityRows([{ version: '0.4.0' }], new Map(), () => constants, () => '');
  assert.equal(none[0].duckdb, '—');
});
