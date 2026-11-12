import assert from 'node:assert/strict';
import { test } from 'node:test';

import { repositoryAssets, repositoryPath } from './sync-extension-repository.mjs';

const release = (tag, assets, extra = {}) => ({
  tag_name: tag,
  draft: false,
  prerelease: false,
  assets: assets.map((name) => ({ name, browser_download_url: `https://x/${tag}/${name}` })),
  ...extra,
});

test('a signed asset maps to its DuckDB version and platform directory', () => {
  assert.equal(
    repositoryPath('moraine.v2.0.0.osx_arm64.duckdb_extension.gz'),
    'v2.0.0/osx_arm64/moraine.duckdb_extension.gz',
  );
  assert.equal(repositoryPath('moraine.v2.0.0.osx_arm64.duckdb_extension'), null);
  assert.equal(repositoryPath('moraine.osx_arm64.duckdb_extension.gz'), null);
});

test('the newest published release with signed builds supplies the repository', () => {
  const releases = [
    release('ext-v0.9.0-build.1', ['moraine.v2.0.0.osx_arm64.duckdb_extension.gz']),
    release('ext-v0.8.0', ['moraine.v2.0.0.osx_arm64.duckdb_extension'], { draft: false }),
    release('ext-v0.7.0', [
      'moraine.v2.0.0.osx_arm64.duckdb_extension',
      'moraine.v2.0.0.osx_arm64.duckdb_extension.gz',
      'moraine.v2.0.0.linux_amd64.duckdb_extension.gz',
    ]),
    release('ext-v0.6.0', ['moraine.v2.0.0.osx_arm64.duckdb_extension.gz'], { draft: true }),
  ];

  assert.deepEqual(repositoryAssets(releases), [
    {
      path: 'v2.0.0/linux_amd64/moraine.duckdb_extension.gz',
      url: 'https://x/ext-v0.7.0/moraine.v2.0.0.linux_amd64.duckdb_extension.gz',
    },
    {
      path: 'v2.0.0/osx_arm64/moraine.duckdb_extension.gz',
      url: 'https://x/ext-v0.7.0/moraine.v2.0.0.osx_arm64.duckdb_extension.gz',
    },
  ]);
});

test('no signed release means an empty repository', () => {
  assert.deepEqual(
    repositoryAssets([release('ext-v0.8.0', ['moraine.v2.0.0.osx_arm64.duckdb_extension'])]),
    [],
  );
});
