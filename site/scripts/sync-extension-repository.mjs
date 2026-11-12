// Assembles the DuckDB extension repository the site serves under /ext:
// the signed builds of the newest published ext-v<version> release, laid
// out as <duckdb-version>/<platform>/moraine.duckdb_extension.gz. The
// public key file beside them is committed. Runs at build time; the
// downloaded files are gitignored.
import { mkdir, stat, writeFile } from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

import { fetchReleases } from './sync-releases.mjs';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const repositoryDir = path.resolve(scriptDir, '../public/ext');

const releasedTag = /^ext-v(\d+\.\d+\.\d+)$/;
const signedAsset =
  /^moraine\.(v\d+\.\d+\.\d+)\.(linux_amd64|linux_arm64|osx_amd64|osx_arm64)\.duckdb_extension\.gz$/;

// Where a signed release asset sits in the repository, or null for any
// other asset.
export function repositoryPath(assetName) {
  const match = assetName.match(signedAsset);
  return match ? `${match[1]}/${match[2]}/moraine.duckdb_extension.gz` : null;
}

function compareVersionsDescending(a, b) {
  const left = a.split('.').map(Number);
  const right = b.split('.').map(Number);
  for (let i = 0; i < 3; i += 1) {
    if (left[i] !== right[i]) return right[i] - left[i];
  }
  return 0;
}

// The signed assets of the newest published release that carries any,
// sorted by repository path. Drafts, prereleases, and dev builds are
// skipped, as the Releases page skips them.
export function repositoryAssets(releases) {
  const candidates = releases
    .filter((release) => !release.draft && !release.prerelease)
    .map((release) => ({ release, tag: release.tag_name.match(releasedTag) }))
    .filter(({ tag }) => tag)
    .sort((a, b) => compareVersionsDescending(a.tag[1], b.tag[1]));

  for (const { release } of candidates) {
    const assets = release.assets
      .map(({ name, browser_download_url }) => ({
        path: repositoryPath(name),
        url: browser_download_url,
      }))
      .filter(({ path: target }) => target)
      .sort((a, b) => a.path.localeCompare(b.path));
    if (assets.length > 0) return assets;
  }
  return [];
}

async function sizeOf(file) {
  try {
    return (await stat(file)).size;
  } catch {
    return -1;
  }
}

export async function syncExtensionRepository({ fetchImpl = fetch, offline = false } = {}) {
  if (offline) return 0;

  const assets = repositoryAssets(await fetchReleases(fetchImpl));
  for (const { path: target, url } of assets) {
    const file = path.join(repositoryDir, target);
    await mkdir(path.dirname(file), { recursive: true });

    const head = await fetchImpl(url, { method: 'HEAD', redirect: 'follow' });
    const expected = Number(head.headers.get('content-length') ?? -2);
    if (head.ok && expected === (await sizeOf(file))) continue;

    const response = await fetchImpl(url, { redirect: 'follow' });
    if (!response.ok) {
      throw new Error(`downloading ${url} failed (HTTP ${response.status})`);
    }
    await writeFile(file, Buffer.from(await response.arrayBuffer()));
  }
  return assets.length;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const offline = process.env.MORAINE_SITE_OFFLINE === '1';
  const count = await syncExtensionRepository({ offline });
  console.log(
    offline
      ? 'offline: extension repository left as is'
      : `synced ${count} signed builds into public/ext/`,
  );
}
