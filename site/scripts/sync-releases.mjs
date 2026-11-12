// Builds the Releases page: each version's CHANGELOG.md entry followed by
// the extension binaries its ext-v<version> GitHub release carries, by
// DuckDB version and platform. Runs at build time; the output is gitignored.
import { execFileSync } from 'node:child_process';
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

const releasedTag = /^ext-v(\d+\.\d+\.\d+)$/;
const platformNames = 'linux_amd64|linux_arm64|osx_amd64|osx_arm64';
const extensionAsset = new RegExp(
  `^moraine\\.(v[^.]+(?:\\.[^.]+)*)\\.(${platformNames})\\.duckdb_extension$`,
);

// Releases before the per-DuckDB-version naming carry one build per
// platform and state the DuckDB version in the notes.
const legacyAsset = new RegExp(`^moraine\\.(${platformNames})\\.duckdb_extension$`);
const notedDuckdbVersion = /DuckDB (v\d+\.\d+\.\d+)/;
const unstatedDuckdbVersion = 'unstated';

function assetTarget(name, body) {
  const versioned = name.match(extensionAsset);
  if (versioned) return { duckdb: versioned[1], platform: versioned[2] };

  const legacy = name.match(legacyAsset);
  if (!legacy) return null;
  const noted = body?.match(notedDuckdbVersion);
  return { duckdb: noted ? noted[1] : unstatedDuckdbVersion, platform: legacy[1] };
}

const platforms = [
  ['linux_amd64', 'Linux x86-64'],
  ['linux_arm64', 'Linux arm64'],
  ['osx_amd64', 'macOS x86-64'],
  ['osx_arm64', 'macOS arm64'],
];

// moraine version -> DuckDB version -> platform -> download URL, from the
// published ext-v<version> releases only: drafts, prereleases, dev builds
// (any tag suffix) and assets that are not extension binaries are skipped.
export function downloadsByVersion(releases) {
  const byVersion = new Map();

  for (const release of releases) {
    if (release.draft || release.prerelease) continue;
    const tag = release.tag_name.match(releasedTag);
    if (!tag) continue;

    const byDuckdb = byVersion.get(tag[1]) ?? new Map();
    for (const { name, browser_download_url } of release.assets) {
      const target = assetTarget(name, release.body);
      if (!target) continue;
      const byPlatform = byDuckdb.get(target.duckdb) ?? new Map();
      byPlatform.set(target.platform, browser_download_url);
      byDuckdb.set(target.duckdb, byPlatform);
    }
    if (byDuckdb.size > 0) byVersion.set(tag[1], byDuckdb);
  }

  return byVersion;
}

function compareVersionsDescending(a, b) {
  const parts = (v) => v.replace(/^v/, '').split('.').map(Number);
  const [pa, pb] = [parts(a), parts(b)];
  for (let i = 0; i < Math.max(pa.length, pb.length); i += 1) {
    const diff = (pb[i] ?? 0) - (pa[i] ?? 0);
    if (diff !== 0) return diff;
  }
  return 0;
}

function downloadTable(byDuckdb) {
  const header = `| DuckDB | ${platforms.map(([, label]) => label).join(' | ')} |`;
  const rule = `|---|${platforms.map(() => '---').join('|')}|`;
  const rows = [...byDuckdb.keys()].sort(compareVersionsDescending).map((duckdb) => {
    const cells = platforms.map(([key]) => {
      const url = byDuckdb.get(duckdb).get(key);
      return url ? `[download](${url})` : '';
    });
    return `| ${duckdb} | ${cells.join(' | ')} |`;
  });
  return ['### Download', '', header, rule, ...rows].join('\n');
}

// The store-format and DuckLake-catalog-version constants as a release's
// source declares them: `const NAME: u64 = <int or NAME>;` and
// `const (MAX_)?CATALOG_VERSION: &str = "x.y";`. Releases before the floor
// was introduced read from format 1; before the catalog version was
// recorded they served 1.0.
export function parseVersionConstants(text) {
  const numbers = new Map();
  for (const [, name, value] of text.matchAll(/const ([A-Z_]+): u64 = ([^;]+);/g)) {
    const resolved = /^\d+$/.test(value.trim()) ? Number(value) : numbers.get(value.trim());
    if (resolved !== undefined) numbers.set(name, resolved);
  }
  if (!numbers.has('MAX_FORMAT_VERSION')) return null;

  const strings = new Map();
  for (const [, name, value] of text.matchAll(/const ((?:MAX_)?CATALOG_VERSION): &str = "([^"]+)";/g)) {
    strings.set(name, value);
  }
  const catalogMin = strings.get('CATALOG_VERSION') ?? '1.0';
  return {
    formatMin: numbers.get('MIN_FORMAT_VERSION') ?? 1,
    formatMax: numbers.get('MAX_FORMAT_VERSION'),
    catalogMin,
    catalogMax: strings.get('MAX_CATALOG_VERSION') ?? catalogMin,
  };
}

const unknown = '—';

// The oldest release the page lists; earlier ones stay in the changelog.
const oldestListed = '0.6.0';

export function listedReleases(entries) {
  return entries.filter(({ version }) => compareVersionsDescending(version, oldestListed) <= 0);
}

function range(low, high) {
  return low === high ? `${low}` : `${low} – ${high}`;
}

// The DuckDB versions a tag's `.github/duckdb-versions` manifest lists.
function manifestVersions(text) {
  return text
    .split('\n')
    .filter((line) => line.trim() && !line.startsWith('#'))
    .map((line) => line.trim().split(/\s+/)[0]);
}

// One row per run of consecutive releases with the same DuckDB builds,
// store-format range, and catalog versions. `entries` are newest first.
// DuckDB versions come from the release assets, or from the tag's manifest
// when no assets were fetched.
export function compatibilityRows(entries, downloads, constantsFor, manifestFor = () => '') {
  const cells = entries.map(({ version }) => {
    const fromAssets = [...(downloads.get(version)?.keys() ?? [])];
    const duckdb = (fromAssets.length ? fromAssets : manifestVersions(manifestFor(`v${version}`) ?? ''))
      .sort(compareVersionsDescending)
      .reverse();
    const constants = parseVersionConstants(constantsFor(`v${version}`) ?? '');
    return {
      version,
      duckdb: duckdb.length ? duckdb.join(', ') : unknown,
      format: constants ? range(constants.formatMin, constants.formatMax) : unknown,
      catalog: constants ? range(constants.catalogMin, constants.catalogMax) : unknown,
    };
  });

  const rows = [];
  for (const cell of cells) {
    const last = rows.at(-1);
    const same =
      last && last.duckdb === cell.duckdb && last.format === cell.format && last.catalog === cell.catalog;
    if (same) last.oldest = cell.version;
    else rows.push({ newest: cell.version, oldest: cell.version, ...cell });
  }
  return rows.map(({ newest, oldest, duckdb, format, catalog }) => ({
    versions: range(oldest, newest),
    duckdb,
    format,
    catalog,
  }));
}

function compatibilityTable(rows) {
  return [
    '## Compatibility',
    '',
    'Choose a build matching your DuckDB version and platform. The store-format',
    'column lists the formats each build can open. Older stores need',
    '`moraine_migrate`; newer stores need a newer Moraine build. The last column',
    'lists supported DuckLake catalog versions. See [Upgrading](/guide/operating/#upgrading).',
    '',
    '| Moraine | DuckDB | Catalog store format | DuckLake catalog version |',
    '|---|---|---|---|',
    ...rows.map((row) => `| ${row.versions} | ${row.duckdb} | ${row.format} | ${row.catalog} |`),
  ].join('\n');
}

const preamble = `Download the extension for your DuckDB version and platform below.
Start DuckDB with \`-unsigned\` to load it; see
[Getting started](/guide/getting-started/) for instructions. Release notes
come from the [changelog](https://github.com/morainedb/moraine/blob/main/CHANGELOG.md).`;

export function renderReleases(entries, downloads, compatibility = []) {
  const sections = entries.map(({ version, date, compareUrl, body }) => {
    const name = compareUrl ? `[${version}](${compareUrl})` : version;
    const parts = [`## ${name} — ${date}`];
    if (body) parts.push(body);
    const byDuckdb = downloads.get(version);
    if (byDuckdb) parts.push(downloadTable(byDuckdb));
    return parts.join('\n\n');
  });

  return [
    '---',
    'title: "Releases"',
    'description: "Changelog and extension downloads for every moraine release."',
    'tableOfContents: { maxHeadingLevel: 2 }',
    '---',
    '',
    preamble,
    '',
    ...(compatibility.length ? [compatibilityTable(compatibility), ''] : []),
    ...sections.flatMap((section) => [section, '']),
  ].join('\n');
}

const repositoryRoot = path.resolve(scriptDir, '../..');

// Git output at a release tag, or an empty string when the tag is
// unavailable (a shallow clone, or a version never tagged).
function git(args) {
  try {
    return execFileSync('git', args, {
      cwd: repositoryRoot,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    });
  } catch {
    return '';
  }
}

const constantsAtTag = (tag) =>
  git(['grep', '-h', '-E', 'const [A-Z_]+: (u64|&str) =', tag, '--', 'crates/moraine/src']);
const manifestAtTag = (tag) => git(['show', `${tag}:.github/duckdb-versions`]);

const releasesUrl = 'https://api.github.com/repos/morainedb/moraine/releases?per_page=100';

function nextPage(linkHeader) {
  const match = linkHeader?.match(/<([^>]+)>;\s*rel="next"/);
  return match ? match[1] : null;
}

// Every release the repository has, across pages. A non-2xx response (the
// unauthenticated rate limit, most often) fails the build with the two
// ways out named.
export async function fetchReleases(fetchImpl = fetch, token = process.env.GITHUB_TOKEN) {
  const headers = {
    Accept: 'application/vnd.github+json',
    'User-Agent': 'moraine-site-build',
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  };

  const releases = [];
  let url = releasesUrl;
  const hint = 'set GITHUB_TOKEN or MORAINE_SITE_OFFLINE=1 to skip the download tables';
  while (url) {
    let response;
    try {
      response = await fetchImpl(url, { headers });
    } catch (error) {
      throw new Error(`GitHub releases request failed (${error.message})\n${hint}`, {
        cause: error,
      });
    }
    if (!response.ok) {
      throw new Error(
        `GitHub releases request failed (HTTP ${response.status}): ${await response.text()}\n${hint}`,
      );
    }
    releases.push(...(await response.json()));
    url = nextPage(response.headers.get('link'));
  }
  return releases;
}

export async function syncReleases({ fetchImpl = fetch, offline = false } = {}) {
  const entries = listedReleases(parseChangelog(await readFile(changelogPath, 'utf8')));
  const downloads = offline ? new Map() : downloadsByVersion(await fetchReleases(fetchImpl));

  const compatibility = compatibilityRows(entries, downloads, constantsAtTag, manifestAtTag);
  await writeFile(outputPath, renderReleases(entries, downloads, compatibility));
  return {
    versions: entries.length,
    withDownloads: entries.filter(({ version }) => downloads.has(version)).length,
  };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const offline = process.env.MORAINE_SITE_OFFLINE === '1';
  const { versions, withDownloads } = await syncReleases({ offline });
  console.log(
    `synced ${versions} releases (${withDownloads} with downloads) to src/content/docs/releases.md`,
  );
}
