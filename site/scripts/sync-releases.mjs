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

const releasedTag = /^ext-v(\d+\.\d+\.\d+)$/;
const extensionAsset =
  /^moraine\.(v[^.]+(?:\.[^.]+)*)\.(linux_amd64|linux_arm64|osx_amd64|osx_arm64)\.duckdb_extension$/;

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
      const match = name.match(extensionAsset);
      if (!match) continue;
      const byPlatform = byDuckdb.get(match[1]) ?? new Map();
      byPlatform.set(match[2], browser_download_url);
      byDuckdb.set(match[1], byPlatform);
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

const preamble = `Each release ships the DuckDB extension as unsigned binaries, one per
DuckDB version and platform; a binary loads only into the DuckDB version
it names and needs a CLI started with \`-unsigned\`. See
[Getting started](/guide/getting-started/) for loading one. Changes are
listed from the repository [changelog](https://github.com/morainedb/moraine/blob/main/CHANGELOG.md).`;

export function renderReleases(entries, downloads) {
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
    '---',
    '',
    preamble,
    '',
    ...sections.flatMap((section) => [section, '']),
  ].join('\n');
}

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
  while (url) {
    const response = await fetchImpl(url, { headers });
    if (!response.ok) {
      throw new Error(
        `GitHub releases request failed (HTTP ${response.status}): ${await response.text()}\n` +
          'set GITHUB_TOKEN or MORAINE_SITE_OFFLINE=1 to skip the download tables',
      );
    }
    releases.push(...(await response.json()));
    url = nextPage(response.headers.get('link'));
  }
  return releases;
}

export async function syncReleases({ fetchImpl = fetch, offline = false } = {}) {
  const entries = parseChangelog(await readFile(changelogPath, 'utf8'));
  const downloads = offline ? new Map() : downloadsByVersion(await fetchReleases(fetchImpl));

  await writeFile(outputPath, renderReleases(entries, downloads));
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
