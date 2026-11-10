// Copies the design documents from docs/rfcs into the Starlight content
// tree: the numbered RFCs become pages, the directory README becomes the
// section's index page, and the repository ROADMAP becomes a top-level
// page. Frontmatter is derived from each file's H1, and links are
// rewritten: RFCs become site links, anything else relative points at the
// file on GitHub.
import { mkdir, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const sourceDir = path.resolve(scriptDir, '../../docs/rfcs');
const outputDir = path.resolve(scriptDir, '../src/content/docs/rfcs');
const roadmapSource = path.resolve(scriptDir, '../../ROADMAP.md');
const roadmapOutput = path.resolve(scriptDir, '../src/content/docs/roadmap.md');
const benchmarksSource = path.resolve(scriptDir, '../../docs/benchmarks.md');
const benchmarksOutput = path.resolve(scriptDir, '../src/content/docs/benchmarks.md');
const githubBlobBase = 'https://github.com/morainedb/moraine/blob/main';

// An RFC is a numbered kebab-title file; the template stays out.
export function isRfcFile(name) {
  return /^\d{4}-[a-z0-9-]+\.md$/.test(name) && name !== '0000-template.md';
}

// RFCs kept in the repository but off the site: the design is binding, the
// feature it serves is not announced.
const withheld = new Set(['0011-commit-log-seam.md']);

export function isPublishedRfc(name) {
  return isRfcFile(name) && !withheld.has(name);
}

function splitTitle(filename, source) {
  const lines = source.split('\n');
  const h1Index = lines.findIndex((line) => line.startsWith('# '));
  if (h1Index === -1) throw new Error(`${filename}: no H1 title`);

  const title = lines[h1Index].slice(2).trim();
  const body = lines
    .slice(h1Index + 1)
    .join('\n')
    .replace(/^\n+/, '');
  return { title, body };
}

// `baseDir` is the source file's directory within the repository.
function rewriteLinks(body, baseDir = 'docs/rfcs') {
  return body.replace(/\]\(([^)\s]+)\)/g, (match, target) => {
    if (/^(https?:|mailto:|#)/.test(target)) return match;

    const [file, fragment] = target.split('#');
    const anchor = fragment === undefined ? '' : `#${fragment}`;
    const resolved = path.posix.normalize(path.posix.join(baseDir, file));
    const name = path.posix.basename(resolved);
    if (path.posix.dirname(resolved) === 'docs/rfcs' && isPublishedRfc(name)) {
      return `](/rfcs/${name.slice(0, -3)}/${anchor})`;
    }

    return `](${githubBlobBase}/${resolved}${anchor})`;
  });
}

function frontmatter(title, label, order) {
  return [
    '---',
    `title: ${JSON.stringify(title)}`,
    `sidebar: { label: ${JSON.stringify(label)}, order: ${order} }`,
    '---',
    '',
  ].join('\n');
}

// Passages between `<!-- site:omit -->` and `<!-- /site:omit -->` stay in
// the repository and leave the site; `<!-- /site:omit some text -->` puts
// that text in their place. A line the cut empties goes with it; a table row
// is marked inside its first cell, so the bare pipe counts as empty.
const omitted = /<!-- site:omit -->[\s\S]*?<!-- \/site:omit(?: ([^>]*?))? -->/g;
const cut = '\u0000';

function stripOmitted(body) {
  return body
    .replace(omitted, (match, instead) => instead ?? cut)
    .split('\n')
    .filter((line) => !(line.includes(cut) && line.replaceAll(cut, '').replace(/[|\s]/g, '') === ''))
    .map((line) => line.replaceAll(cut, '').trimEnd())
    .join('\n')
    .replace(/\n{3,}/g, '\n\n');
}

export function transformRfc(filename, source) {
  const { title, body } = splitTitle(filename, source);
  const order = Number(filename.slice(0, 4));
  const topic = title.replace(/^RFC \d{4}: /, '');
  return frontmatter(topic, topic, order) + rewriteLinks(stripOmitted(body));
}

function sections(body) {
  const result = new Map();
  let heading = '';
  for (const line of body.split('\n')) {
    const match = line.match(/^## (.+)$/);
    if (match) {
      heading = match[1];
      result.set(heading, []);
      continue;
    }
    if (!result.has(heading)) result.set(heading, []);
    result.get(heading).push(line);
  }
  return result;
}

// Drops list items that link a withheld RFC, including their continuation
// lines, then any group heading left with no items under it.
function publishedIndex(lines) {
  const kept = [];
  let skipping = false;
  for (const line of lines) {
    if (/^- /.test(line)) skipping = [...withheld].some((name) => line.includes(name));
    else if (!/^\s+\S/.test(line)) skipping = false;
    if (!skipping) kept.push(line);
  }

  const result = [];
  for (let i = 0; i < kept.length; i += 1) {
    if (/^### /.test(kept[i])) {
      const next = kept.findIndex((line, j) => j > i && /^### /.test(line));
      const group = kept.slice(i + 1, next === -1 ? kept.length : next);
      if (!group.some((line) => /^- /.test(line))) {
        i = (next === -1 ? kept.length : next) - 1;
        continue;
      }
      result.push(kept[i].replace(/^### /, '## '));
      continue;
    }
    result.push(kept[i]);
  }
  return result;
}

// The section overview: the README's opening paragraph and its Index, with
// group headings promoted one level. The contributor process stays behind.
export function transformReadme(source) {
  const { body } = splitTitle('README.md', source);
  const parts = sections(body);
  const intro = (parts.get('') ?? []).join('\n').trim();
  const index = publishedIndex(parts.get('Index') ?? []).join('\n').trim();
  return (
    frontmatter('Design', 'Overview', 0) + rewriteLinks([intro, index].join('\n\n')) + '\n'
  );
}

export function transformRoadmap(source) {
  const { body } = splitTitle('ROADMAP.md', source);
  return (
    ['---', 'title: "Roadmap"', '---', ''].join('\n') + rewriteLinks(stripOmitted(body), '.') + '\n'
  );
}

// The benchmark page lives in docs/, so its relative links resolve from there.
export function transformBenchmarks(source) {
  const { body } = splitTitle('benchmarks.md', source);
  return (
    ['---', 'title: "Benchmarks"', '---', ''].join('\n') +
    rewriteLinks(stripOmitted(body), 'docs') +
    '\n'
  );
}

export async function syncRfcs() {
  const entries = await readdir(sourceDir);
  const rfcs = entries.filter(isPublishedRfc).sort();

  await rm(outputDir, { recursive: true, force: true });
  await mkdir(outputDir, { recursive: true });

  const readme = await readFile(path.join(sourceDir, 'README.md'), 'utf8');
  await writeFile(path.join(outputDir, 'index.md'), transformReadme(readme));

  for (const name of rfcs) {
    const source = await readFile(path.join(sourceDir, name), 'utf8');
    await writeFile(path.join(outputDir, name), transformRfc(name, source));
  }

  await writeFile(roadmapOutput, transformRoadmap(await readFile(roadmapSource, 'utf8')));
  await writeFile(
    benchmarksOutput,
    transformBenchmarks(await readFile(benchmarksSource, 'utf8')),
  );
  return rfcs.length;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const count = await syncRfcs();
  console.log(`synced ${count} RFCs to src/content/docs/rfcs/, the roadmap, and the benchmarks`);
}
