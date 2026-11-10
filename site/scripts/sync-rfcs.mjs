// Copies the design documents from docs/rfcs into the Starlight content
// tree: the numbered RFCs become pages, the directory README becomes the
// section's index page. Frontmatter is derived from each file's H1, and
// links are rewritten: sibling RFCs become site links, anything else
// relative points at the file on GitHub.
import { mkdir, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const sourceDir = path.resolve(scriptDir, '../../docs/rfcs');
const outputDir = path.resolve(scriptDir, '../src/content/docs/rfcs');
const githubBlobBase = 'https://github.com/morainedb/moraine/blob/main';

// An RFC is a numbered kebab-title file; the template stays out.
export function isRfcFile(name) {
  return /^\d{4}-[a-z0-9-]+\.md$/.test(name) && name !== '0000-template.md';
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

function rewriteLinks(body) {
  return body.replace(/\]\(([^)\s]+)\)/g, (match, target) => {
    if (/^(https?:|mailto:|#)/.test(target)) return match;

    const [file, fragment] = target.split('#');
    const anchor = fragment === undefined ? '' : `#${fragment}`;
    if (isRfcFile(file)) return `](/rfcs/${file.slice(0, -3)}/${anchor})`;

    const resolved = path.posix.normalize(path.posix.join('docs/rfcs', file));
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

export function transformRfc(filename, source) {
  const { title, body } = splitTitle(filename, source);
  const order = Number(filename.slice(0, 4));
  const label = title.replace(/^RFC \d{4}: /, '');
  return frontmatter(title, label, order) + rewriteLinks(body);
}

export function transformReadme(source) {
  const { body } = splitTitle('README.md', source);
  return frontmatter('Design documents', 'About', 0) + rewriteLinks(body);
}

export async function syncRfcs() {
  const entries = await readdir(sourceDir);
  const rfcs = entries.filter(isRfcFile).sort();

  await rm(outputDir, { recursive: true, force: true });
  await mkdir(outputDir, { recursive: true });

  const readme = await readFile(path.join(sourceDir, 'README.md'), 'utf8');
  await writeFile(path.join(outputDir, 'index.md'), transformReadme(readme));

  for (const name of rfcs) {
    const source = await readFile(path.join(sourceDir, name), 'utf8');
    await writeFile(path.join(outputDir, name), transformRfc(name, source));
  }
  return rfcs.length;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const count = await syncRfcs();
  console.log(`synced ${count} RFCs to src/content/docs/rfcs/`);
}
