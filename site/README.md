# site

The project website — [Astro](https://astro.build) +
[Starlight](https://starlight.astro.build), deployed to GitHub Pages at
<https://morainedb.com/> by `.github/workflows/site.yml`, which
pushes the built site to the `morainedb/morainedb.github.io` repository
(deploy key in the `SITE_DEPLOY_KEY` secret).

Content lives in `src/content/docs/`. The Design, Roadmap, and Benchmarks
pages are not authored here: `scripts/sync-rfcs.mjs` copies the RFCs and
their README from `../docs/rfcs/`, `../ROADMAP.md`, and
`../docs/benchmarks.md` before every `dev`/`build` (the synced copies are
gitignored). Edit them at the source.

The Blog is [starlight-blog](https://starlight-blog-docs.vercel.app/): one
Markdown file per post in `src/content/docs/blog/`, with `title`, `date`,
and `authors` in the frontmatter. A post marked `draft: true` renders in
`dev` and is left out of the build. The feed is at `/blog/rss.xml`. The
index at `/blog/` is `src/pages/blog/index.astro`, a list of posts in
place of the plugin's previews; `src/routeData.ts` drops the sidebar on
blog pages; and `src/components/PageTitle.astro` puts a link back to the
blog above a post's title.

Page views go to [Plausible](https://plausible.io/morainedb.com) via a
script tag that `astro.config.mjs` adds in production builds only; the dev
server and the preview URL are not counted. The dashboard is on Contour's
Plausible account.

```sh
npm install
npm test         # sync-script unit tests
npm run dev      # local dev server (syncs RFCs first)
npm run build    # production build to ./dist/
```
