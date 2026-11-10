# site

The project website — [Astro](https://astro.build) +
[Starlight](https://starlight.astro.build), deployed to GitHub Pages at
<https://morainedb.github.io/> by `.github/workflows/site.yml`, which
pushes the built site to the `morainedb/morainedb.github.io` repository
(deploy key in the `SITE_DEPLOY_KEY` secret).

Content lives in `src/content/docs/`. The Design section is not authored
here: `scripts/sync-rfcs.mjs` copies the RFCs and their README from
`../docs/rfcs/` before every `dev`/`build` (the synced copies are
gitignored). Edit them at the source.

```sh
npm install
npm test         # sync-script unit tests
npm run dev      # local dev server (syncs RFCs first)
npm run build    # production build to ./dist/
```
