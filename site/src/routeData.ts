import { defineRouteMiddleware } from '@astrojs/starlight/route-data';

// Blog pages have no left sidebar: a post reads as an article and the index
// as a list. Starlight widens a page without a sidebar to splash width; the
// column is pinned back to the width the docs read at, and the rule under
// the title goes so the date and author sit with it.
export const onRequest = defineRouteMiddleware((context) => {
	const { starlightRoute } = context.locals;
	if (starlightRoute.id === 'blog' || starlightRoute.id.startsWith('blog/')) {
		starlightRoute.hasSidebar = false;
		starlightRoute.head.push({
			tag: 'style',
			attrs: {},
			content: [
				'html:root:not([data-has-sidebar]) { --sl-content-width: 50rem; }',
				'.content-panel + .content-panel { border-top: 0; padding-block-start: 0; }',
			].join('\n'),
		});
	}
});
