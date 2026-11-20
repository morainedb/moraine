// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightThemeNova from 'starlight-theme-nova';
import starlightBlog from 'starlight-blog';

export default defineConfig({
	site: 'https://morainedb.com',
	// The architecture page moved from the guide into the design section.
	redirects: { '/guide/architecture/': '/rfcs/architecture/' },
	integrations: [
		starlight({
			// Nova first: the blog's own header link and Markdown override would
			// otherwise replace the theme's.
			plugins: [
				starlightThemeNova(),
				starlightBlog({
					navigation: 'none',
					// The index is src/pages/blog/index.astro, a list of every post.
					postCount: Infinity,
					metrics: { readingTime: true },
					authors: {
						alex: { name: 'Alex Propp', url: 'https://contour.app' },
					},
				}),
			],
			customCss: ['./src/styles/custom.css'],
			routeMiddleware: './src/routeData.ts',
			// Plausible, in production builds only, so neither the dev server nor
			// a preview counts. Cookieless, so no consent banner.
			head:
				process.env.NODE_ENV === 'production'
					? [
							{
								tag: 'script',
								attrs: { async: true, src: 'https://plausible.io/js/pa-2zOZjC7EnmUKzl3j4_1Xu.js' },
							},
							{
								tag: 'script',
								content:
									'window.plausible=window.plausible||function(){(plausible.q=plausible.q||[]).push(arguments)},plausible.init=plausible.init||function(i){plausible.o=i||{}};plausible.init()',
							},
						]
					: [],
			components: {
				Header: './src/components/Header.astro',
				MarkdownContent: './src/components/MarkdownContent.astro',
				PageTitle: './src/components/PageTitle.astro',
			},
			title: 'Moraine',
			description:
				'A DuckLake catalog backend that stores the catalog in object storage on SlateDB.',
			logo: {
				light: './src/assets/moraine.svg',
				dark: './src/assets/moraine-dark.svg',
			},
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/morainedb/moraine' },
				{ icon: 'email', label: 'Contact', href: 'mailto:info@contour.app' },
			],
			sidebar: [
				{
					label: 'Guide',
					items: [
						{ slug: 'guide/getting-started' },
						{ slug: 'guide/configuration' },
						{ slug: 'guide/operating' },
						{ slug: 'guide/comparison' },
					],
				},
				{
					label: 'Design',
					items: [{ autogenerate: { directory: 'rfcs' } }],
				},
				{ label: 'Benchmarks', slug: 'benchmarks' },
				{ label: 'Releases', slug: 'releases' },
				{ label: 'Roadmap', slug: 'roadmap' },
				{ label: 'Blog', link: '/blog/' },
			],
		}),
	],
});
