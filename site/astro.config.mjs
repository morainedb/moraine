// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightThemeNova from 'starlight-theme-nova';

export default defineConfig({
	site: 'https://morainedb.com',
	// The architecture page moved from the guide into the design section.
	redirects: { '/guide/architecture/': '/rfcs/architecture/' },
	integrations: [
		starlight({
			plugins: [starlightThemeNova()],
			customCss: ['./src/styles/custom.css'],
			components: { Header: './src/components/Header.astro' },
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
			],
		}),
	],
});
