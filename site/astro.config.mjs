// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightThemeNova from 'starlight-theme-nova';

export default defineConfig({
	site: 'https://morainedb.github.io',
	integrations: [
		starlight({
			plugins: [starlightThemeNova()],
			customCss: ['./src/styles/custom.css'],
			title: 'moraine',
			description:
				'A DuckLake catalog that lives in your bucket — SlateDB-backed, serverless, nothing to operate.',
			logo: {
				light: './src/assets/moraine.svg',
				dark: './src/assets/moraine-dark.svg',
			},
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/morainedb/moraine' },
			],
			sidebar: [
				{
					label: 'Guide',
					items: [
						{ slug: 'guide/what-is-moraine' },
						{ slug: 'guide/getting-started' },
						{ slug: 'guide/architecture' },
						{ slug: 'guide/embedding' },
						{ slug: 'guide/operating' },
					],
				},
				{
					label: 'Design (RFCs)',
					collapsed: true,
					items: [{ autogenerate: { directory: 'rfcs' } }],
				},
			],
		}),
	],
});
