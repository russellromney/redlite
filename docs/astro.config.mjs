// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	site: 'https://redlite.dev',
	integrations: [
		starlight({
			title: '🔴 Redlite',
			description: 'SQLite-backed Redis-compatible embedded key-value store',
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/russellromney/redlite' },
			],
			head: [
				{
					tag: 'meta',
					attrs: { property: 'og:image', content: 'https://redlite.dev/og.png' },
				},
			],
			customCss: ['./src/styles/custom.css'],
			sidebar: [
				{
					label: 'Getting Started',
					items: [
						{ label: 'Introduction', slug: 'getting-started/introduction' },
						{ label: 'Installation', slug: 'getting-started/installation' },
						{ label: 'Quick Start', slug: 'getting-started/quickstart' },
					],
				},
				{
					label: 'Usage',
					items: [
						{ label: 'Embedded (Library)', slug: 'usage/embedded' },
						{ label: 'Server Mode', slug: 'usage/server' },
					],
				},
				{
					label: 'Commands',
					autogenerate: { directory: 'commands' },
				},
				{
					label: 'Reference',
					items: [
						{ label: 'Configuration', slug: 'reference/configuration' },
						{ label: 'Schema', slug: 'reference/schema' },
					],
				},
			],
		}),
	],
});
