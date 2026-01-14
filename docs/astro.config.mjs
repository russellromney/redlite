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
			editLink: {
				baseUrl: 'https://github.com/russellromney/redlite/edit/main/docs/',
			},
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
					label: 'SDKs & Languages',
					items: [
						{ label: 'Overview', slug: 'sdks/overview' },
						{ label: 'Python', slug: 'sdks/python' },
						{ label: 'TypeScript/Node', slug: 'sdks/typescript' },
						{ label: 'Rust', slug: 'sdks/rust' },
						{ label: 'WASM/Edge', slug: 'sdks/wasm', badge: { text: 'Soon', variant: 'tip' } },
						{ label: 'Go', slug: 'sdks/go', badge: { text: 'Planned', variant: 'caution' } },
						{ label: 'Kotlin', slug: 'sdks/kotlin', badge: { text: 'Planned', variant: 'caution' } },
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
					items: [
						{ label: 'Overview', slug: 'commands/overview' },
						{ label: 'Strings', slug: 'commands/strings' },
						{ label: 'Keys', slug: 'commands/keys' },
						{ label: 'Hashes', slug: 'commands/hashes' },
						{ label: 'Lists', slug: 'commands/lists' },
						{ label: 'Sets', slug: 'commands/sets' },
						{ label: 'Sorted Sets', slug: 'commands/sorted-sets' },
						{ label: 'Streams', slug: 'commands/streams' },
						{ label: 'Custom', slug: 'commands/custom' },
					],
				},
				{
					label: 'Reference',
					items: [
						{ label: 'Configuration', slug: 'reference/configuration' },
						{ label: 'History Tracking', slug: 'reference/history' },
					],
				},
				{
					label: 'Internals',
					collapsed: true,
					items: [
						{ label: 'Schema', slug: 'internals/schema' },
					],
				},
			],
		}),
	],
});
