// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

const channel = process.env.DOCS_CHANNEL || 'stable';
const isTag = channel.startsWith('v');
const base = `/${channel}`;

// https://astro.build/config
export default defineConfig({
	site: 'https://www.getnauka.com',
	base,
	integrations: [
		starlight({
			expressiveCode: {
				themes: ['github-dark-dimmed', 'github-light'],
				styleOverrides: {
					borderRadius: '0.5rem',
					borderColor: 'var(--sl-color-gray-5)',
					frames: {
						shadowColor: 'transparent',
						frameBoxShadowCssValue: 'none',
					},
					codeFontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace',
					codeFontSize: '0.8rem',
					codeLineHeight: '1.75',
					codePaddingInline: '1.5rem',
					codePaddingBlock: '1.25rem',
				},
			},
			customCss: [
				'@fontsource-variable/inter',
				'./src/styles/custom.css',
			],
			title: `Nauka${isTag ? ` ${channel}` : channel !== 'stable' ? ` (${channel})` : ''}`,
			favicon: '/favicon.svg',
			head: [
				{ tag: 'link', attrs: { rel: 'icon', type: 'image/png', href: '/favicon.png' } },
				{ tag: 'script', attrs: { type: 'module' }, content: `
					import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
					const theme = document.documentElement.dataset.theme === 'light' ? 'default' : 'dark';
					mermaid.initialize({ startOnLoad: true, theme });
				` },
			],
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/sifrah/nauka' }],
			components: {
				SocialIcons: './src/components/SocialIcons.astro',
				ThemeSelect: './src/components/ThemeSelect.astro',
				Sidebar: './src/components/Sidebar.astro',
			},
			sidebar: [
				{
					label: 'Getting Started',
					items: [
						{ label: 'Introduction', slug: '' },
						{ label: 'Installation', slug: 'getting-started/installation' },
						{ label: 'Quick Start', slug: 'getting-started/quick-start' },
						{ label: 'Core Concepts', slug: 'getting-started/concepts' },
						{ label: 'Creating a Mesh', slug: 'getting-started/creating-a-mesh' },
						{ label: 'Adding Nodes', slug: 'getting-started/adding-nodes' },
						{ label: 'Operations', slug: 'getting-started/operations' },
						{ label: 'Troubleshooting', slug: 'getting-started/troubleshooting' },
					],
				},
				{ label: 'Core', autogenerate: { directory: 'layers/core' } },
				{ label: 'State', autogenerate: { directory: 'layers/state' } },
				{ label: 'Hypervisor', autogenerate: { directory: 'layers/hypervisor' } },
				{
					label: 'API Reference',
					items: [
						{ label: 'REST API (Scalar)', link: `${base}/rest/` },
						{ label: 'Rust API (rustdoc)', link: `${base}/api/nauka_core/` },
					],
				},
				{ label: 'Contributing', autogenerate: { directory: 'contributing' } },
			],
		}),
	],
});
