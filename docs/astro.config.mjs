// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import fs from 'node:fs';
import path from 'node:path';

const channel = process.env.DOCS_CHANNEL || 'stable';
const isTag = channel.startsWith('v');
const base = `/${channel}`;

// Auto-discover layer docs — no manual sidebar entries needed.
// Scans docs/src/content/docs/layers/ for subdirectories (created by sync-docs.sh).
// Each layer gets a sidebar section with autogenerate.
const layersDir = path.resolve('src/content/docs/layers');
const layerNames = {
	core: 'Core',
	state: 'State',
	hypervisor: 'Hypervisor',
	org: 'Organization',
	compute: 'Compute',
	overlay: 'Overlay',
	storage: 'Storage',
};
const layerSidebar = fs.existsSync(layersDir)
	? fs.readdirSync(layersDir, { withFileTypes: true })
		.filter(d => d.isDirectory())
		.sort((a, b) => {
			// Fixed order for known layers, alphabetical for new ones
			const order = ['core', 'state', 'hypervisor', 'org', 'compute', 'overlay', 'storage'];
			const ai = order.indexOf(a.name);
			const bi = order.indexOf(b.name);
			if (ai !== -1 && bi !== -1) return ai - bi;
			if (ai !== -1) return -1;
			if (bi !== -1) return 1;
			return a.name.localeCompare(b.name);
		})
		.map(d => ({
			label: layerNames[d.name] || d.name.charAt(0).toUpperCase() + d.name.slice(1),
			autogenerate: { directory: `layers/${d.name}` },
		}))
	: [];

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
				{
					label: 'Platform',
					items: [
						{ label: 'Architecture', slug: 'architecture' },
						{ label: 'Networking', slug: 'networking' },
					],
				},
				...layerSidebar,
				{ label: 'Chaos Monkey', autogenerate: { directory: 'chaos-monkey' } },
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
