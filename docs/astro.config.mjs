// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

const channel = process.env.DOCS_CHANNEL || 'stable';
const base = `/${channel}`;

// https://astro.build/config
export default defineConfig({
	site: 'https://www.getnauka.com',
	base,
	integrations: [
		starlight({
			customCss: ['./src/styles/custom.css'],
			title: `Nauka${channel !== 'stable' ? ` (${channel})` : ''}`,
			favicon: '/favicon.svg',
			head: [
				{ tag: 'link', attrs: { rel: 'icon', type: 'image/png', href: '/favicon.png' } },
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
			],
		}),
	],
});
