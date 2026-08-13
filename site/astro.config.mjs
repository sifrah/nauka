// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

export default defineConfig({
  site: "https://getnauka.com",
  integrations: [
    starlight({
      title: "Nauka",
      titleDelimiter: "·",
      description:
        "A distributed storage engine that heals itself — one binary, one key, zero configuration.",
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/sifrah/nauka",
        },
      ],
      editLink: {
        baseUrl: "https://github.com/sifrah/nauka/edit/main/site/",
      },
      lastUpdated: true,
      // Search is Pagefind: built at compile time, served from the site
      // itself. No Algolia, no third-party request, nothing to key.
      customCss: ["./src/styles/custom.css"],
      sidebar: [
        {
          label: "Start here",
          items: [
            { label: "Quickstart", slug: "quickstart" },
            { label: "Install", slug: "install" },
            { label: "Durability & consistency", slug: "durability" },
          ],
        },
        {
          label: "Operate",
          items: [
            { label: "Deploy a cluster", slug: "deploy" },
            { label: "Growing and shrinking", slug: "growing" },
            { label: "Organisations & spaces", slug: "multi-tenant" },
            { label: "Monitoring & metrics", slug: "monitoring" },
            { label: "Egress budgets & cache", slug: "egress-and-cache" },
            { label: "Operations", slug: "operations" },
          ],
        },
        {
          label: "How it works",
          items: [
            { label: "Architecture", slug: "architecture" },
            { label: "Erasure coding and storage", slug: "erasure-core" },
            { label: "Placement and healing", slug: "cluster" },
            { label: "Transport", slug: "transport" },
            { label: "Consensus", slug: "consensus" },
            { label: "Identity and membership", slug: "identity" },
          ],
        },
        {
          label: "Reference",
          items: [
            { label: "CLI reference", slug: "cli" },
            { label: "HTTP API", slug: "api-http" },
            { label: "End-to-end encryption", slug: "encryption" },
            { label: "Design decisions", slug: "decisions" },
          ],
        },
      ],
    }),
  ],
});
