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
            { label: "Install", slug: "install" },
            { label: "Deploy a cluster", slug: "deploy" },
          ],
        },
        {
          label: "How it works",
          items: [
            { label: "Architecture", slug: "architecture" },
            { label: "Erasure coding and storage", slug: "erasure-core" },
            { label: "Transport", slug: "transport" },
            { label: "Consensus", slug: "consensus" },
            { label: "Cluster", slug: "cluster" },
            { label: "Identity and discovery", slug: "identity-and-discovery" },
          ],
        },
        {
          label: "Using it",
          items: [
            { label: "HTTP API", slug: "api-http" },
            { label: "End-to-end encryption", slug: "encryption" },
            { label: "Operations", slug: "operations" },
          ],
        },
        {
          label: "Project",
          items: [
            { label: "Design decisions", slug: "decisions" },
            { label: "Backlog", slug: "backlog" },
          ],
        },
      ],
    }),
  ],
});
