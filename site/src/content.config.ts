import { defineCollection } from "astro:content";
import { docsLoader } from "@astrojs/starlight/loaders";
import { docsSchema } from "@astrojs/starlight/schema";

// Without this, Astro auto-generates a bare `docs` collection whose entry ids
// keep their file extension, and every `slug:` in the Starlight sidebar fails
// to resolve.
export const collections = {
  docs: defineCollection({ loader: docsLoader(), schema: docsSchema() }),
};
