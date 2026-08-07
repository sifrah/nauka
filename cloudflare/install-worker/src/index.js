/**
 * sh.getnauka.com — serves the Nauka install script.
 *
 *   curl -sSfL https://sh.getnauka.com | sh
 *
 * The script is fetched from the repository and held in Cloudflare's edge
 * cache, so the usual case costs no round trip to GitHub and a GitHub
 * outage does not break installs. It is served as text/plain: browsers
 * show it instead of downloading it, which matters — people should be
 * able to read a script before piping it into a shell.
 */

const SOURCE =
  "https://raw.githubusercontent.com/sifrah/nauka/main/install.sh";
const CACHE_SECONDS = 300;

export default {
  async fetch(request) {
    if (request.method !== "GET" && request.method !== "HEAD") {
      return new Response("method not allowed\n", {
        status: 405,
        headers: { Allow: "GET, HEAD" },
      });
    }

    const upstream = await fetch(SOURCE, {
      cf: { cacheTtl: CACHE_SECONDS, cacheEverything: true },
    });

    if (!upstream.ok) {
      // Never pipe a half-baked body into someone's shell.
      return new Response(
        "# the install script is temporarily unavailable\n" +
          "# install from source instead: https://github.com/sifrah/nauka\n",
        { status: 502, headers: { "Content-Type": "text/plain; charset=utf-8" } },
      );
    }

    const headers = {
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": `public, max-age=${CACHE_SECONDS}`,
      "X-Content-Type-Options": "nosniff",
    };
    // A HEAD response must carry no body — returning one is an error in
    // Workers, not merely wasteful.
    if (request.method === "HEAD") {
      return new Response(null, { headers });
    }
    return new Response(upstream.body, { headers });
  },
};
