// pinkdb extension repository — Worker front door for the `pinkdb-extensions`
// R2 bucket. Format v1 (see ../FORMAT.md).
//
//   GET /            → short info page: title, format version, and links to the
//                      docs catalog UI (https://pinkdb-docs.defaultcms.workers.dev/extensions)
//                      and to /index.json. The browsable catalog UI itself
//                      lives in the docs site; it fetches /index.json
//                      cross-origin, so the catalog carries CORS *.
//   OPTIONS /index.json → CORS preflight for the catalog (204)
//   GET /index.json  → the catalog (short cache, ETag, CORS *)
//   GET /artifacts/… → artifact bytes (octet-stream, Content-Length, ETag,
//                      immutable caching — artifacts are immutable; no CORS,
//                      they are plain `<a download>` targets)
//   anything else    → 404 (no directory listings)
//
// Module syntax, no build step, no dependencies.

const FORMAT_VERSION = 1;
const INDEX_KEY = "index.json";
const INDEX_CACHE = "public, max-age=60";
const ARTIFACT_CACHE = "public, max-age=31536000, immutable";
const TEXT = "text/plain; charset=utf-8";
// Cross-origin access for the catalog: the docs Extensions page renders the
// catalog client-side from another origin (GET /index.json). Artifacts stay
// plain `<a download>` targets and keep their existing headers.
const CORS_ORIGIN = { "access-control-allow-origin": "*" };

export default {
  async fetch(request, env) {
    const { pathname } = new URL(request.url);

    // Belt-and-braces CORS preflight for the catalog. A simple cross-origin
    // GET (the case the docs page uses) does not need this, but it keeps
    // custom-header clients working. Only /index.json answers OPTIONS; every
    // other route (including /artifacts/…) keeps the 405 below.
    if (request.method === "OPTIONS" && pathname === "/index.json") {
      return new Response(null, {
        status: 204,
        headers: {
          ...CORS_ORIGIN,
          "access-control-allow-methods": "GET, HEAD, OPTIONS",
          "access-control-max-age": "86400",
          "cache-control": "no-store",
        },
      });
    }

    if (request.method !== "GET" && request.method !== "HEAD") {
      return new Response("method not allowed\n", {
        status: 405,
        headers: { allow: "GET, HEAD", "cache-control": "no-store" },
      });
    }

    if (pathname === "/") {
      return new Response(infoPage(), {
        headers: {
          "content-type": "text/html; charset=utf-8",
          "cache-control": "no-store",
        },
      });
    }

    if (pathname === "/index.json") {
      return await serveIndex(request, env);
    }

    if (pathname.startsWith("/artifacts/")) {
      return await serveArtifact(request, env, pathname);
    }

    return notFound(`no such path: ${pathname}\n`);
  },
};

function infoPage() {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>pinkdb extension repository</title>
<meta name="description" content="Machine endpoints of the pinkdb extension repository (format v1): the catalog at /index.json (CORS *), artifact bytes under /artifacts/. The browsable catalog UI lives in the pinkdb docs.">
<meta name="color-scheme" content="dark">
<style>
:root {
  --bg: #131118;
  --bg-soft: #1a1721;
  --bg-code: #0e0c13;
  --border: #2d2938;
  --border-soft: #221f2c;
  --text: #e9e6f1;
  --muted: #a7a2b6;
  --pink: #ff5f8f;
  --pink-strong: #ff83a8;
  --pink-dim: rgba(255, 95, 143, 0.13);
  --radius: 10px;
  --mono: ui-monospace, "SF Mono", SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
  --sans: system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--text);
  font-family: var(--sans);
  font-size: 16px;
  line-height: 1.65;
  -webkit-font-smoothing: antialiased;
}
::selection { background: var(--pink-dim); color: var(--text); }
:focus-visible { outline: 2px solid var(--pink-strong); outline-offset: 2px; }
.container {
  max-width: 52rem;
  margin-inline: auto;
  width: 100%;
  padding: clamp(2.5rem, 9vw, 5.5rem) clamp(1rem, 4vw, 2rem);
}
.kicker {
  text-transform: uppercase; letter-spacing: 0.14em; font-size: 0.78rem;
  font-weight: 700; color: var(--pink-strong); margin: 0 0 0.6rem;
}
h1 {
  font-size: clamp(1.9rem, 4.5vw, 2.6rem); line-height: 1.2;
  letter-spacing: -0.015em; margin: 0 0 0.75rem;
}
p { margin: 0.65rem 0; }
a { color: var(--pink-strong); text-decoration-thickness: 1px; text-underline-offset: 2px; }
a:hover { color: #ffb3ca; }
code {
  font-family: var(--mono); font-size: 0.88em; background: var(--bg-code);
  border: 1px solid var(--border-soft); border-radius: 5px;
  padding: 0.08em 0.35em; overflow-wrap: anywhere;
}
.lede { font-size: 1.06rem; color: var(--muted); }
.links { list-style: none; padding: 0; margin: 1.5rem 0; display: grid; gap: 0.6rem; }
.links li {
  border: 1px solid var(--border); border-radius: var(--radius);
  background: var(--bg-soft); padding: 0.9rem 1.1rem;
}
.links a { font-weight: 600; }
.links .note { display: block; color: var(--muted); font-size: 0.9rem; font-weight: 400; margin-top: 0.15rem; }
.meta { color: var(--muted); font-size: 0.85rem; font-family: var(--mono); overflow-wrap: anywhere; }
</style>
</head>
<body>
<main class="container">
  <p class="kicker">extension repository</p>
  <h1>pinkdb extension repository</h1>
  <p class="lede">Hosted, immutable catalog of built pinkdb extension artifacts
  (C ABI v1, format v${FORMAT_VERSION}). This Worker serves the machine
  endpoints; the browsable catalog UI lives in the docs.</p>
  <ul class="links">
    <li>
      <a href="https://pinkdb-docs.defaultcms.workers.dev/extensions">Browse the catalog in the docs</a>
      <span class="note">Search, per-extension metadata, <code>.install &lt;name&gt;</code> commands and downloads.</span>
    </li>
    <li>
      <a href="/index.json">GET /index.json</a>
      <span class="note">The raw catalog: plain JSON, ETag, <code>Access-Control-Allow-Origin: *</code>.</span>
    </li>
  </ul>
  <p class="meta">format v${FORMAT_VERSION} &middot; artifacts:
  /artifacts/&lt;name&gt;/&lt;version&gt;/&lt;platform&gt;/&lt;file&gt; &middot; immutable &middot;
  verify the SHA-256 before loading</p>
</main>
</body>
</html>
`;
}

async function serveIndex(request, env) {
  const object = await env.EXT_BUCKET.get(INDEX_KEY);
  if (object === null) {
    return new Response("index.json not found: the catalog has not been published yet\n", {
      status: 404,
      headers: { "content-type": TEXT, "cache-control": "no-store", ...CORS_ORIGIN },
    });
  }
  const etag = object.httpEtag;
  if (matchesEtag(request, etag)) {
    return new Response(null, {
      status: 304,
      headers: { etag, "cache-control": INDEX_CACHE, ...CORS_ORIGIN },
    });
  }
  return new Response(request.method === "HEAD" ? null : object.body, {
    headers: {
      "content-type": "application/json; charset=utf-8",
      "content-length": String(object.size),
      etag,
      "cache-control": INDEX_CACHE,
      ...CORS_ORIGIN,
    },
  });
}

async function serveArtifact(request, env, pathname) {
  let key;
  try {
    key = decodeURIComponent(pathname.slice(1)); // "/artifacts/…" → "artifacts/…"
  } catch {
    return notFound("invalid artifact path encoding\n");
  }
  if (key === "artifacts/" || key.endsWith("/")) {
    return notFound("no such artifact (no directory listings)\n");
  }

  const object = await env.EXT_BUCKET.get(key);
  if (object === null) {
    return notFound(`artifact not found: ${key}\n`);
  }
  const etag = object.httpEtag;
  if (matchesEtag(request, etag)) {
    return new Response(null, { status: 304, headers: { etag, "cache-control": ARTIFACT_CACHE } });
  }
  return new Response(request.method === "HEAD" ? null : object.body, {
    headers: {
      "content-type": "application/octet-stream",
      "content-length": String(object.size),
      etag,
      "cache-control": ARTIFACT_CACHE,
    },
  });
}

function matchesEtag(request, etag) {
  const value = request.headers.get("if-none-match");
  return value === etag || value === "*";
}

function notFound(message) {
  return new Response(message, {
    status: 404,
    headers: { "content-type": TEXT, "cache-control": "no-store" },
  });
}
