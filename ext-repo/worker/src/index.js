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
const DOCS_URL = "https://pinkdb-docs.defaultcms.workers.dev";
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
  // Info page in the docs' product-docs language: neutral surfaces, one pink
  // accent, light + dark from prefers-color-scheme, the rounded-square brand
  // mark. System fonts only (the Worker ships no font files, and nothing here
  // may hit a third party). Styling lives inline; no scripts.
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>pinkdb extension repository</title>
<meta name="description" content="Machine endpoints of the pinkdb extension repository (format v1): the catalog at /index.json (CORS *), artifact bytes under /artifacts/. The browsable catalog UI lives in the pinkdb docs.">
<meta name="color-scheme" content="light dark">
<meta name="theme-color" content="#ffffff" media="(prefers-color-scheme: light)">
<meta name="theme-color" content="#0e0d10" media="(prefers-color-scheme: dark)">
<meta property="og:type" content="website">
<meta property="og:title" content="pinkdb extension repository">
<meta property="og:description" content="Format v1 machine endpoints: /index.json (CORS *) and immutable artifact bytes under /artifacts/.">
<meta property="og:url" content="https://pinkdb-ext-repo.defaultcms.workers.dev/">
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Crect x='1' y='1' width='22' height='22' rx='6.5' fill='%23c2255c'/%3E%3Cg fill='%23fff'%3E%3Crect x='6' y='12' width='3' height='6' rx='1.2'/%3E%3Crect x='10.5' y='8' width='3' height='10' rx='1.2'/%3E%3Crect x='15' y='5.5' width='3' height='12.5' rx='1.2'/%3E%3C/g%3E%3C/svg%3E">
<style>
:root {
  color-scheme: light dark;
  --bg: #ffffff;
  --bg-subtle: #fafafb;
  --surface: #f6f6f8;
  --border: #e6e4ea;
  --border-strong: #d3d0d9;
  --text: #17151b;
  --text-2: #47434f;
  --text-3: #6b6575;
  --accent: #c2255c;
  --accent-text: #b81f56;
  --accent-soft: #fdf0f5;
  --accent-border: #f5c6d8;
  --on-accent: #ffffff;
  --code-bg: #f7f7f9;
  --sans: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
  --radius-sm: 6px;
  --radius: 10px;
  --radius-lg: 14px;
  --shadow-sm: 0 1px 2px rgba(23, 21, 27, 0.05);
  --shadow: 0 1px 2px rgba(23, 21, 27, 0.04), 0 4px 16px -4px rgba(23, 21, 27, 0.08);
  --ease: cubic-bezier(0.2, 0.7, 0.2, 1);
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0e0d10;
    --bg-subtle: #121115;
    --surface: #17161b;
    --border: #2a2830;
    --border-strong: #3a3742;
    --text: #edebf0;
    --text-2: #bdb8c5;
    --text-3: #9a95a3;
    --accent: #f472a6;
    --accent-text: #f58fb6;
    --accent-soft: #2a1520;
    --accent-border: #5a2a3f;
    --on-accent: #1b0a12;
    --code-bg: #141317;
    --shadow-sm: 0 1px 2px rgba(0, 0, 0, 0.4);
    --shadow: 0 1px 2px rgba(0, 0, 0, 0.3), 0 6px 20px -6px rgba(0, 0, 0, 0.5);
  }
}
* { box-sizing: border-box; }
html { background: var(--bg); }
body {
  margin: 0;
  background: var(--bg);
  color: var(--text);
  font-family: var(--sans);
  font-size: 16px;
  line-height: 1.65;
  -webkit-font-smoothing: antialiased;
  text-rendering: optimizeLegibility;
}
::selection { background: var(--accent-soft); color: var(--text); }
:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; border-radius: 4px; }
a { color: var(--accent-text); text-underline-offset: 3px; }
.top {
  border-bottom: 1px solid var(--border);
  background: var(--bg);
}
.top-inner {
  display: flex;
  align-items: center;
  gap: 1rem;
  max-width: 56rem;
  height: 3.75rem;
  margin-inline: auto;
  padding-inline: clamp(1.25rem, 5vw, 2rem);
}
.brand {
  display: inline-flex;
  align-items: center;
  gap: 0.55rem;
  color: var(--text);
  text-decoration: none;
}
.brand-mark { width: 1.6rem; height: 1.6rem; flex: none; }
.brand-mark-bg { fill: var(--accent); }
.brand-mark-bar { fill: var(--on-accent); }
.brand-name { font-weight: 700; font-size: 1.15rem; letter-spacing: -0.03em; }
.brand-tag {
  font-size: 0.75rem;
  font-weight: 600;
  color: var(--text-3);
  border: 1px solid var(--border);
  border-radius: 999px;
  padding: 0.05rem 0.5rem;
  line-height: 1.5;
}
.brand:hover .brand-name { color: var(--accent-text); }
.top-link {
  margin-left: auto;
  padding: 0.38rem 0.75rem;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  color: var(--text-2);
  font-size: 0.875rem;
  font-weight: 500;
  text-decoration: none;
  transition: color 0.15s var(--ease), border-color 0.15s var(--ease), background-color 0.15s var(--ease);
}
.top-link:hover { color: var(--text); border-color: var(--border-strong); background: var(--surface); }
main {
  max-width: 56rem;
  margin-inline: auto;
  padding: clamp(2.5rem, 8vw, 4.5rem) clamp(1.25rem, 5vw, 2rem) clamp(3rem, 8vw, 5rem);
}
.kicker {
  display: inline-flex;
  align-items: center;
  gap: 0.5rem;
  margin: 0 0 1.25rem;
  padding: 0.25rem 0.75rem 0.25rem 0.3rem;
  border: 1px solid var(--border);
  border-radius: 999px;
  background: var(--bg);
  color: var(--text-2);
  font-size: 0.82rem;
  font-weight: 500;
  box-shadow: var(--shadow-sm);
}
.kicker b {
  padding: 0.05rem 0.5rem;
  border-radius: 999px;
  background: var(--accent-soft);
  color: var(--accent-text);
  font-size: 0.75rem;
  font-weight: 650;
}
h1 {
  margin: 0 0 0.9rem;
  font-size: clamp(2rem, 5.4vw, 3rem);
  font-weight: 750;
  line-height: 1.05;
  letter-spacing: -0.04em;
  max-width: 20ch;
  text-wrap: balance;
}
h2 {
  margin: 2.75rem 0 0.75rem;
  font-size: 0.8rem;
  font-weight: 650;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--text-3);
}
.lede { margin: 0; font-size: 1.125rem; line-height: 1.6; color: var(--text-2); max-width: 60ch; }
p { max-width: 68ch; }
.links {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(16rem, 1fr));
  gap: 1rem;
  list-style: none;
  margin: 2.25rem 0 0;
  padding: 0;
}
.card {
  position: relative;
  padding: 1.1rem 1.2rem 1.2rem;
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  background: var(--bg);
  transition: border-color 0.15s var(--ease), box-shadow 0.15s var(--ease);
}
.card:hover { border-color: var(--border-strong); box-shadow: var(--shadow); }
.card.primary { border-color: var(--accent-border); background: var(--accent-soft); }
.card.primary:hover { border-color: var(--accent); }
.num {
  display: inline-block;
  margin-bottom: 0.45rem;
  font-family: var(--mono);
  font-size: 0.72rem;
  font-weight: 600;
  letter-spacing: 0.08em;
  color: var(--accent-text);
}
.card a {
  display: block;
  font-weight: 650;
  font-size: 1.02rem;
  color: var(--text);
  text-decoration: none;
}
/* Whole-card links (one link per card). */
.card a::after { content: ""; position: absolute; inset: 0; border-radius: inherit; }
.card:hover a { color: var(--accent-text); }
.note { display: block; margin-top: 0.3rem; color: var(--text-2); font-size: 0.92rem; line-height: 1.6; }
code {
  font-family: var(--mono);
  font-size: 0.86em;
  font-variant-numeric: tabular-nums;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 5px;
  padding: 0.1em 0.35em;
  overflow-wrap: anywhere;
}
.route {
  margin: 0 0 0.9rem;
  padding: 0.95rem 1.1rem;
  background: var(--code-bg);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  font-family: var(--mono);
  font-size: 0.84rem;
  line-height: 1.65;
  color: var(--text);
  overflow-x: auto;
}
.route .seg { color: var(--accent-text); }
.meta { margin: 0; font-size: 0.9rem; color: var(--text-3); max-width: 70ch; }
.chips { display: flex; flex-wrap: wrap; gap: 0.5rem; list-style: none; margin: 1.25rem 0 0; padding: 0; }
.chips li {
  padding: 0.2rem 0.65rem;
  border: 1px solid var(--border);
  border-radius: 999px;
  background: var(--bg-subtle);
  color: var(--text-2);
  font-family: var(--mono);
  font-size: 0.75rem;
  font-variant-numeric: tabular-nums;
}
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after { transition-duration: 0.001ms !important; }
}
@media (max-width: 30rem) {
  .brand-tag { display: none; }
}
</style>
</head>
<body>
<header class="top">
  <div class="top-inner">
    <a class="brand" href="${DOCS_URL}/" aria-label="pinkdb docs">
      <svg class="brand-mark" viewBox="0 0 24 24" aria-hidden="true" focusable="false">
        <rect x="1" y="1" width="22" height="22" rx="6.5" class="brand-mark-bg"/>
        <rect x="6" y="12" width="3" height="6" rx="1.2" class="brand-mark-bar"/>
        <rect x="10.5" y="8" width="3" height="10" rx="1.2" class="brand-mark-bar"/>
        <rect x="15" y="5.5" width="3" height="12.5" rx="1.2" class="brand-mark-bar"/>
      </svg>
      <span class="brand-name">pinkdb</span>
      <span class="brand-tag">extensions</span>
    </a>
    <a class="top-link" href="${DOCS_URL}/">Docs</a>
  </div>
</header>
<main>
  <p class="kicker"><b>format v${FORMAT_VERSION}</b> Extension repository</p>
  <h1>pinkdb extension repository</h1>
  <p class="lede">Hosted, immutable catalog of built pinkdb extension artifacts
  (C ABI v1, format v${FORMAT_VERSION}). This Worker serves the machine
  endpoints; the browsable catalog UI lives in the docs.</p>

  <ul class="links">
    <li class="card primary">
      <span class="num">01</span>
      <a href="${DOCS_URL}/extensions">Browse the catalog in the docs</a>
      <span class="note">Search, per-extension metadata, <code>.install &lt;name&gt;</code> commands and downloads.</span>
    </li>
    <li class="card">
      <span class="num">02</span>
      <a href="/index.json">GET /index.json</a>
      <span class="note">The raw catalog: plain JSON, ETag, <code>Access-Control-Allow-Origin: *</code>.</span>
    </li>
  </ul>

  <h2>Artifact route</h2>
  <pre class="route">/artifacts/<span class="seg">&lt;name&gt;</span>/<span class="seg">&lt;version&gt;</span>/<span class="seg">&lt;platform&gt;</span>/<span class="seg">&lt;file&gt;</span></pre>
  <p class="meta">format v${FORMAT_VERSION} &#183; immutable bytes, byte-identical downloads
  &#183; always verify the SHA-256 from the catalog before loading. No directory
  listings, no writes.</p>
  <ul class="chips">
    <li>cors: /index.json only</li>
    <li>catalog cache: 60 s</li>
    <li>artifacts: immutable, 1 y</li>
    <li>405 on non-GET</li>
  </ul>
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
