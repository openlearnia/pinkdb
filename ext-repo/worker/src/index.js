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
  // "Soft data-zine" info page: the same paper/ink/pink language as the docs
  // site, with system fonts only (the Worker ships no font files, and nothing
  // here may hit a third party). Styling lives inline; no scripts.
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>pinkdb extension repository</title>
<meta name="description" content="Machine endpoints of the pinkdb extension repository (format v1): the catalog at /index.json (CORS *), artifact bytes under /artifacts/. The browsable catalog UI lives in the pinkdb docs.">
<meta name="color-scheme" content="light">
<meta name="theme-color" content="#F7F1E8">
<meta property="og:type" content="website">
<meta property="og:title" content="pinkdb extension repository">
<meta property="og:description" content="Format v1 machine endpoints: /index.json (CORS *) and immutable artifact bytes under /artifacts/.">
<meta property="og:url" content="https://pinkdb-ext-repo.defaultcms.workers.dev/">
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='8' fill='%23F7F1E8'/%3E%3Cpath d='M7.5 10.5h11M7.5 15.5h7.5' stroke='%23241F1C' stroke-width='2.6' stroke-linecap='round'/%3E%3Ccircle cx='21.4' cy='21.4' r='5.9' fill='%23C9557A' stroke='%23241F1C' stroke-width='2.4'/%3E%3C/svg%3E">
<style>
:root {
  color-scheme: light;
  --paper: #f7f1e8;
  --paper-2: #fcf8f1;
  --paper-3: #efe6d6;
  --tape: rgba(228, 214, 191, 0.85);
  --ink: #241f1c;
  --ink-2: #52483f;
  --ink-3: #6e6156;
  --line-strong: #b9a88f;
  --pink: #c9557a;
  --pink-deep: #a63a5e;
  --pink-soft: #f4dbe2;
  --blue: #2c4a66;
  --sans: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
  --shadow-sm: 2px 3px 0 rgba(36, 31, 28, 0.2);
  --shadow: 4px 5px 0 rgba(36, 31, 28, 0.16);
}
* { box-sizing: border-box; }
html { background: var(--paper); }
body {
  margin: 0;
  background: var(--paper);
  color: var(--ink);
  font-family: var(--sans);
  font-size: 16.5px;
  line-height: 1.65;
  -webkit-font-smoothing: antialiased;
}
/* One fixed, non-interactive paper grain over everything. */
body::after {
  content: "";
  position: fixed;
  inset: 0;
  z-index: 80;
  pointer-events: none;
  opacity: 0.34;
  mix-blend-mode: multiply;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='190' height='190'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.82' numOctaves='2' stitchTiles='stitch'/%3E%3CfeColorMatrix type='saturate' values='0'/%3E%3C/filter%3E%3Crect width='190' height='190' filter='url(%23n)' opacity='0.2'/%3E%3C/svg%3E");
  background-size: 190px 190px;
}
::selection { background: var(--pink-soft); color: var(--ink); }
:focus-visible { outline: 3px solid var(--blue); outline-offset: 3px; border-radius: 3px; }
main {
  position: relative;
  max-width: 60rem;
  margin-inline: auto;
  padding: clamp(2.6rem, 9vw, 5.5rem) clamp(1.25rem, 5vw, 3rem) clamp(3rem, 8vw, 5rem);
}
.sun {
  position: absolute;
  top: clamp(0.5rem, 4vw, 2rem);
  right: clamp(1rem, 5vw, 3rem);
  width: clamp(5rem, 16vw, 9rem);
  height: clamp(5rem, 16vw, 9rem);
  border-radius: 50%;
  background: var(--pink);
  border: 2.5px solid var(--ink);
  box-shadow: 6px 8px 0 rgba(36, 31, 28, 0.14);
  rotate: 7deg;
  pointer-events: none;
}
.tape {
  position: absolute;
  top: clamp(2rem, 7vw, 4rem);
  left: clamp(1.4rem, 6vw, 3.4rem);
  width: 5.2rem;
  height: 1.4rem;
  background: var(--tape);
  border-inline: 1px dashed rgba(36, 31, 28, 0.28);
  rotate: -3deg;
  pointer-events: none;
}
.kicker {
  display: inline-block;
  margin: 0 0 1.1rem;
  padding: 0.18rem 0.62rem 0.22rem;
  font-family: var(--mono);
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--ink-2);
  background: var(--paper-3);
  border: 1.6px solid var(--ink);
  border-radius: 4px 8px 5px 9px;
  box-shadow: var(--shadow-sm);
  rotate: -1.6deg;
}
h1 {
  margin: 0 0 0.9rem;
  font-family: var(--sans);
  font-size: clamp(2rem, 5.6vw, 3.1rem);
  font-weight: 800;
  line-height: 1.02;
  letter-spacing: -0.035em;
  max-width: 22ch;
}
.lede { margin: 0 0 0.4rem; font-size: 1.1rem; line-height: 1.6; color: var(--ink-2); max-width: 62ch; }
p { max-width: 68ch; }
.cut { border: 0; border-top: 2px dashed var(--line-strong); margin: 2.1rem 0 1.9rem; }
.links { list-style: none; padding: 0; margin: 0 0 2rem; display: grid; gap: 1.2rem; }
.card {
  position: relative;
  background: var(--paper-2);
  border: 1.8px solid var(--ink);
  border-radius: 10px 16px 12px 18px;
  box-shadow: var(--shadow);
  padding: 1.05rem 1.2rem 1.15rem;
  transition: translate 0.2s cubic-bezier(0.2, 0.7, 0.2, 1), box-shadow 0.2s cubic-bezier(0.2, 0.7, 0.2, 1), rotate 0.2s cubic-bezier(0.2, 0.7, 0.2, 1);
}
.card:hover { translate: -2px -3px; rotate: -0.4deg; box-shadow: 6px 8px 0 rgba(36, 31, 28, 0.16); }
.card::before {
  content: "";
  position: absolute;
  top: -0.66rem;
  left: 1.5rem;
  width: 4.4rem;
  height: 1.28rem;
  background: var(--tape);
  border-inline: 1px dashed rgba(36, 31, 28, 0.26);
  rotate: -2.2deg;
  pointer-events: none;
}
.num {
  display: inline-block;
  margin-bottom: 0.5rem;
  padding: 0.06rem 0.45rem 0.1rem;
  font-family: var(--mono);
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.12em;
  color: var(--pink-deep);
  border: 1.5px solid var(--ink);
  border-radius: 3px 6px 4px 7px;
  box-shadow: var(--shadow-sm);
  rotate: -2deg;
}
.card a {
  display: block;
  font-weight: 650;
  font-size: 1.05rem;
  color: var(--blue);
  text-decoration: underline;
  text-decoration-thickness: 1.5px;
  text-underline-offset: 3px;
  text-decoration-color: rgba(44, 74, 102, 0.35);
}
.card a:hover { color: var(--pink-deep); text-decoration-color: var(--pink); }
.note { display: block; margin-top: 0.25rem; color: var(--ink-2); font-size: 0.9rem; }
code {
  font-family: var(--mono);
  font-size: 0.85em;
  font-variant-numeric: tabular-nums;
  background: var(--paper-3);
  border: 1px solid var(--line-strong);
  border-radius: 6px;
  padding: 0.06em 0.32em;
  overflow-wrap: anywhere;
}
.route {
  margin: 0 0 1.1rem;
  padding: 0.95rem 1.05rem 1rem;
  background: var(--paper-2);
  background-image: repeating-linear-gradient(to bottom, transparent 0 27px, rgba(44, 74, 102, 0.05) 27px 28px);
  border: 1.8px solid var(--ink);
  border-radius: 8px 12px 9px 13px;
  box-shadow: var(--shadow);
  font-family: var(--mono);
  font-size: 0.84rem;
  color: var(--ink);
  overflow-x: auto;
}
.meta { margin: 0.4rem 0 0; font-family: var(--mono); font-size: 0.78rem; color: var(--ink-3); max-width: 70ch; }
.chips { display: flex; flex-wrap: wrap; gap: 0.55rem; list-style: none; margin: 1.5rem 0 0; padding: 0; }
.chips li {
  font-family: var(--mono);
  font-size: 0.74rem;
  color: var(--ink-2);
  background: var(--paper-2);
  border: 1.6px solid var(--ink);
  border-radius: 5px 9px 6px 10px;
  padding: 0.2rem 0.6rem 0.24rem;
  box-shadow: var(--shadow-sm);
  font-variant-numeric: tabular-nums;
}
.chips li:nth-child(odd) { rotate: -1.3deg; background: var(--paper-3); }
.chips li:nth-child(even) { rotate: 1deg; }
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after { transition-duration: 0.001ms !important; }
}
@media (max-width: 40rem) {
  body { font-size: 16px; }
  .sun { opacity: 0.85; }
}
</style>
</head>
<body>
<main>
  <span class="sun" aria-hidden="true"></span>
  <span class="tape" aria-hidden="true"></span>
  <p class="kicker">extension repository &#183; format v${FORMAT_VERSION}</p>
  <h1>pinkdb extension repository</h1>
  <p class="lede">Hosted, immutable catalog of built pinkdb extension artifacts
  (C ABI v1, format v${FORMAT_VERSION}). This Worker serves the machine
  endpoints; the browsable catalog UI lives in the docs.</p>

  <hr class="cut">

  <ul class="links">
    <li class="card">
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

  <pre class="route">/artifacts/&lt;name&gt;/&lt;version&gt;/&lt;platform&gt;/&lt;file&gt;</pre>
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
