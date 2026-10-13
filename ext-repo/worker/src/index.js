// pinkdb extension repository — Worker front door for the `pinkdb-extensions`
// R2 bucket. Format v1 (see ../FORMAT.md).
//
//   GET /            → tiny info page
//   GET /index.json  → the catalog (short cache + ETag)
//   GET /artifacts/… → artifact bytes (octet-stream, Content-Length, ETag,
//                      immutable caching — artifacts are immutable)
//   anything else    → 404 (no directory listings)
//
// Module syntax, no build step, no dependencies.

const FORMAT_VERSION = 1;
const INDEX_KEY = "index.json";
const INDEX_CACHE = "public, max-age=60";
const ARTIFACT_CACHE = "public, max-age=31536000, immutable";
const TEXT = "text/plain; charset=utf-8";

export default {
  async fetch(request, env) {
    const { pathname } = new URL(request.url);

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
  return [
    "<!doctype html>",
    '<meta charset="utf-8">',
    "<title>pinkdb extensions</title>",
    "<h1>pinkdb extensions repository</h1>",
    `<p>Format version: ${FORMAT_VERSION}</p>`,
    '<p>Catalog: <a href="/index.json">/index.json</a></p>',
    "",
  ].join("\n");
}

async function serveIndex(request, env) {
  const object = await env.EXT_BUCKET.get(INDEX_KEY);
  if (object === null) {
    return notFound("index.json not found: the catalog has not been published yet\n");
  }
  const etag = object.httpEtag;
  if (matchesEtag(request, etag)) {
    return new Response(null, { status: 304, headers: { etag, "cache-control": INDEX_CACHE } });
  }
  return new Response(request.method === "HEAD" ? null : object.body, {
    headers: {
      "content-type": "application/json; charset=utf-8",
      "content-length": String(object.size),
      etag,
      "cache-control": INDEX_CACHE,
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
