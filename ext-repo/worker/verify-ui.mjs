#!/usr/bin/env node
// verify-ui.mjs — checks the repository Worker after the docs/Worker split.
//
//   node verify-ui.mjs [repo-base-url] [docs-base-url]
//
// Defaults to the live repository
// (https://pinkdb-ext-repo.defaultcms.workers.dev) and the live docs
// (https://pinkdb-docs.defaultcms.workers.dev). Checks, in order:
//   1. local Worker: GET / is the short info page — self-contained, no
//      catalog script, links to the docs Extensions page and /index.json,
//      styled with the docs' "soft data-zine" palette using system fonts
//      only; OPTIONS /index.json answers the CORS preflight; with a stubbed
//      R2 binding GET /index.json (200/304/404) carries
//      Access-Control-Allow-Origin: *; /artifacts/… keeps the 405 for
//      OPTIONS (only the catalog is CORS-enabled);
//   2. live Worker: GET / matches the page this checkout generates and keeps
//      cache-control: no-store; /index.json keeps its documented headers and
//      ETag/304 support and adds the CORS header (also on 304); artifacts
//      download byte-identically to the catalog sha256 and keep their exact
//      headers (no CORS added); unknown paths still 404 text/plain;
//   3. docs-side static checks on the generated site (website/public/, run
//      `python3 website/build.py` first): extensions.html exists, carries the
//      inline catalog script (fetch + copy affordances), and exposes the
//      pure helper section (__PURE_END__), which is lifted and run against
//      the live catalog — artifact URLs, search, hash shortening, size
//      formatting (missing `size` included), card/details rendering with the
//      install commands and the versions table, and the loading/empty/error/
//      not-found views; every generated page carries the Extensions nav entry
//      (the extensions page marks it active); no generated page loads an
//      external asset;
//   4. design system: the generated stylesheet and font files are the
//      "soft data-zine" set (paper/ink/pink tokens, three self-hosted OFL
//      families with licences, swap, tabular numerals, reduced-motion
//      guards); every page carries favicon + canonical + OpenGraph metadata;
//   5. live docs: GET <docs>/extensions answers 200 text/html with the
//      catalog script and the repository URL; a regular docs page keeps the
//      nav entry;
//   6. the local index.json mirror (gitignored) is compared as a warning.
//
// The docs-side checks assume the generated page points at the default live
// repository; they are skipped (with a warning) when a custom repo base URL
// is passed. Exits non-zero on the first failure group that fails.

import { createHash } from "node:crypto";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const REPO_DEFAULT = "https://pinkdb-ext-repo.defaultcms.workers.dev";
const DOCS_DEFAULT = "https://pinkdb-docs.defaultcms.workers.dev";
const REPO = (process.argv[2] || REPO_DEFAULT).replace(/\/+$/, "");
const DOCS = (process.argv[3] || DOCS_DEFAULT).replace(/\/+$/, "");
const HERE = dirname(fileURLToPath(import.meta.url));
const SITE = join(HERE, "..", "..", "website", "public");
const PURE_END = "/* __PURE_END__ */";

let passed = 0;
let failed = 0;

function check(ok, label, detail) {
  if (ok) {
    passed += 1;
    console.log(`ok    ${label}`);
  } else {
    failed += 1;
    console.error(`FAIL  ${label}${detail ? ` — ${detail}` : ""}`);
  }
}

function extractScript(page) {
  const open = page.indexOf("<script>");
  const close = page.indexOf("</scr" + "ipt>");
  if (open === -1 || close === -1 || close < open) return null;
  return page.slice(open + "<script>".length, close);
}

function loadPure(page) {
  const script = extractScript(page);
  if (script === null) return { error: "no inline <script> found" };
  const marker = script.indexOf(PURE_END);
  if (marker === -1) return { error: `no ${PURE_END} marker in the page script` };
  const prefix = script.slice(0, marker).replace(/^\s+/, "");
  try {
    // The prefix opens an IIFE; close it and return the `pure` object.
    const factory = new Function(`return (${prefix}\nreturn pure;\n}()));\n`);
    return { pure: factory() };
  } catch (error) {
    return { error: `pure section does not evaluate: ${error.message}` };
  }
}

// No external stylesheets/scripts/fonts/images on any generated page. The
// canonical <link rel="canonical"> is metadata (it points at the page's own
// URL), not an asset, so it is excluded.
const ASSET_PATTERNS = [
  [/<script\b[^>]*\bsrc=/i, "<script src="],
  [/<link\b(?![^>]*\brel="canonical")[^>]*\bhref="https?:/i, '<link href="http…"'],
  [/<img\b/i, "<img"],
  [/<iframe\b/i, "<iframe"],
  [/@import/i, "@import"],
  [/url\(\s*["']?https?:/i, "url(http…)"],
];

function assetViolations(html) {
  return ASSET_PATTERNS.filter(([re]) => re.test(html)).map(([, label]) => label);
}

function htmlFiles(dir) {
  const out = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) out.push(...htmlFiles(path));
    else if (entry.isFile() && entry.name.endsWith(".html")) out.push(path);
  }
  return out.sort();
}

// ------------------------------------------------------------- local Worker

const worker = await import(new URL("./src/index.js", import.meta.url));

const stubBody = JSON.stringify({
  format_version: 1,
  updated: "2026-10-14T00:00:00Z",
  extensions: [],
});
const stubEnv = {
  EXT_BUCKET: {
    async get() {
      return { httpEtag: '"stub"', size: stubBody.length, body: stubBody };
    },
  },
};
const emptyEnv = { EXT_BUCKET: { async get() { return null; } } };
const localFetch = (path, init) =>
  worker.default.fetch(new Request(`https://repo.invalid${path}`, init), stubEnv);

const localResponse = await localFetch("/");
const localPage = await localResponse.text();
check(localResponse.status === 200, "local GET / answers 200");
check(
  localResponse.headers.get("content-type") === "text/html; charset=utf-8",
  "local GET / serves text/html; charset=utf-8",
  localResponse.headers.get("content-type"),
);
check(
  localResponse.headers.get("cache-control") === "no-store",
  "local GET / keeps cache-control: no-store",
  localResponse.headers.get("cache-control"),
);
check(
  localPage.includes("<h1>pinkdb extension repository</h1>"),
  "info page carries the repository heading",
);
check(
  localPage.includes("https://pinkdb-docs.defaultcms.workers.dev/extensions"),
  "info page links to the docs Extensions page",
);
check(localPage.includes('href="/index.json"'), "info page links to /index.json");
check(localPage.includes("format v1"), "info page names the catalog format version");
check(
  localPage.includes("<style>") && localPage.includes("</style>"),
  "info page styles are inline (no external stylesheet)",
);
check(!localPage.includes("<script"), "info page no longer carries the catalog script");
check(
  assetViolations(localPage).length === 0,
  "info page loads no external assets",
  assetViolations(localPage).join(", "),
);
check(localPage.includes("--paper: #f7f1e8"), "info page uses the docs' paper palette");
check(
  localPage.includes("ui-sans-serif") && localPage.includes("ui-monospace"),
  "info page keeps to a system type stack",
);
check(
  !localPage.includes("@font-face") && !localPage.includes("fonts.googleapis"),
  "info page requests no webfonts (system stack only)",
);
check(
  localPage.includes("prefers-reduced-motion"),
  "info page guards its transitions behind prefers-reduced-motion",
);
check(
  localPage.includes('rel="icon"') && localPage.includes("data:image/svg+xml"),
  "info page carries an inline (data URI) favicon",
);
check(
  localPage.includes('class="tape"') && localPage.includes('class="sun"'),
  "info page keeps the zine motifs (tape strip, pink sun)",
);

const preflight = await localFetch("/index.json", { method: "OPTIONS" });
check(preflight.status === 204, "local OPTIONS /index.json answers 204", `HTTP ${preflight.status}`);
check(
  preflight.headers.get("access-control-allow-origin") === "*",
  "local OPTIONS /index.json allows any origin",
  preflight.headers.get("access-control-allow-origin"),
);
check(
  (preflight.headers.get("access-control-allow-methods") || "").includes("GET"),
  "local OPTIONS /index.json lists GET",
  preflight.headers.get("access-control-allow-methods"),
);
check(Boolean(preflight.headers.get("access-control-max-age")),
  "local OPTIONS /index.json carries access-control-max-age");

const artifactPreflight = await localFetch("/artifacts/x/y", { method: "OPTIONS" });
check(artifactPreflight.status === 405,
  "OPTIONS on /artifacts/… is not special-cased (405, machine endpoint unchanged)",
  `HTTP ${artifactPreflight.status}`);

const localIndex = await localFetch("/index.json");
check(localIndex.status === 200, "local GET /index.json (stubbed R2) answers 200");
check(
  localIndex.headers.get("access-control-allow-origin") === "*",
  "local GET /index.json carries Access-Control-Allow-Origin: *",
  localIndex.headers.get("access-control-allow-origin"),
);
const localNotModified = await localFetch("/index.json", {
  headers: { "if-none-match": '"stub"' },
});
check(
  localNotModified.status === 304 &&
    localNotModified.headers.get("access-control-allow-origin") === "*",
  "local /index.json 304 also carries the CORS header",
  `HTTP ${localNotModified.status}, ${localNotModified.headers.get("access-control-allow-origin")}`,
);
const localMissingIndex = await worker.default.fetch(
  new Request("https://repo.invalid/index.json"),
  emptyEnv,
);
check(
  localMissingIndex.status === 404 &&
    localMissingIndex.headers.get("access-control-allow-origin") === "*",
  "local missing /index.json 404 also carries the CORS header",
  `HTTP ${localMissingIndex.status}`,
);

// -------------------------------------------------------------- live Worker

let livePage = "";
try {
  const liveResponse = await fetch(`${REPO}/`, { headers: { accept: "text/html" } });
  livePage = await liveResponse.text();
  check(liveResponse.status === 200, `live GET ${REPO}/ answers 200`, `HTTP ${liveResponse.status}`);
  check(
    liveResponse.headers.get("cache-control") === "no-store",
    "live GET / keeps cache-control: no-store",
    liveResponse.headers.get("cache-control"),
  );
} catch (error) {
  check(false, `live GET ${REPO}/ succeeds`, error.message);
}

if (livePage !== "") {
  check(
    livePage.includes("https://pinkdb-docs.defaultcms.workers.dev/extensions") &&
      livePage.includes('href="/index.json"'),
    "live info page links to the docs Extensions page and /index.json",
  );
  check(livePage === localPage, "live page matches the page generated by this checkout");
  check(assetViolations(livePage).length === 0, "live info page loads no external assets");
}

// ------------------------------------------------------- live catalog + CORS

let catalog = null;
try {
  // Cloudflare weakens the ETag (W/"…") when it compresses a response, and the
  // Worker's If-None-Match handling is an exact match; request identity
  // encoding so the strong ETag round-trips.
  const response = await fetch(`${REPO}/index.json`, {
    headers: { "accept-encoding": "identity" },
  });
  const etag = response.headers.get("etag");
  check(response.status === 200, `live GET /index.json answers 200`, `HTTP ${response.status}`);
  check(
    response.headers.get("access-control-allow-origin") === "*",
    "/index.json carries Access-Control-Allow-Origin: *",
    response.headers.get("access-control-allow-origin"),
  );
  check(
    response.headers.get("content-type") === "application/json; charset=utf-8",
    "/index.json keeps application/json; charset=utf-8",
    response.headers.get("content-type"),
  );
  check(
    response.headers.get("cache-control") === "public, max-age=60",
    "/index.json keeps cache-control: public, max-age=60",
    response.headers.get("cache-control"),
  );
  check(Boolean(etag), "/index.json carries an ETag");
  const notModified = await fetch(`${REPO}/index.json`, {
    headers: { "if-none-match": etag || "", "accept-encoding": "identity" },
  });
  check(notModified.status === 304, "/index.json supports If-None-Match -> 304",
    `HTTP ${notModified.status}`);
  check(
    notModified.headers.get("access-control-allow-origin") === "*",
    "/index.json 304 keeps the CORS header",
    notModified.headers.get("access-control-allow-origin"),
  );
  const livePreflight = await fetch(`${REPO}/index.json`, { method: "OPTIONS" });
  check(
    livePreflight.status === 204 &&
      livePreflight.headers.get("access-control-allow-origin") === "*",
    "live OPTIONS /index.json answers the CORS preflight",
    `HTTP ${livePreflight.status}`,
  );
  catalog = await response.json();
  check(catalog && catalog.format_version === 1, "catalog is format v1",
    catalog && `format_version ${catalog.format_version}`);
  check(Array.isArray(catalog && catalog.extensions), "catalog has an extensions array");
} catch (error) {
  check(false, `live GET /index.json succeeds`, error.message);
}

// ------------------------------------------------- docs-side static checks

let docsPage = null;
const extensionsPath = join(SITE, "extensions.html");
if (!existsSync(extensionsPath)) {
  check(false, "generated docs site is present",
    `missing ${extensionsPath} (run: python3 website/build.py)`);
} else {
  docsPage = readFileSync(extensionsPath, "utf8");
  check(docsPage.includes("<script>"), "extensions page carries the inline catalog script");
  check(docsPage.includes("fetch(INDEX_URL"),
    "the catalog script fetches INDEX_URL (the cross-origin /index.json)");
  check(docsPage.includes(REPO_DEFAULT), "extensions page references the repository origin");
  check(
    docsPage.includes("data-copy") && docsPage.includes("navigator.clipboard"),
    "extensions page has copy affordances wired to the clipboard",
  );
  check(/<label[^>]*for="filter"/.test(docsPage),
    "extensions page search input has an associated <label>");
  check(
    docsPage.includes('<a href="extensions" class="active" aria-current="page">Extensions</a>'),
    "extensions page marks the Extensions nav entry active",
  );
  check(
    assetViolations(docsPage).length === 0,
    "extensions page loads no external assets",
    assetViolations(docsPage).join(", "),
  );
}

if (existsSync(SITE)) {
  const pages = htmlFiles(SITE);
  check(pages.length >= 11, "generated site has all pages", `${pages.length} html files`);
  for (const file of pages) {
    const rel = relative(SITE, file);
    const expected = rel.startsWith("docs/") ? "../extensions" : "extensions";
    const html = readFileSync(file, "utf8");
    const plain = `<a href="${expected}">Extensions</a>`;
    const active = `<a href="${expected}" class="active" aria-current="page">Extensions</a>`;
    // Count inside the site nav only: pages may also link here from content.
    const nav = (html.match(/<nav[\s\S]*?<\/nav>/) || [""])[0];
    const occurrences = nav.split(">Extensions</a>").length - 1;
    check(
      (nav.includes(plain) || nav.includes(active)) && occurrences === 1,
      `${rel} carries exactly one Extensions nav entry`,
      `${occurrences} occurrences`,
    );
    check(
      assetViolations(html).length === 0,
      `${rel} loads no external assets`,
      assetViolations(html).join(", "),
    );
    check(
      html.includes('rel="icon"') &&
        html.includes('rel="canonical"') &&
        html.includes('property="og:image"') &&
        html.includes('property="og:title"') &&
        html.includes("assets/style.css"),
      `${rel} carries favicon + canonical + OpenGraph + the stylesheet`,
    );
  }
}

// ------------------------------------------------ design-system static checks

const cssPath = join(SITE, "assets", "style.css");
if (existsSync(cssPath)) {
  const css = readFileSync(cssPath, "utf8");
  check(css.includes("font-display: swap"), "stylesheet uses font-display: swap");
  for (const family of ["Bricolage Grotesque", "Public Sans", "JetBrains Mono"]) {
    check(css.includes(`font-family: "${family}"`),
      `stylesheet declares the self-hosted ${family} face`);
  }
  check(css.includes("--paper: #f7f1e8") && css.includes("--ink: #241f1c"),
    "stylesheet keeps the paper/ink palette tokens");
  check(/font-variant-numeric:\s*tabular-nums/.test(css),
    "data surfaces use tabular numerals");
  check(css.includes("prefers-reduced-motion"),
    "stylesheet guards motion behind prefers-reduced-motion");
  check(!/@import|url\(\s*["']?https?:/i.test(css),
    "stylesheet loads no external fonts or imports");
  check(css.includes("dvh") || !css.includes("100vh"),
    "no 100vh sections (viewport-relative heights only)");
  check(/\.ticker-track\s*\{[^}]*animation/.test(css),
    "stylesheet animates the ticker strip");
} else {
  check(false, "generated stylesheet is present", `missing ${cssPath}`);
}

for (const [font, min] of [
  ["bricolage-grotesque.woff2", 20000],
  ["public-sans.woff2", 20000],
  ["public-sans-italic.woff2", 20000],
  ["jetbrains-mono.woff2", 20000],
]) {
  const path = join(SITE, "assets", "fonts", font);
  check(
    existsSync(path) && statSync(path).size > min,
    `self-hosted font ${font} ships in the generated site`,
    existsSync(path) ? `${statSync(path).size} bytes` : "missing",
  );
}
check(
  existsSync(join(SITE, "assets", "fonts", "OFL-Bricolage-Grotesque.txt")) &&
    existsSync(join(SITE, "assets", "fonts", "OFL-Public-Sans.md")) &&
    existsSync(join(SITE, "assets", "fonts", "OFL-JetBrains-Mono.txt")),
  "OFL licence texts ship next to the woff2 files",
);
check(
  existsSync(join(SITE, "assets", "favicon.svg")) &&
    existsSync(join(SITE, "assets", "favicon.png")) &&
    existsSync(join(SITE, "assets", "og.png")),
  "branded favicon (SVG + PNG) and the OpenGraph card are generated",
);

const landing = readFileSync(join(SITE, "index.html"), "utf8");
check(
  landing.includes('class="ticker"') && landing.includes("ticker-track"),
  "landing page carries the ticker strip",
);
check(landing.includes("cards zig"), "landing page uses the broken-grid card arrangement");
check(
  landing.includes('role="img"') && landing.includes("sk-before") && landing.includes("sk-after"),
  "landing page draws the benchmark as an inline sketch (before/after bars)",
);
check(
  readFileSync(join(SITE, "docs", "architecture.html"), "utf8").includes("sk-arrowhead"),
  "architecture page redraws the pipeline as an inline sketch",
);
check(
  readFileSync(join(SITE, "404.html"), "utf8").includes("404 — page not found"),
  "404 page is branded and self-describing",
);
check(
  readFileSync(join(SITE, "docs", "sql.html"), "utf8").includes('class="marginalia"'),
  "docs pages carry margin notes (marginalia)",
);

// ------------------------------------- pure helpers vs. the live catalog

let pure = null;
if (docsPage !== null) {
  const lifted = loadPure(docsPage);
  check(!lifted.error, "extensions page exposes a testable pure section", lifted.error);
  if (!lifted.error) pure = lifted.pure;
}

if (catalog && pure) {
  const entries = catalog.extensions;

  check(Object.keys(pure).length >= 20, "pure section exports the helper set",
    `${Object.keys(pure).length} keys`);
  check(typeof pure.indexUrl === "string" && pure.indexUrl.endsWith("/index.json"),
    "pure section exposes the catalog URL", pure.indexUrl);
  if (REPO === REPO_DEFAULT) {
    check(pure.indexUrl === `${REPO}/index.json`,
      "the docs page fetches the live repository's index.json", pure.indexUrl);
  } else {
    console.warn("warn  custom repo URL: skipping the docs page / repository URL equality check");
  }
  const pageRepo = pure.indexUrl.slice(0, -"/index.json".length);

  for (const entry of entries) {
    const artifactPath = pure.artifactPath(entry);
    check(
      pure.artifactUrl(entry) === `${pageRepo}/${entry.file}` && !artifactPath.includes("//"),
      `artifactUrl(${entry.name} ${entry.version}) derives the absolute repository URL`,
      pure.artifactUrl(entry),
    );

    const response = await fetch(`${REPO}${artifactPath}`);
    const bytes = Buffer.from(await response.arrayBuffer());
    check(response.status === 200, `GET ${artifactPath} answers 200`, `HTTP ${response.status}`);
    check(
      response.headers.get("content-type") === "application/octet-stream",
      "artifact keeps application/octet-stream",
      response.headers.get("content-type"),
    );
    check(
      (response.headers.get("cache-control") || "").includes("immutable"),
      "artifact keeps immutable caching",
      response.headers.get("cache-control"),
    );
    check(
      !response.headers.has("access-control-allow-origin"),
      "artifact headers are unchanged (no CORS added)",
      response.headers.get("access-control-allow-origin") || "",
    );
    const digest = createHash("sha256").update(bytes).digest("hex");
    check(digest === entry.sha256, `artifact sha256 matches the catalog for ${entry.name} ${entry.version}`,
      `${digest} vs ${entry.sha256}`);
    check(
      Number(response.headers.get("content-length")) === bytes.length,
      "artifact Content-Length matches the bytes",
      response.headers.get("content-length"),
    );

    if (Object.prototype.hasOwnProperty.call(entry, "size")) {
      check(entry.size === bytes.length,
        `entry size matches the artifact bytes (${entry.name} ${entry.version})`,
        `${entry.size} vs ${bytes.length}`);
    } else {
      check(pure.formatSize(entry.size) === "—",
        `missing size renders as an em dash (${entry.name} ${entry.version})`);
    }

    // Search matches name, description, and platform.
    check(pure.filterEntries(entries, entry.name).includes(entry),
      `search by name finds ${entry.name}`);
    check(pure.filterEntries(entries, entry.platform).includes(entry),
      `search by platform finds ${entry.platform}`);
    check(pure.filterEntries(entries, "zz-no-such-extension-zz").length === 0,
      "search with no match returns an empty list");
    check(pure.shortHash(entry.sha256) === `${entry.sha256.slice(0, 12)}…`,
      "SHA-256 is truncated to 12 hex chars plus an ellipsis");

    const card = pure.cardHtml(entry);
    check(
      card.includes(entry.name) &&
        card.includes(entry.version) &&
        card.includes(entry.platform) &&
        card.includes(String(entry.abi)) &&
        card.includes(pure.artifactUrl(entry)) &&
        card.includes("#/" + encodeURIComponent(entry.name)),
      `card renders name/version/platform/abi/download/details for ${entry.name}`,
    );

    const detail = pure.detailHtml([entry]);
    check(
      detail.includes(`.install ${entry.name}`) &&
        detail.includes(`.install ${entry.name}@${entry.version}`),
      `details render both install commands for ${entry.name}`,
    );
    check(
      detail.includes(pure.artifactUrl(entry)) &&
        detail.includes(entry.sha256) &&
        detail.includes("Raw catalog entry"),
      `details render the download link, the full SHA-256 and the raw entry for ${entry.name}`,
    );
    check(
      detail.includes("All versions and platforms") &&
        detail.includes(`<code>${entry.version}</code>`) &&
        detail.includes(`<code>${entry.platform}</code>`),
      `details always render the all-versions table (${entry.name})`,
    );
  }

  if (entries.length > 0) {
    const first = entries[0];
    const bumped = { ...first, version: "99.0.0", platform: "linux-x64", file: `artifacts/${first.name}/99.0.0/linux-x64/${first.name}.so` };
    delete bumped.size;
    delete bumped.sha256;
    const multi = pure.detailHtml([first, bumped]);
    check(
      multi.includes("All versions and platforms") &&
        multi.includes("99.0.0") &&
        multi.includes("linux-x64"),
      "details render the all-versions table when an extension has several entries",
    );
  }

  check(pure.formatSize(1024) === "1 KiB", "formatSize(1024) is 1 KiB", pure.formatSize(1024));
  check(pure.formatSize(0) === "0 B", "formatSize(0) is 0 B", pure.formatSize(0));
  check(pure.formatSize(undefined) === "—", "formatSize(undefined) is an em dash");
  check(pure.compareVersions("0.10.0", "0.9.0") > 0, "versions sort numerically (0.10.0 > 0.9.0)");
  check(pure.countText(1, 1, "") === "1 extension", "count text singular");
  check(pure.countText(0, 2, "zzz") === "0 of 2 extensions", "count text with an active filter");

  const emptyView = pure.listView({ format_version: 1, updated: null, extensions: [] }, "");
  check(emptyView.includes("No extensions have been published yet."),
    "empty catalog renders the empty state");
  check(pure.errorView("boom").includes("Catalog unavailable") &&
    pure.errorView("boom").includes("boom") &&
    pure.errorView("boom").includes('id="retry"'),
    "fetch failure renders the error state with a retry button");
  check(pure.notFoundView("name", "ghost").includes("No such extension") &&
    pure.notFoundView("path", "/a/b").includes("No such page"),
    "unknown hash routes render the not-found states");
  check(pure.loadingView().includes("spinner") && pure.loadingView().includes("index.json"),
    "loading state mentions the spinner and the catalog URL");
}

// ---------------------------------------------------------- live docs pages

try {
  const response = await fetch(`${DOCS}/extensions`, { headers: { accept: "text/html" } });
  const body = await response.text();
  check(response.status === 200, `live GET ${DOCS}/extensions answers 200`, `HTTP ${response.status}`);
  check(
    (response.headers.get("content-type") || "").startsWith("text/html"),
    "live extensions page is text/html",
    response.headers.get("content-type"),
  );
  check(body.includes(REPO_DEFAULT) && body.includes("fetch(INDEX_URL"),
    "live extensions page carries the catalog script");
  check(
    body.includes('<a href="extensions" class="active" aria-current="page">Extensions</a>'),
    "live extensions page marks the nav entry active",
  );
  check(assetViolations(body).length === 0, "live extensions page loads no external assets");
  if (docsPage !== null) {
    check(body === docsPage, "live extensions page matches the generated file");
  }
  const regular = await fetch(`${DOCS}/docs/sql`, { headers: { accept: "text/html" } });
  const regularBody = await regular.text();
  check(regular.status === 200 && regularBody.includes('<a href="../extensions">Extensions</a>'),
    "live docs pages keep the Extensions nav entry",
    `HTTP ${regular.status}`);
} catch (error) {
  check(false, `live GET ${DOCS}/extensions succeeds`, error.message);
}

// --------------------------------------------------------------- live 404

try {
  const response = await fetch(`${REPO}/no-such-path`);
  check(response.status === 404, "unknown paths still answer 404", `HTTP ${response.status}`);
  check(
    (response.headers.get("content-type") || "").startsWith("text/plain"),
    "404 keeps text/plain",
    response.headers.get("content-type"),
  );
} catch (error) {
  check(false, "live 404 check succeeds", error.message);
}

// ------------------------------------------------- local mirror (advisory)

const mirrorPath = join(HERE, "..", "index.json");
if (existsSync(mirrorPath) && catalog) {
  try {
    const mirror = JSON.parse(readFileSync(mirrorPath, "utf8"));
    const same =
      JSON.stringify(mirror.extensions) === JSON.stringify(catalog.extensions) &&
      mirror.format_version === catalog.format_version;
    if (!same) console.warn("warn  local ext-repo/index.json mirror differs from the live catalog (it may be stale)");
  } catch (error) {
    console.warn(`warn  local ext-repo/index.json mirror is unreadable: ${error.message}`);
  }
}

console.log(`\n${passed} passed, ${failed} failed (repo ${REPO}, docs ${DOCS})`);
process.exit(failed === 0 ? 0 : 1);
