# pinkdb console — design system

Status: **implemented** in wave 121c (2026-10-30). The live stylesheet is
`crates/pinkdb-server-ui/src/assets/console.css`; the copy in this folder is
the design snapshot the mockup and gallery use.

The warehouse console (`pinkdb-server --enable-ui`) grew feature by feature
across waves 78, 121 and 121b. Each wave added its own colors, spacing and
widgets, so it has no system: about 40 raw color and size values, five tabs
squeezed into a 270 px sidebar, admin forms in the catalog tree, unicode
glyphs as icons (▶ ⏱ ⏳), and lowercase button labels next to Title Case
ones. This document defines the system that replaces it. Three files go with
it:

| File | What it is |
| --- | --- |
| [`console.css`](console.css) | Tokens, base styles and every component: the design snapshot. The app's stylesheet starts from it and adds app-only parts (virtual grid, editor overlay, CSP-safe size classes). |
| [`mockup.html`](mockup.html) | A clickable mockup of the shell with every section: Query, Catalog, History, Access, Storage, Ask, Import, login. Static data. |
| [`components.html`](components.html) | The component gallery: each component with its variants and states, in light and dark. |

To view them, serve this folder (`python3 -m http.server -d docs/console-design 8793`)
and open `mockup.html`. The theme switch in the top bar flips light and dark.

---

## 1. Principles

1. **The data is the brightest thing on screen.** Chrome is neutral and
   quiet; the grid, the SQL and the numbers carry the contrast. The accent
   color marks only the primary action, the current selection and focus.
2. **Dense, not cramped.** Warehouse users scan thousands of rows: 13 px UI
   text, 28 px controls, 24 px grid rows, a 4 px spacing grid. Hierarchy
   comes from weight, tone and spacing, not from boxes inside boxes.
3. **One place for each kind of thing.** The rail picks a section, the
   explorer lists its objects, the workspace holds the work, the inspector
   shows details. Admin screens are pages, not sidebar panels.
4. **State is always visible.** Connection, database context, role,
   read-only mode, durability and the running query live in the status bar,
   where they never scroll away.
5. **Keyboard first.** Every action has a focus state; frequent actions have
   shortcuts, and the UI shows them (`⌘↵`, `⌘K`, `Esc`).
6. **Every value is a token.** Components use semantic tokens only
   (`--text-muted`, `--border`), never raw hex or pixel values. A new
   component that needs a new value adds a token first.

## 2. Layout

### 2.1 The shell

```
┌────────────────────────────────────────────────────────────────────────┐
│ Top bar 44   [◆ pinkdb] [▾ main]   [⌘K Search…]          Import  Ask  ☺ │
├────┬──────────────┬──────────────────────────────────────┬─────────────┤
│Rail│ Explorer     │ Workspace                            │ Inspector   │
│ 52 │ 272 (200-480)│ fluid                                │ 380         │
│    │              │                                      │ (optional)  │
│ Q  │ section's    │ page header or worksheet tabs        │ Ask, cell   │
│ C  │ objects:     │ toolbar                              │ viewer,     │
│ H  │ tree, list,  │ content                              │ object      │
│ ── │ filters      │                                      │ details     │
│ A  │              │                                      │             │
│ S  │              │                                      │             │
├────┴──────────────┴──────────────────────────────────────┴─────────────┤
│ Status bar 26  ● Connected · main · admin · durable       12 rows · 4 ms│
└────────────────────────────────────────────────────────────────────────┘
```

| Region | Size | Role |
| --- | --- | --- |
| Top bar | 44 px | Brand, database context, command search, global actions (Import, Ask), account menu. |
| Rail | 52 px | Section switcher. Icon plus a 10 px label. The data sections sit on top; the admin sections (Access, Storage) sit under a divider. |
| Explorer | 272 px, 200–480 px resizable, collapsible (`⌘B`) | The section's objects. Only Query and Catalog have one; History, Access and Storage are full-width pages. |
| Workspace | fluid, min 480 px | The work: a worksheet, or a page. |
| Inspector | 380 px, optional | Contextual detail on the right: the Ask agent, the cell viewer, an object's details. One at a time; `Esc` closes it. |
| Status bar | 26 px | Session state on the left, the active run on the right. Notices appear here, not in the top bar. |

Panels are separated by a 1 px `--border` line, never by gaps or shadows.
Only floating things (menus, dialogs, toasts) have shadows.

### 2.2 Sections

| Section | Explorer | Workspace |
| --- | --- | --- |
| **Query** (default) | *Objects* / *Worksheets* switch; the catalog tree or the saved worksheet list | Worksheet tabs → sheet toolbar → SQL editor → splitter → results |
| **Catalog** | The catalog tree | The selected object's page: header with kind, row count and actions; *Columns* (name, type, nulls, distinct, min, max), *Preview* and, for lake tables, *History* tabs |
| **History** | — | A filterable table of runs: status, statement, worksheet, rows, duration, started. A row opens its SQL in a new worksheet. |
| **Access** | — | *Users and roles* table and *Grants* table; *New user*, *New role* and *Grant* open dialogs. Non-admins see their own principals read-only. |
| **Storage** | — | Summary cards (mode, location, WAL), the facts table, import formats, *Checkpoint* and *Import data*. |

Every page uses the same page header: title (20 px), a one-line description,
and actions on the right with the primary one last.

### 2.3 The worksheet

```
┌ tabs ───────────────────────────────────────────────────────────────┐
│ ▣ Revenue by month ●  ▣ orders preview   ▣ Untitled   +              │
├ toolbar ────────────────────────────────────────────────────────────┤
│ [Revenue by month      ]  ▾ main    Saved          Share  [▶ Run ▾] │
├ editor ─────────────────────────────────────────────────────────────┤
│  1 │ SELECT date_trunc('month', o_orderdate) AS month,              │
│  2 │        sum(o_totalprice) AS revenue                            │
├ splitter (drag, double-click resets) ───────────────────────────────┤
│ Result 1  Result 2  Error                 [Grid Chart Plan Profile] │
│ 1,204 rows · 38 ms                              ⧉ Copy  ↓ CSV ↓ JSON│
│ # │ month        │ revenue                                          │
│ 1 │ 1992-01-01   │ 4,128,551.20                                     │
└─────────────────────────────────────────────────────────────────────┘
```

* **Run** is the one primary button on the screen. Its menu holds *Run
  statement at cursor* (`⌘↵`), *Run all* (`⇧⌘↵`) and *Explain*. While a run
  is in flight it turns into a secondary **Cancel** with a spinner.
* The editor gets a line-number gutter and a current-line highlight.
* The splitter between editor and results is draggable; the ratio is kept
  per tab.
* Results views are a segmented control on the right; statement tabs on the
  left use the underline tab style. The summary line (rows, duration,
  truncation) sits under them, downloads at its right.

## 3. Tokens

All tokens are CSS custom properties on `:root`, overridden under
`[data-theme="dark"]` and `prefers-color-scheme: dark`. Components use the
**semantic** tokens; the **palette** ramps exist only to define them.

### 3.1 Color — palette

Neutral is a slightly cool gray; the accent is pinkdb's raspberry.

| Step | Gray | Accent |
| --- | --- | --- |
| 0 | `#ffffff` | — |
| 25 | `#fafafb` | — |
| 50 | `#f4f4f6` | `#fff0f5` |
| 100 | `#ececef` | `#ffdeeb` |
| 200 | `#e1e1e6` | `#fcc2d7` |
| 300 | `#cdcdd4` | `#faa2c1` |
| 400 | `#a7a7b1` | `#f783ac` |
| 500 | `#6e6e79` | `#f06595` |
| 600 | `#55555f` | `#e64980` |
| 700 | `#474750` | `#d6336c` |
| 800 | `#2d2d34` | `#c2255c` |
| 900 | `#1c1c21` | `#a61e4d` |
| 950 | `#121215` | — |

Status hues (light text / light tint · dark text / dark tint): green
`#237032`/`#ebfbee` · `#69db7c`/`#12291a`, amber `#9a4d00`/`#fff4e6` ·
`#ffc078`/`#2e2110`, red `#c92a2a`/`#fff5f5` · `#ff8787`/`#2f1618`, blue
`#1864ab`/`#e7f5ff` · `#74c0fc`/`#0f2233`. Every pair is at least 5:1.

### 3.2 Color — semantic

| Token | Light | Dark | Use |
| --- | --- | --- | --- |
| `--bg-canvas` | gray-25 | `#0e0e11` | App background, workspace behind pages |
| `--bg-surface` | gray-0 | `#151519` | Panels, cards, editor, grid |
| `--bg-subtle` | gray-50 | `#1b1b20` | Top bar, toolbars, table headers, inactive tabs |
| `--bg-muted` | gray-100 | `#232329` | Hover fill, code chips, tracks |
| `--bg-inset` | gray-50 | `#0b0b0d` | Inputs in dark mode, sunken wells |
| `--bg-overlay` | `rgb(18 18 21 / .45)` | `rgb(0 0 0 / .6)` | Dialog backdrop |
| `--border` | gray-200 | `#2a2a31` | Panel dividers, table rules, control borders |
| `--border-subtle` | gray-100 | `#202026` | Grid cell rules |
| `--border-strong` | gray-300 | `#3a3a43` | Hovered controls |
| `--text` | gray-900 | `#ececf0` | Body text, data |
| `--text-muted` | gray-600 | `#a0a0ab` | Secondary text, labels, types |
| `--text-subtle` | gray-500 | `#878793` | Placeholders, NULL, row numbers, disabled |
| `--text-on-accent` | `#ffffff` | `#ffffff` | Text on the primary button |
| `--accent` | accent-700 | accent-700 | Primary button, active indicators |
| `--accent-hover` | accent-800 | accent-800 | Primary hover |
| `--accent-text` | accent-800 | accent-400 | Accent-colored text and icons |
| `--accent-soft` | accent-50 | `rgb(230 73 128 / .14)` | Selected row, active nav item |
| `--focus` | accent-600 | accent-500 | Focus ring |
| `--selection` | `rgb(28 111 209 / .12)` | `rgb(116 192 252 / .16)` | Selected grid cells, text selection |
| `--{success,warning,danger,info}` | status text | status text | Status text and icons |
| `--{success,warning,danger,info}-soft` | status tint | status tint | Callout and badge fills |

Selection in the grid is **blue**, not the accent: pink stays for brand and
action, and blue is the convention spreadsheet users expect.

### 3.3 Typography

System fonts only (the console ships no font files and loads nothing
external).

* `--font-sans`: `-apple-system, BlinkMacSystemFont, "Segoe UI Variable Text", "Segoe UI", system-ui, Roboto, sans-serif`
* `--font-mono`: `ui-monospace, "SF Mono", "JetBrains Mono", "Cascadia Code", Menlo, Consolas, monospace`

| Token | Size / line | Weight | Use |
| --- | --- | --- | --- |
| `--text-2xs` | 10 / 12 | 500 | Rail labels |
| `--text-xs` | 11 / 16 | 400–600 | Captions, overlines, column types, badges |
| `--text-sm` | 12 / 16 | 400 | Secondary text, grid cells, tree details |
| `--text-md` | 13 / 20 | 400–600 | **Default** UI text, controls, tree |
| `--text-lg` | 14 / 20 | 600 | Panel and card titles |
| `--text-xl` | 16 / 24 | 600 | Dialog titles |
| `--text-2xl` | 20 / 28 | 600 | Page titles |
| `--text-3xl` | 26 / 32 | 600 | Metric values on cards |

Three weights: 400 regular, 500 medium (labels, buttons), 600 semibold
(titles). Numbers in tables, grids and metrics use
`font-variant-numeric: tabular-nums`. Overlines (section labels in the
explorer) are `--text-xs`, 600, uppercase, `letter-spacing: .04em`,
`--text-subtle`.

### 3.4 Space, size, shape

* **Spacing** (4 px grid): `--sp-0_5` 2, `--sp-1` 4, `--sp-1_5` 6, `--sp-2` 8,
  `--sp-3` 12, `--sp-4` 16, `--sp-5` 20, `--sp-6` 24, `--sp-8` 32, `--sp-10` 40.
* **Control heights**: `--h-sm` 24 (toolbars, inline row actions), `--h-md`
  28 (default), `--h-lg` 32 (dialogs, login).
* **Rows**: grid 24, tree 26, table 32, list item 44 (two-line).
* **Radius**: `--r-xs` 3 (badges, kbd), `--r-sm` 5 (controls, tabs),
  `--r-md` 7 (cards, menus, callouts), `--r-lg` 10 (dialogs), `--r-full`.
* **Borders**: always 1 px. Active indicators are 2 px.
* **Elevation**:
  `--shadow-sm` (cards on canvas; light only),
  `--shadow-md` (menus, completion, popovers),
  `--shadow-lg` (dialogs, toasts). Dark mode leans on borders, not shadows.
* **Z-index**: sticky 10, splitter 20, dropdown 100, overlay 200, dialog 210,
  toast 300, tooltip 400.
* **Motion**: `--dur-fast` 80 ms (hover, press), `--dur-med` 140 ms
  (menus, tabs), `--dur-slow` 220 ms (panels, dialogs);
  `--ease: cubic-bezier(.2, 0, 0, 1)`. `prefers-reduced-motion` turns
  everything except the spinner off.
* **Focus**: `box-shadow: 0 0 0 2px var(--bg-surface), 0 0 0 4px var(--focus)`
  on `:focus-visible` only.

### 3.5 Code and data colors

Syntax (light / dark): keyword `#a61e4d`/`#f783ac` (600), string
`#2b8a3e`/`#8ce99a`, number `#1c6fd1`/`#74c0fc`, comment
`#868e96`/`#6e7681` (italic), function `#6741d9`/`#b197fc`, type
`#c2410c`/`#ffa94d`, quoted identifier `#0b7285`/`#66d9e8`, operator
`--text-muted`.

Chart categorical palette (8 hues, ordered for neighbor contrast; the same
in both themes, all ≥ 3:1 on both surfaces): `#4263eb` `#e64980`
`#0ca678` `#e67700` `#7950f2` `#1098ad` `#f76707` `#5c940d`. A single
series uses `--viz-1`.

### 3.6 Icons

One set, drawn for the console: 16 × 16 at a 24-unit viewBox, 1.75 stroke,
round caps and joins, `currentColor`, no fills. Sizes 14 (inline), 16
(default), 18 (rail). Icons always pair with a label or an `aria-label`
tooltip. They ship as inline SVG from one Rust module (`icon.rs`), no icon
font.

The set: `play` `stop` `plus` `x` `check` `chevron-right` `chevron-down`
`search` `database` `table` `view` `layers` (materialized view) `lake`
`index` `bolt` (trigger) `function` (macro) `column` `file-code`
(worksheet) `history` `clock` `users` `shield` (grant) `hard-drive`
`upload` `download` `copy` `sparkles` (Ask) `more` `grid` `chart`
`plan` `gauge` (profile) `sun` `moon` `log-out` `alert` `info`
`panel-left` `share` `key`.

## 4. Components

Classes follow `block`, `block__part`, `block--variant`, plus `is-*` for
state (`is-active`, `is-selected`, `is-running`, `is-dirty`). In the Leptos
app the structural ones become components in a `ui.rs` module
(`Button`, `IconButton`, `Icon`, `Badge`, `Field`, `Callout`,
`EmptyState`, `PageHeader`, `PanelHeader`, `Dialog`, `Tabs`,
`Segmented`, `Menu`).

### Actions

| Component | Spec |
| --- | --- |
| **Button** `.btn` | Heights sm 24 / md 28 / lg 32; padding 0 10 (md); 13 px 500; radius sm; icon 16 with a 6 px gap. Variants: **primary** (accent fill, at most one per view), **secondary** (surface, border; the default), **ghost** (no border, hover fill; toolbars), **danger** (danger text, danger fill on hover; destructive actions confirm). States: hover, active (1 px down), focus-visible ring, disabled (40 % opacity, no pointer), loading (spinner replaces icon, label stays). |
| **Icon button** `.btn--icon` | Square sm/md; ghost by default; always has an `aria-label` and tooltip. |
| **Split button** `.split` | Primary action plus a chevron that opens a menu. Used by Run. |
| **Segmented control** `.seg` | A 2 px inset track on `--bg-muted`; the active segment is a raised surface chip. For views of the same data (Grid / Chart / Plan / Profile; URL / Upload). |
| **Menu** `.menu` | Surface, `--shadow-md`, radius md, 4 px padding; items 28 px with icon, label and shortcut (`.kbd`); separators; danger items last. |

### Inputs

| Component | Spec |
| --- | --- |
| **Input / select / textarea** `.input` | 28 px, padding 0 8, radius sm, `--bg-surface` with `--border`; hover `--border-strong`; focus ring; invalid → danger border plus message. Placeholder `--text-subtle`. Selects get a drawn chevron. |
| **Search** `.search` | Input with a leading search icon and an optional `.kbd` hint (`⌘K`, `/`). |
| **Field** `.field` | Label (12 px 500 `--text-muted`) above the control, 4 px gap; help text or error below in 12 px. Fields in a row align on the control, not the label. |
| **Checkbox / switch** | 16 px box / 28×16 track; accent when on. |

### Navigation

| Component | Spec |
| --- | --- |
| **Rail item** `.rail__item` | 44 × 44 button: 18 px icon over a 10 px label. Active: `--accent-soft` fill and accent icon, 2 px accent bar on the left edge. |
| **Document tabs** `.doctabs` | Worksheet tabs: 32 px, max 220 px with ellipsis; icon, title, dirty dot (●) or running spinner, close on hover. The active tab joins the surface below it (no bottom border, 2 px accent top line). Middle-click closes; `+` at the end. |
| **Underline tabs** `.tabs` | Section tabs (statement results, object page): 13 px 500 `--text-muted`; active `--text` with a 2 px accent underline. |
| **Tree** `.tree` | 26 px rows, 16 px indent per level; chevron, kind icon, name (mono for objects), trailing detail in `--text-subtle`. Hover: `--bg-muted`; selected: `--accent-soft`. Row actions (insert, preview, history) are ghost icon buttons that appear on hover or focus. Group labels are overlines. |
| **Breadcrumb** `.crumbs` | `db › object`, mono for identifiers, `›` in `--text-subtle`. |

### Data display

| Component | Spec |
| --- | --- |
| **Data grid** `.grid` | Mono 12 px, 24 px rows, cells padded 0 10. Header: 2 lines on `--bg-subtle` (name 600 + type 11 px `--text-subtle`), sticky. Row-number gutter 52 px, `--text-subtle`, right-aligned, sticky left. Vertical rules `--border-subtle`, no zebra by default; hover row `--bg-muted`. Numbers right-aligned with tabular nums; NULL is `--text-subtle` italic; booleans and JSON keep the mono face. Selected cell: 2 px `--selection` outline plus fill. Column resize handle on hover. |
| **Table** `.table` | For admin lists: 13 px sans, 36 px rows, header 11 px uppercase overline on `--bg-subtle`, full-width rules, row actions right-aligned and shown on hover. Inside a `.card`. |
| **Key–value list** `.kv` | Two columns: label 12 px `--text-muted` (140 px), value 13 px; mono for paths and ids. |
| **Badge** `.badge` | 18 px high, 11 px 500, radius xs, padding 0 6. Tones: neutral, accent, success, warning, danger, info (soft fill + strong text). Kind badges in the tree use neutral. |
| **Status** `.status` | A 6 px dot plus label: Succeeded (success), Failed (danger), Cancelled (warning), Running (info, pulsing). |
| **Metric card** `.metric` | Card with an overline label, a 26 px tabular value and a 12 px caption. |
| **Kbd** `.kbd` | 18 px, mono 11 px, `--bg-muted`, 1 px border, bottom border 2 px. |
| **Code block** `.code` | Mono 12 px on `--bg-subtle`, radius sm, padding 8 12, scrolls horizontally. |
| **Chart** | SVG; gridlines `--border-subtle`, axis labels 11 px `--text-subtle`, marks from the categorical palette, value tooltips on hover. |

### Feedback

| Component | Spec |
| --- | --- |
| **Callout** `.callout` | Soft tone fill, 1 px tone border at 30 %, radius md, padding 10 12, a 16 px tone icon, title 600 plus body. Tones: info, success, warning, danger. Statement errors are a danger callout with the failed statement in a code block. |
| **Empty state** `.empty` | Centered: 32 px icon in `--text-subtle`, 14 px 600 title, 13 px `--text-muted` body (max 44 ch), optional secondary action. Every list and result pane has one. |
| **Spinner** `.spinner` | 14 px ring, 2 px, accent arc, 0.7 s. |
| **Progress bar** `.bar` | 4 px track on `--bg-muted`, fill `--accent` (profile phases use `--viz-1`). |
| **Toast** `.toast` | Bottom-right, `--shadow-lg`, 360 px max, icon + text + optional action, auto-dismiss after 5 s (errors stay). For results of actions (grant, import, checkpoint) that are not shown in place. |
| **Dialog** `.dialog` | Centered, 560 px (sm 420, lg 760), radius lg, `--shadow-lg`. Header (16 px title, close), body (16 px padding), footer (actions right, primary last, Cancel secondary). Focus trapped; `Esc` and backdrop close it unless work is in flight. Destructive confirms use a danger primary and name the object. |
| **Skeleton** `.skeleton` | `--bg-muted` blocks with a shimmer, for the catalog and tables while they load. |

## 5. Patterns

* **Loading**: lists show skeleton rows; the results pane shows the running
  state (spinner, elapsed time ticking, Cancel, `Esc`); buttons show their
  own spinner. Never a bare "Loading…" line.
* **Errors**: in place, as a danger callout, with the statement and the
  engine's message in mono. Network errors also go to the status bar.
* **Destructive actions** (drop user, revoke, delete worksheet, replace
  table on import) ask in a confirm dialog that shows the exact SQL that
  will run.
* **Read-only consoles** hide write actions entirely rather than disabling
  them, and show a `Read-only` warning badge in the status bar.
* **SQL transparency**: forms that build SQL (grants, imports) show the
  statement they will run in a code block before running it.

## 6. Voice and formatting

* **Sentence case** for every label, title and button: "New worksheet",
  "Run all", "Import data". Never lowercase buttons ("delete", "use") and
  never Title Case.
* Buttons are verbs: Run, Import, Grant, Revoke, Drop user, Checkpoint.
* Numbers: thousands separators (`1,204,551`), tabular nums. Durations:
  `< 1 ms`, `38 ms`, `1.42 s`, `2 min 03 s`. Timestamps: relative in lists
  ("4 min ago") with the absolute time in the tooltip; absolute
  `2026-10-30 14:02:11` in tables.
* Identifiers (databases, tables, columns, users) render in the mono face.
* `NULL` is the literal word, subtle and italic, never an empty cell.

## 7. Accessibility

* Text contrast ≥ 4.5:1 on every surface it sits on, checked per token:
  `--text-muted` 7.4:1 light / 7.0:1 dark, `--text-subtle` ≥ 4.6:1 light /
  ≥ 4.8:1 dark, white on `--accent` 4.6:1 in both themes, status text on
  its tint ≥ 5:1. The primary button keeps accent-700 in dark mode for
  this reason. Chart hues are ≥ 3:1 on both surfaces. Borders are
  decorative (about 1.6:1); a boundary that carries meaning (focus, invalid,
  selection) uses a ≥ 3:1 color.
* Every interactive element is reachable by keyboard and shows the focus
  ring. The grid is one tab stop, moved with arrow keys.
* Icon-only buttons have `aria-label`. Status is never color alone (dot +
  label). The rail is `nav` with `aria-current`.
* Dialogs trap focus and restore it on close.

## 8. Implementation notes

* **No style attributes.** The console's Content-Security-Policy is
  `style-src 'self'`, so markup may not carry `style="…"`: a browser drops
  it. Fixed sizes are classes (`.col-120`, `.depth-1`, `.menu--run`);
  sizes that change at run time (column widths, the editor height, the
  explorer width) are set through the CSSOM (`style:width=` in Leptos),
  which the policy allows.
* **No URLs in the stylesheet.** The server's asset test rejects any
  `http://` or `https://` in served assets, so the select chevron is drawn
  with gradients rather than an inline SVG.
* **Icons** live in `crates/pinkdb-server-ui-web/src/icon.rs`, generated
  from `icons.svg` (every shape converted to path data).
* **Size.** The wasm is built with `--cfg erase_components`
  (`scripts/build-console.sh`), which type-erases component views: 339 KB
  gzipped instead of 449 KB, against 303 KB before this wave.
