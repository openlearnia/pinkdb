//! The app's state: the session, the catalog, the open worksheets and what
//! they ran. Every field is a signal (all `Copy`), so components take the
//! whole [`App`] from context and read what they need.

use std::time::Duration;

use leptos::prelude::*;
use leptos::task::spawn_local;
use pinkdb_server_ui_api::{
    CancelRequest, Catalog, CatalogObject, HistoryEntry, HistoryList, QueryRequest, QueryResponse,
    SaveWorksheet, SessionInfo, Storage, Worksheet, WorksheetList,
};

use crate::api;

/// Which result view a worksheet shows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResultView {
    Grid,
    Chart,
    Profile,
}

/// The section the rail picks (docs/console-design/README.md §2.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Section {
    Query,
    Catalog,
    History,
    Access,
    Storage,
}

impl Section {
    /// Whether the section has an explorer beside its workspace.
    pub fn has_explorer(self) -> bool {
        matches!(self, Section::Query | Section::Catalog)
    }
}

/// What the Query section's explorer lists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExplorerTab {
    Objects,
    Worksheets,
}

/// A run the toolbar asks the editor for (the editor knows the cursor).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunKind {
    /// The selection, or the statement at the cursor.
    Cursor,
    /// Every statement.
    All,
    /// `EXPLAIN` of the statement at the cursor.
    Explain,
}

/// The tone of a callout or a toast.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tone {
    Info,
    Success,
    Warning,
    Danger,
}

impl Tone {
    pub fn name(self) -> &'static str {
        match self {
            Tone::Info => "info",
            Tone::Success => "success",
            Tone::Warning => "warning",
            Tone::Danger => "danger",
        }
    }

    pub fn icon(self) -> &'static str {
        match self {
            Tone::Info => "info",
            Tone::Success => "check-circle",
            Tone::Warning => "alert",
            Tone::Danger => "alert-circle",
        }
    }
}

/// A transient message in the corner.
#[derive(Clone, Debug, PartialEq)]
pub struct Toast {
    pub id: u64,
    pub tone: Tone,
    pub title: String,
    pub body: String,
}

/// A destructive action waiting for a yes: the dialog shows the exact SQL.
#[derive(Clone)]
pub struct Confirm {
    pub title: String,
    pub body: String,
    pub sql: String,
    /// The button's label ("Drop user", "Revoke").
    pub action: String,
    pub on_confirm: Callback<()>,
}

/// The editor's height when a worksheet opens (the splitter changes it).
pub const EDITOR_PX: f64 = 260.0;

/// One open worksheet tab.
#[derive(Clone, Copy)]
pub struct Sheet {
    /// A key for the tab list (local; the server id may not exist yet).
    pub key: u64,
    /// The server id, once saved.
    pub id: RwSignal<Option<String>>,
    pub title: RwSignal<String>,
    pub sql: RwSignal<String>,
    pub shared_with: RwSignal<String>,
    /// The caller owns it (a shared worksheet of someone else is read-only).
    pub mine: RwSignal<bool>,
    /// Edited since the last save.
    pub dirty: RwSignal<bool>,
    /// Bumped on every edit; the autosave timer checks it.
    pub generation: RwSignal<u64>,
    /// The last run.
    pub result: RwSignal<Option<QueryResponse>>,
    /// The run in flight.
    pub running: RwSignal<Option<String>>,
    /// The client error of the last run (network, 4xx).
    pub run_error: RwSignal<Option<String>>,
    /// Which statement result is shown.
    pub tab: RwSignal<usize>,
    pub view: RwSignal<ResultView>,
    /// The selected cell `(row, column)`.
    pub cell: RwSignal<Option<(usize, usize)>>,
    /// When the run in flight started (`Date.now()`).
    pub started: RwSignal<f64>,
    /// The editor's height in pixels.
    pub editor_px: RwSignal<f64>,
}

impl Sheet {
    fn new(key: u64) -> Self {
        Sheet {
            key,
            id: RwSignal::new(None),
            title: RwSignal::new(String::new()),
            sql: RwSignal::new(String::new()),
            shared_with: RwSignal::new(String::new()),
            mine: RwSignal::new(true),
            dirty: RwSignal::new(false),
            generation: RwSignal::new(0),
            result: RwSignal::new(None),
            running: RwSignal::new(None),
            run_error: RwSignal::new(None),
            tab: RwSignal::new(0),
            view: RwSignal::new(ResultView::Grid),
            cell: RwSignal::new(None),
            started: RwSignal::new(0.0),
            editor_px: RwSignal::new(EDITOR_PX),
        }
    }
}

/// The whole app.
#[derive(Clone, Copy)]
pub struct App {
    pub session: RwSignal<Option<SessionInfo>>,
    /// `none`, `basic` or `token`, when a login is needed.
    pub login_mode: RwSignal<Option<String>>,
    pub catalog: RwSignal<Catalog>,
    /// The database statements run in.
    pub db: RwSignal<String>,
    pub sheets: RwSignal<Vec<Sheet>>,
    pub active: RwSignal<u64>,
    pub saved: RwSignal<Vec<Worksheet>>,
    pub history: RwSignal<Vec<HistoryEntry>>,
    pub section: RwSignal<Section>,
    pub explorer_tab: RwSignal<ExplorerTab>,
    pub explorer_open: RwSignal<bool>,
    pub explorer_width: RwSignal<f64>,
    /// The catalog object the Catalog section shows `(database, name)`.
    pub selected: RwSignal<Option<(String, String)>>,
    pub ask_open: RwSignal<bool>,
    /// A one-line notice in the status bar (load and network errors).
    pub notice: RwSignal<String>,
    /// A run the toolbar asked for; the editor takes it.
    pub run_request: RwSignal<Option<RunKind>>,
    /// The editor's caret `(line, column)`, 1-based.
    pub cursor: RwSignal<(usize, usize)>,
    pub toasts: RwSignal<Vec<Toast>>,
    next_toast: RwSignal<u64>,
    pub confirm: RwSignal<Option<Confirm>>,
    /// The storage facts (the status bar and the Storage page).
    pub storage: RwSignal<Option<Storage>>,
    /// `light` or `dark` when the user picked one; empty follows the OS.
    pub theme: RwSignal<String>,
    /// A worksheet the URL names, opened once the saved list arrives.
    pub pending_sheet: RwSignal<Option<String>>,
    /// The first worksheets are open: the URL follows the app from now on.
    pub route_ready: RwSignal<bool>,
    next_key: RwSignal<u64>,
    /// Text the editor should insert at its cursor (the catalog and the ask
    /// box set it; the editor takes it).
    pub insert: RwSignal<Option<String>>,
    /// The Import dialog is open.
    pub import_open: RwSignal<bool>,
    /// Bumped when the access catalog changed (the Access panel reloads).
    pub access_epoch: RwSignal<u64>,
}

impl App {
    pub fn new() -> Self {
        App {
            session: RwSignal::new(None),
            login_mode: RwSignal::new(None),
            catalog: RwSignal::new(Catalog::default()),
            db: RwSignal::new(String::new()),
            sheets: RwSignal::new(Vec::new()),
            active: RwSignal::new(0),
            saved: RwSignal::new(Vec::new()),
            history: RwSignal::new(Vec::new()),
            section: RwSignal::new(Section::Query),
            explorer_tab: RwSignal::new(ExplorerTab::Objects),
            explorer_open: RwSignal::new(true),
            explorer_width: RwSignal::new(272.0),
            selected: RwSignal::new(None),
            ask_open: RwSignal::new(false),
            notice: RwSignal::new(String::new()),
            run_request: RwSignal::new(None),
            cursor: RwSignal::new((1, 1)),
            toasts: RwSignal::new(Vec::new()),
            next_toast: RwSignal::new(1),
            confirm: RwSignal::new(None),
            storage: RwSignal::new(None),
            theme: RwSignal::new(stored_theme()),
            pending_sheet: RwSignal::new(None),
            route_ready: RwSignal::new(false),
            next_key: RwSignal::new(1),
            insert: RwSignal::new(None),
            import_open: RwSignal::new(false),
            access_epoch: RwSignal::new(0),
        }
    }

    pub fn csrf(&self) -> String {
        self.session
            .with_untracked(|s| s.as_ref().map(|s| s.csrf.clone()).unwrap_or_default())
    }

    pub fn admin(&self) -> bool {
        self.session.with(|s| s.as_ref().is_some_and(|s| s.admin))
    }

    /// Switch section, loading what it shows.
    pub fn go(&self, section: Section) {
        self.section.set(section);
        match section {
            Section::History => {
                self.load_history();
                self.load_worksheets(false);
            }
            Section::Catalog => self.load_catalog(),
            Section::Storage => self.load_storage(),
            Section::Query | Section::Access => {}
        }
    }

    /// Show a catalog object's page.
    pub fn show_object(&self, object: &CatalogObject) {
        self.selected
            .set(Some((object.database.clone(), object.name.clone())));
        self.go(Section::Catalog);
    }

    /// Show a toast; it goes away after a few seconds unless it is an error.
    pub fn toast(&self, tone: Tone, title: impl Into<String>, body: impl Into<String>) {
        let id = self.next_toast.get_untracked();
        self.next_toast.set(id + 1);
        self.toasts.update(|t| {
            t.push(Toast {
                id,
                tone,
                title: title.into(),
                body: body.into(),
            })
        });
        if tone != Tone::Danger {
            let toasts = self.toasts;
            set_timeout(
                move || toasts.update(|t| t.retain(|x| x.id != id)),
                Duration::from_secs(5),
            );
        }
    }

    pub fn dismiss_toast(&self, id: u64) {
        self.toasts.update(|t| t.retain(|x| x.id != id));
    }

    /// Ask before a destructive action.
    pub fn ask_first(
        &self,
        title: impl Into<String>,
        body: impl Into<String>,
        sql: impl Into<String>,
        action: impl Into<String>,
        on_confirm: impl Fn(()) + Send + Sync + 'static,
    ) {
        self.confirm.set(Some(Confirm {
            title: title.into(),
            body: body.into(),
            sql: sql.into(),
            action: action.into(),
            on_confirm: Callback::new(on_confirm),
        }));
    }

    /// Pick a theme (`light`, `dark`) and remember it in this browser.
    pub fn set_theme(&self, theme: &str) {
        self.theme.set(theme.to_string());
        apply_theme(theme);
        if let Some(storage) = leptos::prelude::window().local_storage().ok().flatten() {
            let _ = storage.set_item(THEME_KEY, theme);
        }
    }

    /// Whether the page shows the dark theme now.
    pub fn is_dark(&self) -> bool {
        match self.theme.get().as_str() {
            "dark" => true,
            "light" => false,
            _ => leptos::prelude::window()
                .match_media("(prefers-color-scheme: dark)")
                .ok()
                .flatten()
                .is_some_and(|m| m.matches()),
        }
    }

    pub fn load_storage(&self) {
        let app = *self;
        spawn_local(async move {
            match api::get::<Storage>("/api/storage").await {
                Ok(storage) => app.storage.set(Some(storage)),
                Err(e) => app.notice.set(format!("storage: {e}")),
            }
        });
    }

    /// The catalog object `(database, name)`, if it exists.
    pub fn object(&self, database: &str, name: &str) -> Option<CatalogObject> {
        self.catalog.with(|c| {
            c.objects
                .iter()
                .find(|o| o.database.eq_ignore_ascii_case(database) && o.name == name)
                .cloned()
        })
    }

    /// Run one statement outside any worksheet (an Access or Import action),
    /// as the session user; `done` gets the outcome line or the error.
    pub fn run_statement(&self, sql: String, done: impl FnOnce(Result<String, String>) + 'static) {
        let request = QueryRequest {
            sql,
            db: Some(self.db.get_untracked()).filter(|d| !d.is_empty()),
            run_id: api::run_id(),
            worksheet_id: String::new(),
        };
        let csrf = self.csrf();
        spawn_local(async move {
            let outcome = match api::post::<_, QueryResponse>("/api/query", &request, &csrf).await {
                Ok(response) => match response.error {
                    Some(error) => Err(error.message),
                    None => Ok(response
                        .results
                        .iter()
                        .map(|r| r.message.clone())
                        .collect::<Vec<_>>()
                        .join("; ")),
                },
                Err(e) => Err(e.message),
            };
            done(outcome);
        });
    }

    pub fn read_only(&self) -> bool {
        self.session
            .with(|s| s.as_ref().is_some_and(|s| s.read_only))
    }

    /// The active worksheet.
    pub fn current(&self) -> Option<Sheet> {
        let active = self.active.get();
        self.sheets
            .with(|sheets| sheets.iter().find(|s| s.key == active).copied())
    }

    /// Open a new tab and make it active.
    pub fn open(&self, title: &str, sql: &str) -> Sheet {
        let key = self.next_key.get_untracked();
        self.next_key.set(key + 1);
        let sheet = Sheet::new(key);
        sheet.title.set(title.to_string());
        sheet.sql.set(sql.to_string());
        self.sheets.update(|s| s.push(sheet));
        self.active.set(key);
        sheet
    }

    /// Open a saved worksheet (or switch to its tab).
    pub fn open_saved(&self, saved: &Worksheet) {
        let existing = self.sheets.with_untracked(|sheets| {
            sheets
                .iter()
                .find(|s| s.id.get_untracked().as_deref() == Some(saved.id.as_str()))
                .copied()
        });
        if let Some(sheet) = existing {
            self.active.set(sheet.key);
            return;
        }
        let sheet = self.open(&saved.title, &saved.sql);
        sheet.id.set(Some(saved.id.clone()));
        sheet.shared_with.set(saved.shared_with.clone());
        sheet.mine.set(saved.mine);
    }

    /// Close a tab (the worksheet stays saved).
    pub fn close(&self, key: u64) {
        let sheet = self
            .sheets
            .with_untracked(|s| s.iter().find(|s| s.key == key).copied());
        if let Some(sheet) = sheet
            && sheet.dirty.get_untracked()
        {
            self.save(sheet);
        }
        self.sheets.update(|s| s.retain(|s| s.key != key));
        if self.active.get_untracked() == key {
            let last = self.sheets.with_untracked(|s| s.last().map(|s| s.key));
            match last {
                Some(last) => self.active.set(last),
                None => {
                    self.open("Untitled", "");
                }
            }
        }
    }

    /// Note an edit and save after a pause.
    pub fn edited(&self, sheet: Sheet) {
        if !sheet.mine.get_untracked() {
            return;
        }
        sheet.dirty.set(true);
        let generation = sheet.generation.get_untracked() + 1;
        sheet.generation.set(generation);
        let app = *self;
        set_timeout(
            move || {
                if sheet.generation.try_get_untracked() == Some(generation) {
                    app.save(sheet);
                }
            },
            Duration::from_millis(1200),
        );
    }

    /// Save a worksheet now (create it on the first save).
    pub fn save(&self, sheet: Sheet) {
        if !sheet.mine.get_untracked() {
            return;
        }
        let request = SaveWorksheet {
            id: sheet.id.get_untracked(),
            title: sheet.title.get_untracked(),
            sql: sheet.sql.get_untracked(),
            shared_with: sheet.shared_with.get_untracked(),
        };
        let csrf = self.csrf();
        let app = *self;
        spawn_local(async move {
            match api::post::<_, Worksheet>("/api/worksheets", &request, &csrf).await {
                Ok(saved) => {
                    sheet.id.set(Some(saved.id.clone()));
                    sheet.dirty.set(false);
                    app.saved.update(|list| {
                        list.retain(|w| w.id != saved.id);
                        list.insert(0, saved);
                    });
                }
                Err(e) => app.notice.set(format!("not saved: {e}")),
            }
        });
    }

    /// Reload the catalog.
    pub fn load_catalog(&self) {
        let app = *self;
        let db = self.db.get_untracked();
        spawn_local(async move {
            let path = if db.is_empty() {
                "/api/catalog".to_string()
            } else {
                format!("/api/catalog?db={}", encode(&db))
            };
            match api::get::<Catalog>(&path).await {
                Ok(catalog) => {
                    if app.db.get_untracked().is_empty() {
                        app.db.set(catalog.current.clone());
                    }
                    app.catalog.set(catalog);
                }
                Err(e) => app.notice.set(format!("catalog: {e}")),
            }
        });
    }

    pub fn load_worksheets(&self, open_latest: bool) {
        let app = *self;
        spawn_local(async move {
            match api::get::<WorksheetList>("/api/worksheets").await {
                Ok(list) => {
                    // The worksheet the URL names, else the latest of mine.
                    let wanted = app
                        .pending_sheet
                        .get_untracked()
                        .and_then(|id| list.worksheets.iter().find(|w| w.id == id).cloned());
                    let latest = list.worksheets.iter().find(|w| w.mine).cloned();
                    app.saved.set(list.worksheets);
                    if open_latest || wanted.is_some() {
                        app.pending_sheet.set(None);
                        match wanted.or(latest) {
                            Some(sheet) => app.open_saved(&sheet),
                            None if app.sheets.with_untracked(Vec::is_empty) => {
                                app.open("Untitled", STARTER);
                            }
                            None => {}
                        }
                        app.route_ready.set(true);
                    }
                }
                Err(e) => {
                    app.notice.set(format!("worksheets: {e}"));
                    if open_latest {
                        app.open("Untitled", STARTER);
                        app.route_ready.set(true);
                    }
                }
            }
        });
    }

    pub fn load_history(&self) {
        let app = *self;
        spawn_local(async move {
            match api::get::<HistoryList>("/api/history").await {
                Ok(list) => app.history.set(list.history),
                Err(e) => app.notice.set(format!("history: {e}")),
            }
        });
    }

    /// Run `sql` in `sheet`.
    pub fn run(&self, sheet: Sheet, sql: String) {
        if sql.trim().is_empty() || sheet.running.get_untracked().is_some() {
            return;
        }
        let run_id = api::run_id();
        sheet.started.set(js_sys::Date::now());
        sheet.running.set(Some(run_id.clone()));
        sheet.run_error.set(None);
        let request = QueryRequest {
            sql,
            db: Some(self.db.get_untracked()).filter(|d| !d.is_empty()),
            run_id,
            worksheet_id: sheet.id.get_untracked().unwrap_or_default(),
        };
        let csrf = self.csrf();
        let app = *self;
        spawn_local(async move {
            let answer = api::post::<_, QueryResponse>("/api/query", &request, &csrf).await;
            sheet.running.set(None);
            match answer {
                Ok(response) => {
                    let changes_catalog = response.results.iter().any(|r| r.kind == "ok");
                    if !response.db.is_empty() && response.db != app.db.get_untracked() {
                        app.db.set(response.db.clone());
                    }
                    let shown = if response.error.is_some() {
                        response.results.len()
                    } else {
                        response.results.len().saturating_sub(1)
                    };
                    sheet.tab.set(shown);
                    sheet.cell.set(None);
                    sheet.result.set(Some(response));
                    if changes_catalog {
                        app.load_catalog();
                    }
                }
                Err(e) => {
                    if e.status == 401 {
                        app.session.set(None);
                    }
                    sheet.run_error.set(Some(e.message));
                }
            }
            if app.section.get_untracked() == Section::History {
                app.load_history();
            }
        });
    }

    /// Cancel the sheet's run.
    pub fn cancel(&self, sheet: Sheet) {
        let Some(run_id) = sheet.running.get_untracked() else {
            return;
        };
        let csrf = self.csrf();
        spawn_local(async move {
            let _ =
                api::post::<_, serde_json::Value>("/api/cancel", &CancelRequest { run_id }, &csrf)
                    .await;
        });
    }
}

/// Where the theme choice is kept (`boot.js` reads it before the app loads,
/// so the page never flashes the other theme).
const THEME_KEY: &str = "pinkdb-console-theme";

/// The theme this browser picked last.
fn stored_theme() -> String {
    leptos::prelude::window()
        .local_storage()
        .ok()
        .flatten()
        .and_then(|s| s.get_item(THEME_KEY).ok().flatten())
        .filter(|t| t == "light" || t == "dark")
        .unwrap_or_default()
}

/// Set `data-theme` on the page.
pub fn apply_theme(theme: &str) {
    if let Some(root) = leptos::prelude::document().document_element() {
        let _ = if theme.is_empty() {
            root.remove_attribute("data-theme")
        } else {
            root.set_attribute("data-theme", theme)
        };
    }
}

/// Whether this browser runs on a Mac (shortcut labels say ⌘, not Ctrl).
pub fn on_mac() -> bool {
    leptos::prelude::window()
        .navigator()
        .user_agent()
        .is_ok_and(|ua| ua.contains("Mac"))
}

/// A shortcut label: `⌘↵` on a Mac, `Ctrl+↵` elsewhere.
pub fn shortcut(keys: &str) -> String {
    if on_mac() {
        format!("⌘{keys}")
    } else {
        format!("Ctrl+{keys}")
    }
}

/// A row count in a few characters: `25`, `150K`, `6.0M`.
pub fn compact(n: i64) -> String {
    let n = n.max(0) as f64;
    if n >= 1e9 {
        format!("{:.1}B", n / 1e9)
    } else if n >= 1e6 {
        format!("{:.1}M", n / 1e6)
    } else if n >= 1e4 {
        format!("{:.0}K", n / 1e3)
    } else if n >= 1e3 {
        format!("{:.1}K", n / 1e3)
    } else {
        format!("{n:.0}")
    }
}

/// The icon for a catalog object kind.
pub fn kind_icon(kind: &str) -> &'static str {
    match kind {
        "TABLE" => "table",
        "VIEW" => "view",
        "MATERIALIZED VIEW" => "layers",
        "LAKE TABLE" => "lake",
        "INDEX" => "index",
        "TRIGGER" => "bolt",
        "MACRO" => "function",
        _ => "table",
    }
}

/// A catalog kind in sentence case (`MATERIALIZED VIEW` → `Materialized view`).
pub fn kind_label(kind: &str) -> String {
    let lower = kind.to_ascii_lowercase();
    let mut chars = lower.chars();
    match chars.next() {
        Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

/// Whether a catalog kind holds rows.
pub fn is_relation(kind: &str) -> bool {
    matches!(kind, "TABLE" | "VIEW" | "MATERIALIZED VIEW" | "LAKE TABLE")
}

/// What a new worksheet starts with.
pub const STARTER: &str = "-- Ctrl/Cmd+Enter runs the statement at the cursor (or the selection);\n\
-- Shift+Ctrl/Cmd+Enter runs everything. Ctrl+Space completes names.\n\
SHOW TABLES;\n";

/// Percent-encode a query parameter.
pub fn encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

/// Is a column type numeric (right-aligned, chartable)?
pub fn numeric(dtype: &str) -> bool {
    let t = dtype.to_ascii_uppercase();
    ["INT", "DOUBLE", "DECIMAL", "FLOAT", "REAL", "NUMERIC"]
        .iter()
        .any(|k| t.contains(k))
}

/// A count with thousands separators.
pub fn count(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// Milliseconds, readably.
pub fn millis(ms: f64) -> String {
    if ms >= 1000.0 {
        format!("{:.2} s", ms / 1000.0)
    } else if ms >= 10.0 {
        format!("{ms:.0} ms")
    } else {
        format!("{ms:.1} ms")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_and_kinds_read_well() {
        assert_eq!(compact(25), "25");
        assert_eq!(compact(1_204), "1.2K");
        assert_eq!(compact(150_000), "150K");
        assert_eq!(compact(6_001_215), "6.0M");
        assert_eq!(compact(-3), "0");
        assert_eq!(kind_label("MATERIALIZED VIEW"), "Materialized view");
        assert_eq!(kind_icon("LAKE TABLE"), "lake");
        assert!(is_relation("VIEW") && !is_relation("INDEX"));
        assert_eq!(count(1_500_000), "1,500,000");
    }
}
