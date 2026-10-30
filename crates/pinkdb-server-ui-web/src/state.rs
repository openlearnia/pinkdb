//! The app's state: the session, the catalog, the open worksheets and what
//! they ran. Every field is a signal (all `Copy`), so components take the
//! whole [`App`] from context and read what they need.

use std::time::Duration;

use leptos::prelude::*;
use leptos::task::spawn_local;
use pinkdb_server_ui_api::{
    CancelRequest, Catalog, HistoryEntry, HistoryList, QueryRequest, QueryResponse, SaveWorksheet,
    SessionInfo, Worksheet, WorksheetList,
};

use crate::api;

/// Which result view a worksheet shows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResultView {
    Grid,
    Chart,
    Profile,
}

/// The left panel's tab.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Catalog,
    Worksheets,
    History,
    Access,
    Storage,
}

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
    pub side: RwSignal<Side>,
    pub ask_open: RwSignal<bool>,
    /// A one-line notice in the status bar.
    pub notice: RwSignal<String>,
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
            side: RwSignal::new(Side::Catalog),
            ask_open: RwSignal::new(false),
            notice: RwSignal::new(String::new()),
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
                    let latest = list.worksheets.iter().find(|w| w.mine).cloned();
                    app.saved.set(list.worksheets);
                    if open_latest {
                        match latest {
                            Some(sheet) => app.open_saved(&sheet),
                            None => {
                                app.open("Untitled", STARTER);
                            }
                        }
                    }
                }
                Err(e) => {
                    app.notice.set(format!("worksheets: {e}"));
                    if open_latest {
                        app.open("Untitled", STARTER);
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
            if app.side.get_untracked() == Side::History {
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
