//! The console's URLs (wave 121c): the section, the open worksheet and the
//! catalog object live in the path, so Back and Forward work, a reload
//! keeps the place, and a link opens the same view.
//!
//! | Path | View |
//! |---|---|
//! | `/`, `/query` | the worksheets (the latest one) |
//! | `/query/{worksheet id}` | that saved worksheet |
//! | `/catalog` | the current database's objects |
//! | `/catalog/{database}/{object}` | one object's page |
//! | `/history`, `/access`, `/storage`, `/account` | those pages |
//!
//! The server serves the same shell on each of these paths.

use leptos::ev;
use leptos::prelude::*;
use wasm_bindgen::JsValue;

use crate::state::{App, Section, encode};

/// Where the console is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Route {
    /// The worksheets; the saved worksheet's id when it has one.
    Query(Option<String>),
    /// The catalog; an object `(database, name)` when one is open.
    Catalog(Option<(String, String)>),
    History,
    Access,
    Storage,
    Account,
}

/// Decode a percent-encoded path segment (a stray `%` stays as it is).
pub fn decode(segment: &str) -> String {
    fn hex(b: u8) -> Option<u8> {
        (b as char).to_digit(16).map(|d| d as u8)
    }
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let pair = (bytes[i] == b'%')
            .then(|| Some((hex(*bytes.get(i + 1)?)?, hex(*bytes.get(i + 2)?)?)))
            .flatten();
        match pair {
            Some((hi, lo)) => {
                out.push(hi * 16 + lo);
                i += 3;
            }
            None => {
                out.push(bytes[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The route of a path (an unknown path is the worksheets).
pub fn parse(path: &str) -> Route {
    let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
    match parts.as_slice() {
        ["query", id] => Route::Query(Some(decode(id))),
        ["catalog"] => Route::Catalog(None),
        ["catalog", db, name] => Route::Catalog(Some((decode(db), decode(name)))),
        ["history"] => Route::History,
        ["access"] => Route::Access,
        ["storage"] => Route::Storage,
        ["account"] => Route::Account,
        _ => Route::Query(None),
    }
}

/// The path of a route.
pub fn path(route: &Route) -> String {
    match route {
        Route::Query(None) => "/query".to_string(),
        Route::Query(Some(id)) => format!("/query/{}", encode(id)),
        Route::Catalog(None) => "/catalog".to_string(),
        Route::Catalog(Some((db, name))) => format!("/catalog/{}/{}", encode(db), encode(name)),
        Route::History => "/history".to_string(),
        Route::Access => "/access".to_string(),
        Route::Storage => "/storage".to_string(),
        Route::Account => "/account".to_string(),
    }
}

/// The path of a section (for links).
pub fn section_path(section: Section) -> &'static str {
    match section {
        Section::Query => "/query",
        Section::Catalog => "/catalog",
        Section::History => "/history",
        Section::Access => "/access",
        Section::Storage => "/storage",
        Section::Account => "/account",
    }
}

/// Whether going from `from` to `to` replaces the history entry rather than
/// adding one: a worksheet getting its id on its first save, or a switch to
/// an unsaved tab, is not a navigation of its own.
pub fn replaces(from: &Route, to: &Route) -> bool {
    matches!(
        (from, to),
        (Route::Query(None), Route::Query(_)) | (Route::Query(_), Route::Query(None))
    )
}

/// Show a route: switch the section and open what it names. A worksheet
/// that is not in the saved list yet waits for it (`App::pending_sheet`).
pub fn apply(app: App, route: Route) {
    match route {
        Route::Query(id) => {
            app.go(Section::Query);
            if let Some(id) = id {
                let saved = app
                    .saved
                    .with_untracked(|s| s.iter().find(|w| w.id == id).cloned());
                match saved {
                    Some(w) => app.open_saved(&w),
                    None => app.pending_sheet.set(Some(id)),
                }
            }
        }
        Route::Catalog(selected) => {
            app.selected.set(selected);
            app.go(Section::Catalog);
        }
        Route::History => app.go(Section::History),
        Route::Access => app.go(Section::Access),
        Route::Storage => app.go(Section::Storage),
        Route::Account => app.go(Section::Account),
    }
}

/// The route the app shows now.
fn current(app: App) -> Route {
    match app.section.get() {
        Section::Query => Route::Query(app.current().and_then(|s| s.id.get())),
        Section::Catalog => Route::Catalog(app.selected.get()),
        Section::History => Route::History,
        Section::Access => Route::Access,
        Section::Storage => Route::Storage,
        Section::Account => Route::Account,
    }
}

/// The page title of the app's view.
fn title(app: App) -> String {
    let place = match app.section.get() {
        Section::Query => app
            .current()
            .map(|s| s.title.get())
            .filter(|t| !t.is_empty())
            .unwrap_or_else(|| "Untitled".to_string()),
        Section::Catalog => match app.selected.get() {
            Some((db, name)) => format!("{db}.{name}"),
            None => "Catalog".to_string(),
        },
        Section::History => "Query history".to_string(),
        Section::Access => "Access".to_string(),
        Section::Storage => "Storage".to_string(),
        Section::Account => "Account".to_string(),
    };
    format!("{place} · pinkdb")
}

/// The location's path.
pub fn location_path() -> String {
    window().location().pathname().unwrap_or_default()
}

/// Keep the URL and the title in step with the app, and follow Back and
/// Forward. Runs once the first worksheets have opened (`route_ready`), so
/// a deep link is not overwritten before it is honored.
pub fn install(app: App) {
    Effect::new(move |_| {
        if !app.route_ready.get() {
            return;
        }
        let to = current(app);
        document().set_title(&title(app));
        let target = path(&to);
        let here = location_path();
        if target == here {
            return;
        }
        let Ok(history) = window().history() else {
            return;
        };
        let _ = if replaces(&parse(&here), &to) {
            history.replace_state_with_url(&JsValue::NULL, "", Some(&target))
        } else {
            history.push_state_with_url(&JsValue::NULL, "", Some(&target))
        };
    });
    let back = window_event_listener(ev::popstate, move |_| {
        apply(app, parse(&location_path()));
    });
    on_cleanup(move || back.remove());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_round_trip() {
        for route in [
            Route::Query(None),
            Route::Query(Some("w 1".to_string())),
            Route::Catalog(None),
            Route::Catalog(Some(("my db".to_string(), "a/b".to_string()))),
            Route::History,
            Route::Access,
            Route::Storage,
            Route::Account,
        ] {
            assert_eq!(parse(&path(&route)), route, "{route:?}");
        }
        assert_eq!(
            path(&Route::Catalog(Some(("my db".into(), "a/b".into())))),
            "/catalog/my%20db/a%2Fb"
        );
        assert_eq!(parse("/"), Route::Query(None));
        assert_eq!(parse("/nope/x"), Route::Query(None));
        assert_eq!(parse("/catalog/main"), Route::Query(None));
        assert_eq!(decode("%E2%82%AC%2"), "€%2");
        assert_eq!(decode("%é"), "%é");
    }

    #[test]
    fn saving_a_worksheet_replaces_the_entry() {
        assert!(replaces(
            &Route::Query(None),
            &Route::Query(Some("w".into()))
        ));
        assert!(!replaces(
            &Route::Query(Some("a".into())),
            &Route::Query(Some("b".into()))
        ));
        assert!(!replaces(&Route::Query(None), &Route::History));
    }
}
