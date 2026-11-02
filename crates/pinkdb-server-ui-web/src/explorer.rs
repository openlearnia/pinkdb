//! The explorer beside the Query and Catalog workspaces: the catalog tree
//! (databases → schemas → kinds → objects → columns) and the saved
//! worksheets.

use leptos::prelude::*;
use pinkdb_server_ui_api::sql::{quote_ident, quote_string};
use pinkdb_server_ui_api::{CatalogObject, DeleteWorksheet, Worksheet};
use wasm_bindgen::JsCast;

use crate::api;
use crate::app::Drag;
use crate::icon::Icon;
use crate::state::{App, ExplorerTab, Section, compact, count, is_relation, kind_icon};
use crate::ui::Empty;

/// The groups the catalog shows, in order.
pub const GROUPS: &[(&str, &str)] = &[
    ("TABLE", "Tables"),
    ("MATERIALIZED VIEW", "Materialized views"),
    ("LAKE TABLE", "Lake tables"),
    ("INDEX", "Indexes"),
    ("TRIGGER", "Triggers"),
    ("PROCEDURE", "Procedures"),
    ("MACRO", "Macros"),
];

/// The kinds a schema always shows a folder for, even empty; the others
/// appear once the schema holds one.
pub const ALWAYS: &[&str] = &["TABLE", "TRIGGER", "PROCEDURE"];

/// The key a catalog tree node remembers its open state under.
pub fn node_key(path: &[&str]) -> String {
    path.iter()
        .map(|part| part.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

/// The schemas the tree shows for a database, in order: those its objects
/// (the `SCHEMA` rows included) name.
pub fn schemas_of<'a>(objects: impl Iterator<Item = &'a CatalogObject>) -> Vec<String> {
    let mut schemas: Vec<String> = objects.map(|o| o.schema.clone()).collect();
    schemas.sort_by_key(|s| s.to_ascii_lowercase());
    schemas.dedup_by(|a, b| a.eq_ignore_ascii_case(b));
    schemas
}

/// Remember a node the user opened or closed (not while a filter forces the
/// tree open).
fn remember(app: App, key: String, filter: RwSignal<String>) -> impl Fn(web_sys::Event) + 'static {
    move |ev: web_sys::Event| {
        if !filter.get_untracked().is_empty() {
            return;
        }
        if let Some(details) = ev
            .current_target()
            .and_then(|t| t.dyn_into::<web_sys::HtmlDetailsElement>().ok())
        {
            app.remember_open(&key, details.open());
        }
    }
}

/// The SQL that previews a relation.
pub fn preview_sql(object: &CatalogObject) -> String {
    format!(
        "SELECT * FROM {}.{}.{} LIMIT 100;",
        quote_ident(&object.database),
        quote_ident(&object.schema),
        quote_ident(&object.name)
    )
}

/// The SQL that shows a lake table's history.
pub fn lake_sql(object: &CatalogObject) -> String {
    format!(
        "SELECT * FROM {}.{}.{} AT (VERSION => 1) LIMIT 100;\nSELECT * FROM lake_changes({}) LIMIT 100;\n",
        quote_ident(&object.database),
        quote_ident(&object.schema),
        quote_ident(&object.name),
        quote_string(&object.name)
    )
}

/// The name to insert for an object (qualified outside the current
/// database and schema).
pub fn insert_name(app: &App, object: &CatalogObject) -> String {
    if object
        .database
        .eq_ignore_ascii_case(&app.db.get_untracked())
        && object
            .schema
            .eq_ignore_ascii_case(&app.schema.get_untracked())
    {
        quote_ident(&object.name)
    } else {
        format!(
            "{}.{}.{}",
            quote_ident(&object.database),
            quote_ident(&object.schema),
            quote_ident(&object.name)
        )
    }
}

/// Open a preview of a relation in a new worksheet and run it.
pub fn preview(app: App, object: &CatalogObject) {
    let sql = preview_sql(object);
    app.go(Section::Query);
    let sheet = app.open(&object.name, &sql);
    app.run(sheet, sql);
}

#[component]
pub fn Explorer(drag: RwSignal<Option<Drag>>) -> impl IntoView {
    let app = expect_context::<App>();
    let filter = RwSignal::new(String::new());
    let query_section = move || app.section.get() == Section::Query;
    let showing_sheets =
        move || query_section() && app.explorer_tab.get() == ExplorerTab::Worksheets;
    let tab = move |label: &'static str, which: ExplorerTab| {
        view! {
            <button
                class="seg__item"
                class:is-active=move || app.explorer_tab.get() == which
                on:click=move |_| {
                    app.explorer_tab.set(which);
                    if which == ExplorerTab::Worksheets {
                        app.load_worksheets(false);
                    }
                }
            >
                {label}
            </button>
        }
    };
    view! {
        <aside
            class="explorer"
            aria-label="Explorer"
            style:width=move || format!("{}px", app.explorer_width.get())
        >
            <div class="explorer__head">
                <span class="explorer__title">
                    {move || if query_section() { "Explorer" } else { "Catalog" }}
                </span>
                <button
                    class="btn btn--ghost btn--icon btn--sm"
                    aria-label="Refresh"
                    title="Refresh"
                    on:click=move |_| {
                        app.load_catalog();
                        app.load_worksheets(false);
                    }
                >
                    <Icon name="refresh" size=14 />
                </button>
                {move || {
                    query_section()
                        .then(|| {
                            view! {
                                <button
                                    class="btn btn--ghost btn--icon btn--sm"
                                    aria-label="New worksheet"
                                    title="New worksheet"
                                    on:click=move |_| {
                                        app.open("Untitled", "");
                                    }
                                >
                                    <Icon name="plus" size=14 />
                                </button>
                            }
                        })
                }}
                <button
                    class="btn btn--ghost btn--icon btn--sm"
                    aria-label="Hide the explorer"
                    title=format!("Hide the explorer ({})", crate::state::shortcut("B"))
                    on:click=move |_| app.explorer_open.set(false)
                >
                    <Icon name="panel-left" size=14 />
                </button>
            </div>
            <div class="explorer__tools">
                {move || {
                    query_section()
                        .then(|| {
                            view! {
                                <div class="seg" role="tablist">
                                    {tab("Objects", ExplorerTab::Objects)}
                                    {tab("Worksheets", ExplorerTab::Worksheets)}
                                </div>
                            }
                        })
                }}
                <label class="search">
                    <Icon name="search" size=14 />
                    <input
                        id="explorer-filter"
                        class="input input--sm"
                        type="search"
                        aria-label="Filter"
                        placeholder=move || {
                            if showing_sheets() { "Filter worksheets" } else { "Filter tables and columns" }
                        }
                        prop:value=move || filter.get()
                        on:input=move |ev| filter.set(event_target_value(&ev))
                    />
                    <span class="kbd">"/"</span>
                </label>
            </div>
            <div class="explorer__body">
                {move || {
                    if showing_sheets() {
                        view! { <WorksheetList filter /> }.into_any()
                    } else {
                        view! { <CatalogTree filter /> }.into_any()
                    }
                }}
            </div>
            <div
                class="resize-x"
                role="separator"
                aria-orientation="vertical"
                aria-label="Resize the explorer"
                on:mousedown=move |ev: web_sys::MouseEvent| {
                    ev.prevent_default();
                    drag.set(
                        Some(Drag::Explorer {
                            x0: f64::from(ev.client_x()),
                            w0: app.explorer_width.get_untracked(),
                        }),
                    );
                }
                on:dblclick=move |_| app.explorer_width.set(272.0)
            ></div>
        </aside>
    }
}

/// A database's storage badge (wave 124): lakes say so, and an unavailable
/// one says why (in its tooltip). Local databases carry none.
pub fn storage_badge(info: &pinkdb_server_ui_api::DatabaseInfo) -> Option<AnyView> {
    if let Some(reason) = info.unavailable() {
        return Some(
            view! { <span class="badge badge--danger" title=reason.to_string()>"Unavailable"</span> }
                .into_any(),
        );
    }
    info.is_lake().then(|| {
        let title = info.location.clone().unwrap_or_default();
        let text = if info.read_only {
            "Lake · read-only"
        } else {
            "Lake"
        };
        view! { <span class="badge badge--info" title=title>{text}</span> }.into_any()
    })
}

#[component]
fn CatalogTree(filter: RwSignal<String>) -> impl IntoView {
    let app = expect_context::<App>();
    move || {
        let needle = filter.get().to_ascii_lowercase();
        let filtering = !needle.is_empty();
        let catalog = app.catalog.get();
        let current = app.db.get();
        if catalog.databases.is_empty() {
            return view! { <Empty icon="database" title="No databases" body="The catalog is loading, or this server has none." /> }
                .into_any();
        }
        let mut any = false;
        let dbs = catalog
            .databases
            .iter()
            .map(|info| {
                let db = &info.name;
                let objects: Vec<CatalogObject> = catalog
                    .objects
                    .iter()
                    .filter(|o| o.database.eq_ignore_ascii_case(db))
                    .filter(|o| {
                        needle.is_empty()
                            || o.name.to_ascii_lowercase().contains(&needle)
                            || o.columns
                                .iter()
                                .any(|c| c.name.to_ascii_lowercase().contains(&needle))
                    })
                    .cloned()
                    .collect();
                if filtering && objects.is_empty() {
                    return ().into_any();
                }
                any = true;
                let is_current = db.eq_ignore_ascii_case(&current);
                let schemas = schemas_of(objects.iter())
                    .into_iter()
                    .map(|schema| {
                        let in_schema: Vec<&CatalogObject> = objects
                            .iter()
                            .filter(|o| o.schema.eq_ignore_ascii_case(&schema))
                            .collect();
                        let n_objects = in_schema
                            .iter()
                            .filter(|o| o.kind != "SCHEMA")
                            .count();
                        let folders = GROUPS
                            .iter()
                            .filter_map(|(kind, label)| {
                                let members: Vec<CatalogObject> = in_schema
                                    .iter()
                                    .filter(|o| o.kind == *kind)
                                    .map(|o| (*o).clone())
                                    .collect();
                                if members.is_empty() && (filtering || !ALWAYS.contains(kind)) {
                                    return None;
                                }
                                let n = members.len();
                                let key = node_key(&[db, &schema, kind]);
                                let open = filtering || app.tree_is_open(&key, is_current && *kind == "TABLE");
                                let items = if members.is_empty() {
                                    view! { <li class="tree__empty depth-3">"None"</li> }.into_any()
                                } else {
                                    members
                                        .into_iter()
                                        .map(|object| view! { <ObjectItem object filter /> })
                                        .collect_view()
                                        .into_any()
                                };
                                Some(view! {
                                    <li>
                                        <details open=open on:toggle=remember(app, key, filter)>
                                            <summary class="tree__row depth-2" title=format!("{label} in {db}.{schema}")>
                                                <Icon name="chevron-right" class="tree__chev" />
                                                <Icon name=kind_icon(kind) class="tree__kind" />
                                                <span class="tree__name">{*label}</span>
                                                <span class="tree__detail">{n}</span>
                                            </summary>
                                            <ul>{items}</ul>
                                        </details>
                                    </li>
                                })
                            })
                            .collect_view();
                        let key = node_key(&[db, &schema]);
                        let open = filtering || app.tree_is_open(&key, is_current);
                        view! {
                            <li>
                                <details open=open on:toggle=remember(app, key, filter)>
                                    <summary class="tree__row depth-1" title=format!("Schema {db}.{schema}")>
                                        <Icon name="chevron-right" class="tree__chev" />
                                        <Icon name="schema" class="tree__kind" />
                                        <span class="tree__name">{schema.clone()}</span>
                                        <span class="tree__detail">{n_objects}</span>
                                    </summary>
                                    <ul>{folders}</ul>
                                </details>
                            </li>
                        }
                    })
                    .collect_view();
                let db_name = db.clone();
                let key = node_key(&[db]);
                let open = filtering || app.tree_is_open(&key, is_current);
                view! {
                    <li>
                        <details open=open on:toggle=remember(app, key, filter)>
                            <summary class="tree__row" title=format!("Database {db}")>
                                <Icon name="chevron-right" class="tree__chev" />
                                <Icon name="database" class="tree__kind" />
                                <span class="tree__name">{db.clone()}</span>
                                {storage_badge(info)}
                                {if is_current {
                                    view! { <span class="badge badge--accent">"Current"</span> }.into_any()
                                } else {
                                    view! {
                                        <span class="tree__actions">
                                            <button
                                                class="btn btn--ghost btn--sm"
                                                title="Run the worksheet's statements in this database"
                                                on:click=move |ev| {
                                                    ev.prevent_default();
                                                    app.use_database(db_name.clone());
                                                }
                                            >
                                                "Use"
                                            </button>
                                        </span>
                                    }
                                        .into_any()
                                }}
                            </summary>
                            <ul>{schemas}</ul>
                        </details>
                    </li>
                }
                    .into_any()
            })
            .collect_view();
        if !any {
            return view! { <Empty icon="search" title="No matches" body="No table or column has that name." /> }
                .into_any();
        }
        view! { <ul class="tree">{dbs}</ul> }.into_any()
    }
}

#[component]
fn ObjectItem(object: CatalogObject, filter: RwSignal<String>) -> impl IntoView {
    let app = expect_context::<App>();
    let needle = filter.get_untracked().to_ascii_lowercase();
    let relation = is_relation(&object.kind);
    let name = object.name.clone();
    let detail = match (&object.table_name, &object.detail) {
        (Some(t), _) => format!("on {t}"),
        (None, Some(d)) => d.clone(),
        (None, None) => object.rows.map(compact).unwrap_or_default(),
    };
    let title = match object.rows {
        Some(r) => format!(
            "{} · {} rows",
            object.kind.to_ascii_lowercase(),
            count(r.max(0) as u64)
        ),
        None => object.kind.to_ascii_lowercase(),
    };
    let key = (
        object.database.clone(),
        object.schema.clone(),
        object.name.clone(),
    );
    let selected = move || app.selected.get().as_ref() == Some(&key);
    let in_catalog = move || app.section.get() == Section::Catalog;
    let icon = kind_icon(&object.kind);

    // The Catalog section lists objects; a click opens the object's page.
    if !relation || in_catalog() {
        let page = object.clone();
        let insert = insert_name(&app, &object);
        return view! {
            <li>
                <div
                    class="tree__row depth-3"
                    class:is-selected=selected
                    title=title
                    tabindex="0"
                    on:click=move |_| {
                        if in_catalog() {
                            app.show_object(&page);
                        } else {
                            app.insert.set(Some(insert.clone()));
                        }
                    }
                >
                    <Icon name="chevron-right" class="tree__chev tree__chev--none" />
                    <Icon name=icon class="tree__kind" />
                    <span class="tree__name tree__name--mono">{name}</span>
                    <span class="tree__detail">{detail}</span>
                </div>
            </li>
        }
        .into_any();
    }

    let insert = insert_name(&app, &object);
    let for_preview = object.clone();
    let for_page = object.clone();
    let for_lake = object.clone();
    let lake = object.kind == "LAKE TABLE";
    let columns = object
        .columns
        .iter()
        .map(|c| {
            let mut stats = vec![c.dtype.to_ascii_lowercase()];
            if let Some(n) = c.null_count {
                stats.push(format!("nulls {}", count(n.max(0) as u64)));
            }
            if let Some(d) = c.distinct_count {
                stats.push(format!("distinct ≈{}", count(d.max(0) as u64)));
            }
            if let (Some(lo), Some(hi)) = (&c.min, &c.max) {
                stats.push(format!("{lo} … {hi}"));
            }
            let column = quote_ident(&c.name);
            view! {
                <li>
                    <div
                        class="tree__row depth-4"
                        title=stats.join(" · ")
                        on:click=move |_| app.insert.set(Some(column.clone()))
                    >
                        <Icon name="chevron-right" class="tree__chev tree__chev--none" />
                        <Icon name="column" size=14 class="tree__kind" />
                        <span class="tree__name tree__name--mono">{c.name.clone()}</span>
                        <span class="tree__detail mono">{c.dtype.to_ascii_lowercase()}</span>
                    </div>
                </li>
            }
        })
        .collect_view();
    // A filter that matched a column opens the table to show it; otherwise
    // the table is as the user left it.
    let key = node_key(&[&object.database, &object.schema, &object.kind, &object.name]);
    let open = if needle.is_empty() {
        app.tree_is_open(&key, false)
    } else {
        !object.name.to_ascii_lowercase().contains(&needle)
            && object
                .columns
                .iter()
                .any(|c| c.name.to_ascii_lowercase().contains(&needle))
    };
    view! {
        <li>
            <details open=open on:toggle=remember(app, key, filter)>
                <summary class="tree__row depth-3" title=title>
                    <Icon name="chevron-right" class="tree__chev" />
                    <Icon name=icon class="tree__kind" />
                    <span class="tree__name tree__name--mono">{name}</span>
                    <span class="tree__detail">{detail}</span>
                    <span class="tree__actions">
                        <button
                            class="btn btn--ghost btn--icon btn--sm"
                            aria-label="Insert the name at the cursor"
                            title="Insert the name at the cursor"
                            on:click=move |ev| {
                                ev.prevent_default();
                                app.insert.set(Some(insert.clone()));
                            }
                        >
                            <Icon name="insert" size=14 />
                        </button>
                        <button
                            class="btn btn--ghost btn--icon btn--sm"
                            aria-label="Preview 100 rows"
                            title="Preview 100 rows in a new worksheet"
                            on:click=move |ev| {
                                ev.prevent_default();
                                preview(app, &for_preview);
                            }
                        >
                            <Icon name="play" size=14 />
                        </button>
                        {lake
                            .then(|| {
                                view! {
                                    <button
                                        class="btn btn--ghost btn--icon btn--sm"
                                        aria-label="Time travel and the change feed"
                                        title="Time travel and the change feed"
                                        on:click=move |ev| {
                                            ev.prevent_default();
                                            app.open("Lake history", &lake_sql(&for_lake));
                                        }
                                    >
                                        <Icon name="clock" size=14 />
                                    </button>
                                }
                            })}
                        <button
                            class="btn btn--ghost btn--icon btn--sm"
                            aria-label="Details"
                            title="Columns, statistics and indexes"
                            on:click=move |ev| {
                                ev.prevent_default();
                                app.show_object(&for_page);
                            }
                        >
                            <Icon name="info" size=14 />
                        </button>
                    </span>
                </summary>
                <ul>{columns}</ul>
            </details>
        </li>
    }
    .into_any()
}

/// Delete a saved worksheet (after the confirm dialog) and close its tabs.
pub fn delete_worksheet(app: App, sheet: Worksheet) {
    let title = sheet.title.clone();
    app.ask_first(
        format!("Delete “{title}”?"),
        "The worksheet is deleted for you and everyone it is shared with. Its run history stays.",
        "",
        "Delete worksheet",
        move |_| {
            let sheet = sheet.clone();
            let csrf = app.csrf();
            leptos::task::spawn_local(async move {
                match api::post::<_, serde_json::Value>(
                    "/api/worksheets/delete",
                    &DeleteWorksheet {
                        id: sheet.id.clone(),
                    },
                    &csrf,
                )
                .await
                {
                    Ok(_) => {
                        app.saved.update(|l| l.retain(|w| w.id != sheet.id));
                        let open: Vec<u64> = app.sheets.with_untracked(|s| {
                            s.iter()
                                .filter(|s| {
                                    s.id.get_untracked().as_deref() == Some(sheet.id.as_str())
                                })
                                .map(|s| s.key)
                                .collect()
                        });
                        for key in open {
                            if let Some(s) = app
                                .sheets
                                .with_untracked(|s| s.iter().find(|s| s.key == key).copied())
                            {
                                s.dirty.set(false);
                                s.id.set(None);
                            }
                            app.close(key);
                        }
                        app.toast(
                            crate::state::Tone::Success,
                            "Worksheet deleted",
                            sheet.title,
                        );
                    }
                    Err(e) => app.toast(
                        crate::state::Tone::Danger,
                        "Could not delete the worksheet",
                        e.message,
                    ),
                }
            });
        },
    );
}

#[component]
fn WorksheetList(filter: RwSignal<String>) -> impl IntoView {
    let app = expect_context::<App>();
    let open_id = move || app.current().and_then(|s| s.id.get());
    let row = move |sheet: Worksheet| {
        let open = sheet.clone();
        let gone = sheet.clone();
        let id = sheet.id.clone();
        let detail = if sheet.mine {
            sheet.updated_at.chars().take(16).collect::<String>()
        } else {
            sheet.owner.clone()
        };
        let shared = sheet.mine && !sheet.shared_with.trim().is_empty();
        let title = format!(
            "Updated {}{}",
            sheet.updated_at,
            if shared {
                format!(" · shared with {}", sheet.shared_with)
            } else {
                String::new()
            }
        );
        view! {
            <li>
                <div
                    class="tree__row"
                    class:is-selected=move || open_id().as_deref() == Some(id.as_str())
                    title=title
                    tabindex="0"
                    on:click=move |_| app.open_saved(&open)
                >
                    <Icon name="file-code" class="tree__kind" />
                    <span class="tree__name">{sheet.title.clone()}</span>
                    {shared.then(|| view! { <Icon name="share" size=14 class="subtle" /> })}
                    <span class="tree__detail">{detail}</span>
                    {sheet
                        .mine
                        .then(|| {
                            view! {
                                <span class="tree__actions">
                                    <button
                                        class="btn btn--ghost btn--icon btn--sm"
                                        aria-label="Delete this worksheet"
                                        title="Delete this worksheet"
                                        on:click=move |ev| {
                                            ev.stop_propagation();
                                            delete_worksheet(app, gone.clone());
                                        }
                                    >
                                        <Icon name="trash" size=14 />
                                    </button>
                                </span>
                            }
                        })}
                </div>
            </li>
        }
    };
    move || {
        let needle = filter.get().to_ascii_lowercase();
        let saved: Vec<Worksheet> = app
            .saved
            .get()
            .into_iter()
            .filter(|w| needle.is_empty() || w.title.to_ascii_lowercase().contains(&needle))
            .collect();
        if saved.is_empty() {
            return if needle.is_empty() {
                view! {
                    <Empty
                        icon="file-code"
                        title="No saved worksheets"
                        body="Worksheets save as you type. Start one to keep a query around."
                    >
                        <button class="btn btn--sm" on:click=move |_| {
                            app.open("Untitled", "");
                        }>
                            <Icon name="plus" size=14 />
                            "New worksheet"
                        </button>
                    </Empty>
                }
                .into_any()
            } else {
                view! { <Empty icon="search" title="No matches" /> }.into_any()
            };
        }
        let (mine, shared): (Vec<_>, Vec<_>) = saved.into_iter().partition(|w| w.mine);
        view! {
            {(!mine.is_empty())
                .then(|| {
                    view! {
                        <div class="overline explorer__section">"Mine"</div>
                        <ul class="tree">{mine.into_iter().map(row).collect_view()}</ul>
                    }
                })}
            {(!shared.is_empty())
                .then(|| {
                    view! {
                        <div class="overline explorer__section">"Shared with me"</div>
                        <ul class="tree">{shared.into_iter().map(row).collect_view()}</ul>
                    }
                })}
        }
        .into_any()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(schema: &str, name: &str) -> CatalogObject {
        CatalogObject {
            database: "main".to_string(),
            schema: schema.to_string(),
            name: name.to_string(),
            kind: "TABLE".to_string(),
            table_name: None,
            detail: None,
            rows: None,
            columns: Vec::new(),
        }
    }

    #[test]
    fn node_keys_ignore_case_and_keep_parts_apart() {
        assert_eq!(node_key(&["Sales", "MAIN"]), node_key(&["sales", "main"]));
        assert_ne!(node_key(&["a", "bc"]), node_key(&["ab", "c"]));
    }

    #[test]
    fn a_database_shows_its_schemas() {
        assert_eq!(schemas_of(std::iter::empty()), Vec::<String>::new());
        let objects = [
            object("main", "t"),
            object("MAIN", "u"),
            object("audit", "v"),
        ];
        assert_eq!(schemas_of(objects.iter()), vec!["audit", "main"]);
    }

    #[test]
    fn generated_sql_quotes_every_part() {
        let odd = object("a b", "x\"y");
        assert_eq!(
            preview_sql(&odd),
            "SELECT * FROM main.\"a b\".\"x\"\"y\" LIMIT 100;"
        );
    }

    #[test]
    fn every_kind_has_a_folder() {
        for kind in ALWAYS {
            assert!(GROUPS.iter().any(|(k, _)| k == kind), "{kind}");
        }
        assert!(GROUPS.iter().any(|(k, _)| *k == "PROCEDURE"));
        // Plain views are session-local and have no folder.
        assert!(!GROUPS.iter().any(|(k, _)| *k == "VIEW"));
    }
}
