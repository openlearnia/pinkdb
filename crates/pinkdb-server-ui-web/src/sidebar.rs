//! The left panel: the catalog browser, the saved worksheets and the run
//! history.

use leptos::prelude::*;
use pinkdb_server_ui_api::sql::{quote_ident, quote_string};
use pinkdb_server_ui_api::{CatalogObject, DeleteWorksheet, Worksheet};

use crate::api;
use crate::state::{App, Side, count};

/// The groups the catalog shows, in order.
const GROUPS: &[(&str, &str)] = &[
    ("TABLE", "Tables"),
    ("VIEW", "Views"),
    ("MATERIALIZED VIEW", "Materialized views"),
    ("LAKE TABLE", "Lake tables"),
    ("INDEX", "Indexes"),
    ("TRIGGER", "Triggers"),
    ("MACRO", "Macros"),
];

#[component]
pub fn Sidebar() -> impl IntoView {
    let app = expect_context::<App>();
    let tab = move |label: &'static str, side: Side| {
        view! {
            <button
                class="stab"
                class:active=move || app.side.get() == side
                on:click=move |_| {
                    app.side.set(side);
                    match side {
                        Side::History => app.load_history(),
                        Side::Worksheets => app.load_worksheets(false),
                        Side::Catalog => app.load_catalog(),
                        Side::Access | Side::Storage => {}
                    }
                }
            >
                {label}
            </button>
        }
    };
    view! {
        <aside class="sidebar">
            <div class="stabs">
                {tab("Catalog", Side::Catalog)}
                {tab("Sheets", Side::Worksheets)}
                {tab("History", Side::History)}
                {tab("Access", Side::Access)}
                {tab("Storage", Side::Storage)}
            </div>
            {move || match app.side.get() {
                Side::Catalog => view! { <CatalogTree /> }.into_any(),
                Side::Worksheets => view! { <Worksheets /> }.into_any(),
                Side::History => view! { <History /> }.into_any(),
                Side::Access => view! { <crate::access::AccessPanel /> }.into_any(),
                Side::Storage => view! { <crate::access::StoragePanel /> }.into_any(),
            }}
        </aside>
    }
}

/// The SQL that previews a relation.
fn preview_sql(object: &CatalogObject) -> String {
    format!(
        "SELECT * FROM {}.{} LIMIT 100;",
        quote_ident(&object.database),
        quote_ident(&object.name)
    )
}

#[component]
fn CatalogTree() -> impl IntoView {
    let app = expect_context::<App>();
    let filter = RwSignal::new(String::new());
    let tree = move || {
        let needle = filter.get().to_ascii_lowercase();
        let catalog = app.catalog.get();
        let current = app.db.get();
        catalog
            .databases
            .iter()
            .map(|db| {
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
                let open = db.eq_ignore_ascii_case(&current) || !needle.is_empty();
                let groups = GROUPS
                    .iter()
                    .filter_map(|(kind, label)| {
                        let members: Vec<CatalogObject> = objects
                            .iter()
                            .filter(|o| o.kind == *kind)
                            .cloned()
                            .collect();
                        (!members.is_empty()).then(|| {
                            let items = members
                                .into_iter()
                                .map(|object| view! { <ObjectItem object /> })
                                .collect_view();
                            view! {
                                <div class="group">
                                    <div class="group-label">{*label}</div>
                                    <ul>{items}</ul>
                                </div>
                            }
                        })
                    })
                    .collect_view();
                let db_name = db.clone();
                view! {
                    <details class="db" open=open>
                        <summary>
                            <span class="db-name">{db.clone()}</span>
                            <button
                                class="mini"
                                title="Use this database for the worksheet's statements"
                                on:click=move |ev| {
                                    ev.prevent_default();
                                    app.db.set(db_name.clone());
                                }
                            >
                                "use"
                            </button>
                        </summary>
                        {groups}
                    </details>
                }
            })
            .collect_view()
    };
    view! {
        <div class="panel">
            <input
                class="filter"
                type="search"
                placeholder="Filter tables and columns"
                prop:value=move || filter.get()
                on:input=move |ev| filter.set(event_target_value(&ev))
            />
            <div class="tree">{tree}</div>
        </div>
    }
}

#[component]
fn ObjectItem(object: CatalogObject) -> impl IntoView {
    let app = expect_context::<App>();
    let relation = matches!(
        object.kind.as_str(),
        "TABLE" | "VIEW" | "MATERIALIZED VIEW" | "LAKE TABLE"
    );
    let name = object.name.clone();
    let insert_name = if object
        .database
        .eq_ignore_ascii_case(&app.db.get_untracked())
    {
        quote_ident(&object.name)
    } else {
        format!(
            "{}.{}",
            quote_ident(&object.database),
            quote_ident(&object.name)
        )
    };
    let detail = match (&object.table_name, &object.detail) {
        (Some(t), Some(d)) => format!("on {t} · {d}"),
        (Some(t), None) => format!("on {t}"),
        (None, Some(d)) => d.clone(),
        (None, None) => object
            .rows
            .map(|r| format!("{} rows", count(r.max(0) as u64)))
            .unwrap_or_default(),
    };
    if !relation {
        return view! {
            <li class="object">
                <span class="obj-name" on:click=move |_| app.insert.set(Some(insert_name.clone()))>
                    {name}
                </span>
                <span class="obj-detail">{detail}</span>
            </li>
        }
        .into_any();
    }
    let preview = preview_sql(&object);
    let lake = object.kind == "LAKE TABLE";
    let travel = format!(
        "SELECT * FROM {}.{} AT (VERSION => 1) LIMIT 100;",
        quote_ident(&object.database),
        quote_ident(&object.name)
    );
    let changes = format!(
        "SELECT * FROM lake_changes({}) LIMIT 100;",
        quote_string(&object.name)
    );
    let columns = object
        .columns
        .iter()
        .map(|c| {
            let mut stats = Vec::new();
            if let Some(n) = c.null_count {
                stats.push(format!("nulls {}", count(n.max(0) as u64)));
            }
            if let Some(d) = c.distinct_count {
                stats.push(format!("distinct ≈{}", count(d.max(0) as u64)));
            }
            if let (Some(lo), Some(hi)) = (&c.min, &c.max) {
                stats.push(format!("{lo} … {hi}"));
            }
            let title = stats.join(" · ");
            let column = quote_ident(&c.name);
            view! {
                <li class="column" title=title on:click=move |_| app.insert.set(Some(column.clone()))>
                    <span class="col-name">{c.name.clone()}</span>
                    <span class="col-type">{c.dtype.clone()}</span>
                </li>
            }
        })
        .collect_view();
    view! {
        <li class="object">
            <details>
                <summary>
                    <span class="obj-name">{name.clone()}</span>
                    <span class="obj-detail">{detail}</span>
                    <span class="obj-actions">
                        <button
                            class="mini"
                            title="Insert the name at the cursor"
                            on:click=move |ev| {
                                ev.prevent_default();
                                app.insert.set(Some(insert_name.clone()));
                            }
                        >
                            "+"
                        </button>
                        <button
                            class="mini"
                            title="Preview 100 rows in a new worksheet"
                            on:click=move |ev| {
                                ev.prevent_default();
                                let sheet = app.open(&name, &preview);
                                app.run(sheet, preview.clone());
                            }
                        >
                            "▶"
                        </button>
                        {lake
                            .then(|| {
                                view! {
                                    <button
                                        class="mini"
                                        title="Time travel and the change feed"
                                        on:click=move |ev| {
                                            ev.prevent_default();
                                            app.open("Lake history", &format!("{travel}\n{changes}\n"));
                                        }
                                    >
                                        "⏱"
                                    </button>
                                }
                            })}
                    </span>
                </summary>
                <ul class="columns">{columns}</ul>
            </details>
        </li>
    }
    .into_any()
}

#[component]
fn Worksheets() -> impl IntoView {
    let app = expect_context::<App>();
    let delete = move |sheet: Worksheet| {
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
                            .filter(|s| s.id.get_untracked().as_deref() == Some(sheet.id.as_str()))
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
                }
                Err(e) => app.notice.set(format!("delete: {e}")),
            }
        });
    };
    let list = move || {
        let saved = app.saved.get();
        if saved.is_empty() {
            return view! { <p class="empty">"No saved worksheets yet."</p> }.into_any();
        }
        saved
            .into_iter()
            .map(|sheet| {
                let open = sheet.clone();
                let gone = sheet.clone();
                let mine = sheet.mine;
                let meta = if mine {
                    if sheet.shared_with.is_empty() {
                        format!("updated {}", sheet.updated_at)
                    } else {
                        format!("shared with {}", sheet.shared_with)
                    }
                } else {
                    format!("by {}", sheet.owner)
                };
                view! {
                    <li class="saved">
                        <div class="saved-title" on:click=move |_| app.open_saved(&open)>
                            {sheet.title.clone()}
                        </div>
                        <div class="saved-meta">{meta}</div>
                        {mine
                            .then(|| {
                                view! {
                                    <button
                                        class="mini danger"
                                        title="Delete this worksheet"
                                        on:click=move |_| delete(gone.clone())
                                    >
                                        "delete"
                                    </button>
                                }
                            })}
                    </li>
                }
            })
            .collect_view()
            .into_any()
    };
    view! {
        <div class="panel">
            <button class="wide" on:click=move |_| {
                app.open("Untitled", "");
            }>"+ New worksheet"</button>
            <ul class="saved-list">{list}</ul>
        </div>
    }
}

#[component]
fn History() -> impl IntoView {
    let app = expect_context::<App>();
    let list = move || {
        let history = app.history.get();
        if history.is_empty() {
            return view! { <p class="empty">"No runs yet."</p> }.into_any();
        }
        history
            .into_iter()
            .map(|entry| {
                let sql = entry.sql.clone();
                let first = entry
                    .sql
                    .lines()
                    .find(|l| !l.trim().is_empty())
                    .unwrap_or("")
                    .to_string();
                let status = entry.status.clone();
                let meta = format!(
                    "{} · {} ms · {} rows",
                    entry.started_at, entry.elapsed_ms, entry.rows
                );
                view! {
                    <li class="hist" title=entry.error.clone() on:click=move |_| {
                        app.open("From history", &sql);
                    }>
                        <span class=format!("status {status}")>{status.clone()}</span>
                        <span class="hist-sql">{first}</span>
                        <div class="hist-meta">{meta}</div>
                    </li>
                }
            })
            .collect_view()
            .into_any()
    };
    view! {
        <div class="panel">
            <ul class="hist-list">{list}</ul>
        </div>
    }
}
