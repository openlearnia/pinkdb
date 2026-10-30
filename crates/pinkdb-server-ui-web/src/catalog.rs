//! The Catalog section's pages: a database's objects, and one object's
//! columns, statistics, preview and dependents.

use leptos::prelude::*;
use leptos::task::spawn_local;
use pinkdb_server_ui_api::{CatalogObject, QueryRequest, QueryResponse, StatementResult};

use crate::api;
use crate::explorer::{GROUPS, lake_sql, preview, preview_sql};
use crate::icon::Icon;
use crate::state::{
    App, Section, Tone, count, is_relation, kind_icon, kind_label, numeric, plural,
};
use crate::ui::{Callout, Empty};

#[component]
pub fn CatalogPage() -> impl IntoView {
    let app = expect_context::<App>();
    move || {
        let object = app
            .selected
            .get()
            .and_then(|(db, name)| app.object(&db, &name));
        let loading =
            app.selected.with(Option::is_some) && app.catalog.with(|c| c.objects.is_empty());
        match object {
            Some(object) => view! { <ObjectPage object /> }.into_any(),
            // A link to an object, before the catalog has arrived.
            None if loading => {
                view! { <div class="empty"><span class="spinner"></span></div> }.into_any()
            }
            None => view! { <DatabasePage /> }.into_any(),
        }
    }
}

/// A number or a dash.
fn maybe(n: Option<i64>) -> String {
    n.map_or_else(|| "—".to_string(), |n| count(n.max(0) as u64))
}

#[component]
fn DatabasePage() -> impl IntoView {
    let app = expect_context::<App>();
    move || {
        let db = app.db.get();
        let objects: Vec<CatalogObject> = app.catalog.with(|c| {
            c.objects
                .iter()
                .filter(|o| o.database.eq_ignore_ascii_case(&db))
                .cloned()
                .collect()
        });
        let of = |kind: &str| objects.iter().filter(|o| o.kind == kind).count();
        let rows: i64 = objects
            .iter()
            .filter(|o| o.kind == "TABLE" || o.kind == "LAKE TABLE")
            .filter_map(|o| o.rows)
            .sum();
        let table = if objects.is_empty() {
            view! {
                <Empty icon="database" title="This database is empty" body="Create a table in a worksheet, or import a file.">
                    {(!app.read_only())
                        .then(|| {
                            view! {
                                <button class="btn btn--sm" on:click=move |_| app.import_open.set(true)>
                                    <Icon name="upload" size=14 />
                                    "Import data"
                                </button>
                            }
                        })}
                </Empty>
            }
            .into_any()
        } else {
            let mut sorted = objects.clone();
            let order = |k: &str| GROUPS.iter().position(|(g, _)| *g == k).unwrap_or(99);
            sorted.sort_by(|a, b| {
                order(&a.kind)
                    .cmp(&order(&b.kind))
                    .then(a.name.cmp(&b.name))
            });
            let body = sorted
                .into_iter()
                .map(|o| {
                    let open = o.clone();
                    let detail = o
                        .table_name
                        .as_ref()
                        .map(|t| format!("on {t}"))
                        .or_else(|| o.detail.clone())
                        .unwrap_or_default();
                    let columns = if o.columns.is_empty() {
                        "—".to_string()
                    } else {
                        o.columns.len().to_string()
                    };
                    view! {
                        <tr class="is-clickable" on:click=move |_| app.show_object(&open)>
                            <td>
                                <span class="row">
                                    <Icon name=kind_icon(&o.kind) class="subtle" />
                                    <span class="mono">{o.name.clone()}</span>
                                </span>
                            </td>
                            <td><span class="badge">{kind_label(&o.kind)}</span></td>
                            <td class="t-num">{maybe(o.rows)}</td>
                            <td class="t-num">{columns}</td>
                            <td class="muted t-ellipsis">{detail}</td>
                        </tr>
                    }
                })
                .collect_view();
            view! {
                <table class="table">
                    <thead>
                        <tr>
                            <th>"Name"</th>
                            <th class="col-160">"Kind"</th>
                            <th class="t-num col-120">"Rows"</th>
                            <th class="t-num col-100">"Columns"</th>
                            <th>"Detail"</th>
                        </tr>
                    </thead>
                    <tbody>{body}</tbody>
                </table>
            }
            .into_any()
        };
        let n = objects.len();
        view! {
            <div class="page">
                <div class="page__inner">
                    <div class="page-head">
                        <div class="page-head__text">
                            <h1 class="page-head__title">
                                <span class="mono">{db.clone()}</span>
                                <span class="badge">"Database"</span>
                            </h1>
                            <p class="page-head__desc">
                                {format!("{n} object{}. Pick one in the catalog for its columns and statistics.", if n == 1 { "" } else { "s" })}
                            </p>
                        </div>
                    </div>
                    <div class="metrics">
                        <div class="card metric">
                            <span class="overline">"Tables"</span>
                            <span class="metric__value">{of("TABLE")}</span>
                            <span class="metric__caption">{plural(of("LAKE TABLE"), "lake table")}</span>
                        </div>
                        <div class="card metric">
                            <span class="overline">"Materialized views"</span>
                            <span class="metric__value">{of("MATERIALIZED VIEW")}</span>
                            <span class="metric__caption">"stored query results"</span>
                        </div>
                        <div class="card metric">
                            <span class="overline">"Rows"</span>
                            <span class="metric__value">{count(rows.max(0) as u64)}</span>
                            <span class="metric__caption">"across the tables"</span>
                        </div>
                        <div class="card metric">
                            <span class="overline">"Indexes and triggers"</span>
                            <span class="metric__value">{of("INDEX") + of("TRIGGER")}</span>
                            <span class="metric__caption">
                                {format!("{} · {}", plural(of("PROCEDURE"), "procedure"), plural(of("MACRO"), "macro"))}
                            </span>
                        </div>
                    </div>
                    <div class="card card--flush">{table}</div>
                </div>
            </div>
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Tab {
    Columns,
    Preview,
    Dependents,
}

#[component]
fn ObjectPage(object: CatalogObject) -> impl IntoView {
    let app = expect_context::<App>();
    let relation = is_relation(&object.kind);
    // A procedure or macro stands alone: no columns, no dependents.
    let routine = object.table_name.is_none() && !relation;
    let tab = RwSignal::new(if relation {
        Tab::Columns
    } else {
        Tab::Dependents
    });
    let dependents: Vec<CatalogObject> = app.catalog.with_untracked(|c| {
        c.objects
            .iter()
            .filter(|o| {
                o.database.eq_ignore_ascii_case(&object.database)
                    && o.table_name.as_deref() == Some(object.name.as_str())
            })
            .cloned()
            .collect()
    });
    let indexes = dependents.iter().filter(|o| o.kind == "INDEX").count();
    let triggers = dependents.iter().filter(|o| o.kind == "TRIGGER").count();
    let with_nulls = object
        .columns
        .iter()
        .filter(|c| c.null_count.is_some_and(|n| n > 0))
        .count();
    let group = GROUPS
        .iter()
        .find(|(k, _)| *k == object.kind)
        .map_or("Objects", |(_, l)| *l);
    let back = object.database.clone();
    let desc = {
        let mut parts = Vec::new();
        if relation {
            parts.push(format!("{} columns", object.columns.len()));
        }
        if let Some(t) = &object.table_name {
            parts.push(format!("on {t}"));
        }
        if let Some(d) = &object.detail {
            parts.push(d.clone());
        }
        parts.join(" · ")
    };
    let for_open = object.clone();
    let for_preview = object.clone();
    let for_lake = object.clone();
    let lake = object.kind == "LAKE TABLE";
    let actions = relation.then(|| {
        view! {
            {lake
                .then(|| {
                    view! {
                        <button class="btn" on:click=move |_| {
                            app.go(Section::Query);
                            app.open("Lake history", &lake_sql(&for_lake));
                        }>
                            <Icon name="clock" size=14 />
                            "Time travel"
                        </button>
                    }
                })}
            <button class="btn" on:click=move |_| {
                app.go(Section::Query);
                app.open(&for_open.name, &preview_sql(&for_open));
            }>
                <Icon name="file-code" size=14 />
                "Open in worksheet"
            </button>
            <button class="btn btn--primary" on:click=move |_| preview(app, &for_preview)>
                <Icon name="play" size=14 />
                "Preview rows"
            </button>
        }
    });
    let tab_button = move |label: String, which: Tab| {
        view! {
            <button
                class="tabs__item"
                class:is-active=move || tab.get() == which
                role="tab"
                on:click=move |_| tab.set(which)
            >
                {label}
            </button>
        }
    };
    let columns = StoredValue::new(object.columns.clone());
    let columns_table = move || {
        let rows = columns.with_value(|columns| {
            columns
            .iter()
            .map(|c| {
                let min = c.min.clone();
                let max = c.max.clone();
                let dash = || view! { <span class="subtle">"—"</span> };
                view! {
                    <tr>
                        <td class="mono">{c.name.clone()}</td>
                        <td class="mono muted">{c.dtype.to_ascii_lowercase()}</td>
                        <td class="t-num">{maybe(c.null_count)}</td>
                        <td class="t-num">{maybe(c.distinct_count)}</td>
                        <td class="mono t-clip" title=min.clone()>{min.clone().map(|m| m.into_any()).unwrap_or_else(|| dash().into_any())}</td>
                        <td class="mono t-clip" title=max.clone()>{max.clone().map(|m| m.into_any()).unwrap_or_else(|| dash().into_any())}</td>
                    </tr>
                }
            })
            .collect_view()
        });
        view! {
            <table class="table">
                <thead>
                    <tr>
                        <th>"Column"</th>
                        <th>"Type"</th>
                        <th class="t-num">"Nulls"</th>
                        <th class="t-num">"Distinct ≈"</th>
                        <th class="col-22p">"Min"</th>
                        <th class="col-22p">"Max"</th>
                    </tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
        }
    };
    let deps = dependents.clone();
    let dependents_table = move || {
        if deps.is_empty() {
            return view! { <Empty icon="index" title="No indexes or triggers" /> }.into_any();
        }
        let rows = deps
            .iter()
            .map(|o| {
                let open = o.clone();
                view! {
                    <tr class="is-clickable" on:click=move |_| app.show_object(&open)>
                        <td>
                            <span class="row">
                                <Icon name=kind_icon(&o.kind) class="subtle" />
                                <span class="mono">{o.name.clone()}</span>
                            </span>
                        </td>
                        <td><span class="badge">{kind_label(&o.kind)}</span></td>
                        <td class="muted">{o.detail.clone().unwrap_or_default()}</td>
                    </tr>
                }
            })
            .collect_view();
        view! {
            <table class="table">
                <thead><tr><th>"Name"</th><th class="col-140">"Kind"</th><th>"Detail"</th></tr></thead>
                <tbody>{rows}</tbody>
            </table>
        }
        .into_any()
    };
    let preview_object = object.clone();
    let body = move || match tab.get() {
        Tab::Columns => columns_table().into_any(),
        Tab::Preview => view! { <Preview object=preview_object.clone() /> }.into_any(),
        Tab::Dependents => dependents_table().into_any(),
    };
    let ncols = object.columns.len();
    let n_deps = dependents.len();
    view! {
        <div class="page">
            <div class="page__inner">
                <nav class="crumbs" aria-label="Breadcrumb">
                    <Icon name="database" size=14 />
                    <button class="btn btn--ghost btn--sm crumbs__link" on:click=move |_| {
                        app.selected.set(None);
                        app.db.set(back.clone());
                    }>
                        {object.database.clone()}
                    </button>
                    <span class="crumbs__sep">"›"</span>
                    <Icon name="schema" size=14 />
                    {object.schema.clone()}
                    <span class="crumbs__sep">"›"</span>
                    {group}
                    <span class="crumbs__sep">"›"</span>
                    <span class="mono">{object.name.clone()}</span>
                </nav>
                <div class="page-head">
                    <div class="page-head__text">
                        <h1 class="page-head__title">
                            <Icon name=kind_icon(&object.kind) size=18 />
                            <span class="mono">{object.name.clone()}</span>
                            <span class="badge">{kind_label(&object.kind)}</span>
                        </h1>
                        <p class="page-head__desc">{desc}</p>
                    </div>
                    <div class="page-head__actions">{actions}</div>
                </div>
                {relation
                    .then(|| {
                        view! {
                            <div class="metrics">
                                <div class="card metric">
                                    <span class="overline">"Rows"</span>
                                    <span class="metric__value">{maybe(object.rows)}</span>
                                    <span class="metric__caption">
                                        {if object.rows.is_some() { "from the catalog" } else { "computed when queried" }}
                                    </span>
                                </div>
                                <div class="card metric">
                                    <span class="overline">"Columns"</span>
                                    <span class="metric__value">{ncols}</span>
                                    <span class="metric__caption">{format!("{with_nulls} with NULLs")}</span>
                                </div>
                                <div class="card metric">
                                    <span class="overline">"Indexes"</span>
                                    <span class="metric__value">{indexes}</span>
                                    <span class="metric__caption">{format!("{triggers} triggers")}</span>
                                </div>
                            </div>
                        }
                    })}
                {if routine {
                    let (what, usage) = if object.kind == "PROCEDURE" {
                        ("Parameters and language", "Run it with CALL (administrators).")
                    } else {
                        ("Parameters", "Use it like a function in any query.")
                    };
                    view! {
                        <div class="card">
                            <div class="card__body card__body--kv">
                            <dl class="kv">
                                <dt>"Database"</dt>
                                <dd class="mono">{object.database.clone()}</dd>
                                <dt>"Schema"</dt>
                                <dd class="mono">{object.schema.clone()}</dd>
                                <dt>{what}</dt>
                                <dd class="mono">{object.detail.clone().filter(|d| !d.is_empty()).unwrap_or_else(|| "—".to_string())}</dd>
                                <dt>"Usage"</dt>
                                <dd>{usage}</dd>
                            </dl>
                            </div>
                        </div>
                    }
                        .into_any()
                } else {
                    view! {
                        <div class="card card--flush">
                            <div class="tabs" role="tablist">
                                {relation.then(|| tab_button("Columns".to_string(), Tab::Columns))}
                                {relation.then(|| tab_button("Preview".to_string(), Tab::Preview))}
                                {tab_button(format!("Indexes and triggers · {n_deps}"), Tab::Dependents)}
                            </div>
                            {body}
                        </div>
                    }
                        .into_any()
                }}
            </div>
        </div>
    }
}

/// The first rows of a relation, read when the tab opens.
#[component]
fn Preview(object: CatalogObject) -> impl IntoView {
    let app = expect_context::<App>();
    let result = RwSignal::new(None::<Result<StatementResult, String>>);
    let request = QueryRequest {
        sql: preview_sql(&object).replace("LIMIT 100", "LIMIT 50"),
        db: Some(object.database.clone()),
        run_id: api::run_id(),
        worksheet_id: String::new(),
    };
    let csrf = app.csrf();
    spawn_local(async move {
        let outcome = match api::post::<_, QueryResponse>("/api/query", &request, &csrf).await {
            Ok(r) => match (r.error, r.results.into_iter().next()) {
                (Some(e), _) => Err(e.message),
                (None, Some(first)) => Ok(first),
                (None, None) => Err("no result".to_string()),
            },
            Err(e) => Err(e.message),
        };
        result.set(Some(outcome));
    });
    move || match result.get() {
        None => view! { <div class="empty"><span class="spinner"></span></div> }.into_any(),
        Some(Err(e)) => view! {
            <div class="card__body">
                <Callout tone=Tone::Danger title="Could not read the rows">{e}</Callout>
            </div>
        }
        .into_any(),
        Some(Ok(r)) => view! { <StaticGrid result=r max_height=480 /> }.into_any(),
    }
}

/// A small, non-virtual grid (previews in pages and dialogs).
#[component]
pub fn StaticGrid(result: StatementResult, #[prop(optional)] max_height: u32) -> impl IntoView {
    let numeric: Vec<bool> = result.columns.iter().map(|c| numeric(&c.dtype)).collect();
    let head = result
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            view! {
                <th class:is-num=numeric[i]>
                    <span class="grid__name">{c.name.clone()}</span>
                    <span class="grid__type">{c.dtype.clone()}</span>
                </th>
            }
        })
        .collect_view();
    let rows = result
        .rows
        .iter()
        .enumerate()
        .map(|(r, row)| {
            let cells = row
                .iter()
                .enumerate()
                .map(|(c, cell)| match cell {
                    Some(text) => view! { <td class:is-num=numeric.get(c).copied().unwrap_or(false)>{text.clone()}</td> }.into_any(),
                    None => view! { <td class="is-null">"NULL"</td> }.into_any(),
                })
                .collect_view();
            view! {
                <tr>
                    <td class="grid__rn">{r + 1}</td>
                    {cells}
                    <td></td>
                </tr>
            }
        })
        .collect_view();
    let height = (max_height > 0).then(|| format!("{max_height}px"));
    view! {
        <div class="grid" style:max-height=height>
            <table>
                <thead>
                    <tr>
                        <th class="grid__rn">"#"</th>
                        {head}
                        <th class="col-fill"></th>
                    </tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
        </div>
    }
}
