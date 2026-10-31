//! The results panel: one tab per statement, a virtual-scrolled grid, the
//! cell viewer, `EXPLAIN` as a tree, the profile, a chart, and downloads.
//! Every value reaches the page as a text node.

use leptos::html;
use leptos::prelude::*;
use leptos::task::spawn_local;
use pinkdb_server_ui_api::{ExportRequest, StatementResult};
use wasm_bindgen::JsValue;

use crate::api;
use crate::chart::Chart;
use crate::icon::Icon;
use crate::state::{App, ResultView, Sheet, Tone, count, millis, numeric, shortcut};
use crate::ui::{Callout, Empty};

/// Grid metrics (they match `console.css`).
const ROW_HEIGHT: f64 = 24.0;
const RN_WIDTH: f64 = 52.0;
const OVERSCAN: usize = 8;

/// Column widths in pixels, from the header and the first rows.
fn widths(result: &StatementResult) -> Vec<f64> {
    result
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let header = c.name.chars().count().max(c.dtype.chars().count());
            let longest = result
                .rows
                .iter()
                .take(200)
                .map(|row| {
                    row.get(i)
                        .and_then(|v| v.as_deref())
                        .map_or(4, |v| v.chars().count().min(60))
                })
                .max()
                .unwrap_or(0);
            (header.max(longest) as f64 * 7.3 + 22.0).clamp(64.0, 420.0)
        })
        .collect()
}

/// Pretty-print a cell that holds JSON (a VARIANT, a STRUCT, a list).
pub fn pretty(text: &str) -> String {
    let trimmed = text.trim_start();
    if (trimmed.starts_with('{') || trimmed.starts_with('['))
        && let Ok(value) = serde_json::from_str::<serde_json::Value>(text)
    {
        return serde_json::to_string_pretty(&value).unwrap_or_else(|_| text.to_string());
    }
    text.to_string()
}

/// CSV of a result's rows (what the browser holds).
pub fn csv(result: &StatementResult) -> String {
    fn field(out: &mut String, text: &str) {
        if text.contains([',', '"', '\n', '\r']) {
            out.push('"');
            out.push_str(&text.replace('"', "\"\""));
            out.push('"');
        } else {
            out.push_str(text);
        }
    }
    let mut out = String::new();
    for (i, c) in result.columns.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        field(&mut out, &c.name);
    }
    out.push_str("\r\n");
    for row in &result.rows {
        for (i, cell) in row.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            if let Some(text) = cell {
                field(&mut out, text);
            }
        }
        out.push_str("\r\n");
    }
    out
}

/// JSON of a result's rows: numbers and booleans typed by their column.
pub fn json(result: &StatementResult) -> String {
    let rows: Vec<serde_json::Value> = result
        .rows
        .iter()
        .map(|row| {
            let mut object = serde_json::Map::new();
            for (column, cell) in result.columns.iter().zip(row) {
                let value = match cell {
                    None => serde_json::Value::Null,
                    Some(text) if numeric(&column.dtype) => text
                        .parse::<f64>()
                        .ok()
                        .and_then(serde_json::Number::from_f64)
                        .map(|n| {
                            text.parse::<i64>()
                                .map(serde_json::Value::from)
                                .unwrap_or(serde_json::Value::Number(n))
                        })
                        .unwrap_or_else(|| serde_json::Value::String(text.clone())),
                    Some(text) if column.dtype.eq_ignore_ascii_case("BOOLEAN") => {
                        serde_json::Value::Bool(text == "true")
                    }
                    Some(text) => serde_json::Value::String(text.clone()),
                };
                object.insert(column.name.clone(), value);
            }
            serde_json::Value::Object(object)
        })
        .collect();
    serde_json::to_string_pretty(&rows).unwrap_or_default()
}

/// Download a result: from the browser when it holds every row, else as a
/// server export (one read-only statement, re-run).
fn download(app: App, result: StatementResult, format: &'static str) {
    if !result.truncated {
        let (body, mime) = if format == "csv" {
            (csv(&result), "text/csv")
        } else {
            (json(&result), "application/json")
        };
        let parts = js_sys::Array::of1(&JsValue::from_str(&body));
        api::download(&format!("result.{format}"), mime, &parts);
        return;
    }
    let request = ExportRequest {
        sql: result.statement.clone(),
        db: Some(app.db.get_untracked()),
        schema: Some(app.schema.get_untracked()),
        format: format.to_string(),
    };
    let csrf = app.csrf();
    app.toast(
        Tone::Info,
        "Exporting every row",
        "The grid holds part of the result: the server runs the statement again.",
    );
    spawn_local(async move {
        match api::post_raw("/api/export", &request, &csrf, None).await {
            Ok(response) => match response.blob() {
                Ok(promise) => {
                    if let Ok(blob) = wasm_bindgen_futures::JsFuture::from(promise).await {
                        api::download_blob(&format!("result.{format}"), &blob.into());
                    }
                }
                Err(_) => app.toast(Tone::Danger, "Export failed", ""),
            },
            Err(e) => app.toast(Tone::Danger, "Export failed", e.message),
        }
    });
}

/// The running state: a ticking timer and Cancel.
#[component]
fn Running(sheet: Sheet) -> impl IntoView {
    let app = expect_context::<App>();
    let now = RwSignal::new(js_sys::Date::now());
    let tick = set_interval_with_handle(
        move || now.set(js_sys::Date::now()),
        std::time::Duration::from_millis(100),
    )
    .ok();
    on_cleanup(move || {
        if let Some(handle) = tick {
            handle.clear();
        }
    });
    view! {
        <div class="running" role="status">
            <span class="spinner spinner--lg"></span>
            <span class="running__time">
                {move || format!("{:.1} s", ((now.get() - sheet.started.get()) / 1000.0).max(0.0))}
            </span>
            <span>"Running"</span>
            <button class="btn btn--sm" on:click=move |_| app.cancel(sheet)>
                "Cancel"
                <span class="kbd">"Esc"</span>
            </button>
        </div>
    }
}

/// Whether a result is the plan of an `EXPLAIN`.
fn is_explain(result: &StatementResult) -> bool {
    result
        .statement
        .trim_start()
        .to_ascii_lowercase()
        .starts_with("explain")
        && result.columns.len() == 1
}

#[component]
pub fn Results(sheet: Sheet) -> impl IntoView {
    let app = expect_context::<App>();
    let body = move || {
        if sheet.running.get().is_some() {
            return view! { <div class="results__body"><Running sheet /></div> }.into_any();
        }
        if let Some(error) = sheet.run_error.get() {
            return view! {
                <div class="results__error">
                    <Callout tone=Tone::Danger title="The request failed">{error}</Callout>
                </div>
            }
            .into_any();
        }
        let Some(response) = sheet.result.get() else {
            return view! {
                <div class="results__body">
                    <Empty icon="play" title="Run a query">
                        <div class="empty__body">
                            "Results appear here. "
                            <span class="kbd">{shortcut("↵")}</span>
                            " runs the statement at the cursor, "
                            <span class="kbd">{shortcut("⇧↵")}</span>
                            " runs them all."
                        </div>
                    </Empty>
                </div>
            }
            .into_any();
        };
        let tabs = response.results.len() + usize::from(response.error.is_some());
        let tab = sheet.tab.get().min(tabs.saturating_sub(1));
        let tab_bar = (0..tabs)
            .map(|i| {
                let (label, rows) = match response.results.get(i) {
                    Some(r) if r.kind == "rows" => (format!("Result {}", i + 1), Some(r.row_count)),
                    Some(_) => (format!("Statement {}", i + 1), None),
                    None => {
                        let cancelled = response.error.as_ref().is_some_and(|e| e.cancelled);
                        (
                            (if cancelled { "Cancelled" } else { "Error" }).to_string(),
                            None,
                        )
                    }
                };
                let is_error = i >= response.results.len();
                view! {
                    <button
                        class="tabs__item"
                        class:is-active=i == tab
                        class:is-error=is_error
                        role="tab"
                        aria-selected=(i == tab).to_string()
                        on:click=move |_| {
                            sheet.tab.set(i);
                            sheet.cell.set(None);
                        }
                    >
                        {is_error.then(|| view! { <Icon name="alert-circle" size=14 /> })}
                        {label}
                        {rows.map(|n| view! { <span class="tabs__count">{count(n)}</span> })}
                    </button>
                }
            })
            .collect_view();
        let selected = response.results.get(tab).cloned();
        let views = selected.as_ref().filter(|r| r.kind == "rows").map(|r| {
            let explain = is_explain(r);
            let button = move |label: &'static str, icon: &'static str, target: ResultView| {
                view! {
                    <button
                        class="seg__item"
                        class:is-active=move || sheet.view.get() == target
                        on:click=move |_| sheet.view.set(target)
                    >
                        <Icon name=icon size=14 />
                        {label}
                    </button>
                }
            };
            view! {
                <div class="seg" aria-label="Result view">
                    {if explain {
                        button("Plan", "plan", ResultView::Grid)
                    } else {
                        button("Grid", "grid", ResultView::Grid)
                    }}
                    {button("Chart", "chart", ResultView::Chart)}
                    {button("Profile", "gauge", ResultView::Profile)}
                </div>
            }
        });
        let content = match selected {
            Some(result) => view! { <ResultBody sheet result /> }.into_any(),
            None => {
                let error = response.error.clone();
                view! {
                    <div class="results__error">
                        {error
                            .map(|e| {
                                let (tone, title) = if e.cancelled {
                                    (Tone::Warning, format!("Statement {} was cancelled", e.index + 1))
                                } else {
                                    (Tone::Danger, format!("Statement {} failed", e.index + 1))
                                };
                                view! {
                                    <Callout tone title>
                                        <div class="code--wrap">{e.message}</div>
                                        <pre class="code code--wrap">{e.statement}</pre>
                                    </Callout>
                                }
                            })}
                    </div>
                }
                .into_any()
            }
        };
        let _ = app;
        view! {
            <div class="results__head">
                <div class="tabs" role="tablist">{tab_bar}</div>
                {views}
            </div>
            {content}
        }
        .into_any()
    };
    view! { <section class="results" aria-label="Results">{body}</section> }
}

#[component]
fn ResultBody(sheet: Sheet, result: StatementResult) -> impl IntoView {
    let app = expect_context::<App>();
    if result.kind != "rows" {
        let message = result.message.clone();
        let total = millis(result.profile.total_ms);
        return view! {
            <div class="results__ok">
                <Callout tone=Tone::Success title=message>
                    <span class="muted">{total}</span>
                    <pre class="code code--wrap">{result.statement.clone()}</pre>
                </Callout>
            </div>
        }
        .into_any();
    }
    let explain = is_explain(&result);
    let rows_text = if result.truncated {
        view! {
            <span>
                "Showing " <strong>{count(result.rows.len() as u64)}</strong> " of "
                <strong>{count(result.row_count)}</strong> " rows"
            </span>
        }
        .into_any()
    } else {
        view! { <span><strong>{count(result.row_count)}</strong> " rows"</span> }.into_any()
    };
    let csv_result = result.clone();
    let json_result = result.clone();
    let columns = result.columns.len();
    let main = {
        let result = result.clone();
        move || match sheet.view.get() {
            ResultView::Grid if explain => {
                view! { <ExplainTree result=result.clone() /> }.into_any()
            }
            ResultView::Grid => view! { <Grid sheet result=result.clone() /> }.into_any(),
            ResultView::Chart => view! { <Chart result=result.clone() /> }.into_any(),
            ResultView::Profile => view! { <ProfileView result=result.clone() /> }.into_any(),
        }
    };
    view! {
        <div class="results__bar">
            <span class="status status--success">"Succeeded"</span>
            <span class="results__dot">"·"</span>
            {rows_text}
            <span class="results__dot">"·"</span>
            <span>{millis(result.profile.total_ms)}</span>
            <span class="results__dot">"·"</span>
            <span>{format!("{columns} column{}", if columns == 1 { "" } else { "s" })}</span>
            {result.truncated.then(|| view! { <span class="badge badge--warning" title="Downloads fetch every row">"Truncated"</span> })}
            <span class="spacer"></span>
            <button class="btn btn--ghost btn--sm" on:click=move |_| download(app, csv_result.clone(), "csv")>
                <Icon name="download" size=14 />
                "CSV"
            </button>
            <button class="btn btn--ghost btn--sm" on:click=move |_| download(app, json_result.clone(), "json")>
                <Icon name="download" size=14 />
                "JSON"
            </button>
        </div>
        <div class="results__body">{main}</div>
    }
    .into_any()
}

#[component]
fn Grid(sheet: Sheet, result: StatementResult) -> impl IntoView {
    let scroller: NodeRef<html::Div> = NodeRef::new();
    let top = RwSignal::new(0.0f64);
    let height = RwSignal::new(400.0f64);
    let widths = widths(&result);
    let total_width: f64 = widths.iter().sum::<f64>() + RN_WIDTH;
    let numeric: Vec<bool> = result.columns.iter().map(|c| numeric(&c.dtype)).collect();
    let boolean: Vec<bool> = result
        .columns
        .iter()
        .map(|c| c.dtype.eq_ignore_ascii_case("BOOLEAN"))
        .collect();
    let rows = StoredValue::new(result.rows.clone());
    let row_count = result.rows.len();
    let on_scroll = move |_| {
        if let Some(el) = scroller.get_untracked() {
            top.set(el.scroll_top() as f64);
            height.set(el.client_height() as f64);
        }
    };
    Effect::new(move |_| {
        if let Some(el) = scroller.get() {
            height.set(el.client_height() as f64);
        }
    });
    let header = result
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let w = widths[i];
            view! {
                <div class="vgrid__th" class:is-num=numeric[i] style:width=format!("{w}px") title=c.name.clone()>
                    <span class="grid__name">{c.name.clone()}</span>
                    <span class="grid__type">{c.dtype.clone()}</span>
                </div>
            }
        })
        .collect_view();
    let body_widths = widths.clone();
    let visible = move || {
        let first = ((top.get() / ROW_HEIGHT) as usize).saturating_sub(OVERSCAN);
        let shown = (height.get() / ROW_HEIGHT) as usize + 2 * OVERSCAN;
        let last = (first + shown).min(row_count);
        let selected = sheet.cell.get();
        rows.with_value(|rows| {
            (first..last)
                .map(|r| {
                    let cells = rows[r]
                        .iter()
                        .enumerate()
                        .map(|(c, cell)| {
                            let w = body_widths.get(c).copied().unwrap_or(80.0);
                            let is_null = cell.is_none();
                            let is_bool = boolean.get(c).copied().unwrap_or(false);
                            let text = cell.clone().unwrap_or_else(|| "NULL".to_string());
                            let truth = is_bool && text == "true";
                            let falsity = is_bool && text == "false";
                            view! {
                                <div
                                    class="vgrid__td"
                                    class:is-num=numeric.get(c).copied().unwrap_or(false)
                                    class:is-null=is_null
                                    class:is-true=truth
                                    class:is-false=falsity
                                    class:is-selected=selected == Some((r, c))
                                    style:width=format!("{w}px")
                                    on:click=move |_| sheet.cell.set(Some((r, c)))
                                >
                                    {text}
                                </div>
                            }
                        })
                        .collect_view();
                    view! {
                        <div class="vgrid__row" style:top=format!("{}px", r as f64 * ROW_HEIGHT)>
                            <div class="vgrid__rn">{r + 1}</div>
                            {cells}
                        </div>
                    }
                })
                .collect_view()
        })
    };
    let viewer = move || {
        let (r, c) = sheet.cell.get()?;
        let text = rows.with_value(|rows| rows.get(r).and_then(|row| row.get(c)).cloned())?;
        let column = result.columns.get(c)?;
        let name = column.name.clone();
        let dtype = column.dtype.to_ascii_lowercase();
        let shown = text.as_deref().map_or_else(|| "NULL".to_string(), pretty);
        Some(view! {
            <aside class="viewer" aria-label="Cell">
                <div class="panel-head">
                    <span class="panel-head__title">
                        <span class="mono">{name}</span>
                        <span class="subtle mono viewer__type">{dtype}</span>
                    </span>
                    <span class="subtle text-sm">{format!("Row {}", r + 1)}</span>
                    <button class="btn btn--ghost btn--icon btn--sm" aria-label="Close" on:click=move |_| sheet.cell.set(None)>
                        <Icon name="x" size=14 />
                    </button>
                </div>
                <pre class="code" class:is-null=text.is_none()>{shown}</pre>
            </aside>
        })
    };
    view! {
        <div class="vgrid" node_ref=scroller on:scroll=on_scroll role="grid" aria-rowcount=row_count>
            <div class="vgrid__head" style:width=format!("{total_width}px")>
                <div class="vgrid__rn">"#"</div>
                {header}
            </div>
            <div
                class="vgrid__body"
                style:height=format!("{}px", row_count as f64 * ROW_HEIGHT)
                style:width=format!("{total_width}px")
            >
                {visible}
            </div>
        </div>
        {viewer}
    }
}

/// `EXPLAIN` output as an indented tree.
#[component]
fn ExplainTree(result: StatementResult) -> impl IntoView {
    let lines = result
        .rows
        .iter()
        .filter_map(|row| row.first().cloned().flatten())
        .map(|line| {
            let depth = line.chars().take_while(|c| *c == ' ').count() / 2;
            let text = line.trim_start().to_string();
            let (op, rest) = match text.find([' ', '[']) {
                Some(i) => (text[..i].to_string(), text[i..].to_string()),
                None => (text.clone(), String::new()),
            };
            view! {
                <div class="plan__node" style:padding-left=format!("{}px", depth * 20)>
                    {(depth > 0).then(|| view! { <span class="plan__branch">"└"</span> })}
                    <span class="plan__op">{op}</span>
                    <span class="plan__args">{rest}</span>
                </div>
            }
        })
        .collect_view();
    view! { <div class="plan">{lines}</div> }
}

/// The wave-84 profile as bars.
#[component]
fn ProfileView(result: StatementResult) -> impl IntoView {
    let p = result.profile.clone();
    let total = p.total_ms.max(0.001);
    let phases = [
        ("Parse", p.parse_ms),
        ("Bind", p.bind_ms),
        ("Optimize", p.optimize_ms),
        ("Execute", p.execute_ms),
        ("Log", p.log_ms),
    ];
    let rows = phases
        .into_iter()
        .map(|(name, ms)| {
            let pct = (ms / total * 100.0).clamp(0.0, 100.0);
            view! {
                <span class="profile__phase">{name}</span>
                <div class="bar"><div class="bar__fill" style:width=format!("{pct:.1}%")></div></div>
                <span class="profile__ms">{millis(ms)}</span>
            }
        })
        .collect_view();
    view! {
        <div class="profile">
            {rows}
            <span class="profile__phase profile__total">"Total"</span>
            <span class="profile__total muted text-sm">
                {format!("{} · {} rows", p.kind, count(p.rows))}
            </span>
            <span class="profile__ms profile__total">{millis(p.total_ms)}</span>
        </div>
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pinkdb_server_ui_api::{Profile, ResultColumn};

    fn result() -> StatementResult {
        StatementResult {
            statement: "SELECT".into(),
            kind: "rows".into(),
            message: String::new(),
            columns: vec![
                ResultColumn {
                    name: "n".into(),
                    dtype: "BIGINT".into(),
                },
                ResultColumn {
                    name: "s,x".into(),
                    dtype: "VARCHAR".into(),
                },
            ],
            rows: vec![
                vec![Some("1".into()), Some("a\"b".into())],
                vec![None, None],
            ],
            row_count: 2,
            truncated: false,
            profile: Profile::default(),
        }
    }

    #[test]
    fn csv_and_json_downloads() {
        assert_eq!(csv(&result()), "n,\"s,x\"\r\n1,\"a\"\"b\"\r\n,\r\n");
        let parsed: serde_json::Value = serde_json::from_str(&json(&result())).unwrap();
        assert_eq!(parsed[0]["n"], serde_json::json!(1));
        assert_eq!(parsed[1]["s,x"], serde_json::Value::Null);
    }

    #[test]
    fn json_cells_are_pretty_printed() {
        assert_eq!(pretty("{\"a\":1}"), "{\n  \"a\": 1\n}");
        assert_eq!(pretty("{not json"), "{not json");
        assert_eq!(pretty("plain"), "plain");
    }
}
