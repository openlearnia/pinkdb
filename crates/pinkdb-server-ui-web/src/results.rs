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
use crate::state::{App, ResultView, Sheet, count, millis, numeric};

/// Grid metrics (they match `console.css`).
const ROW_HEIGHT: f64 = 24.0;
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
            (header.max(longest) as f64 * 7.6 + 20.0).clamp(64.0, 420.0)
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
        format: format.to_string(),
    };
    let csrf = app.csrf();
    app.notice.set("exporting…".to_string());
    spawn_local(async move {
        match api::post_raw("/api/export", &request, &csrf, None).await {
            Ok(response) => match response.blob() {
                Ok(promise) => {
                    if let Ok(blob) = wasm_bindgen_futures::JsFuture::from(promise).await {
                        api::download_blob(&format!("result.{format}"), &blob.into());
                        app.notice.set(String::new());
                    }
                }
                Err(_) => app.notice.set("export failed".to_string()),
            },
            Err(e) => app.notice.set(format!("export: {e}")),
        }
    });
}

#[component]
pub fn Results(sheet: Sheet) -> impl IntoView {
    let app = expect_context::<App>();
    let body = move || {
        if sheet.running.get().is_some() {
            return view! {
                <div class="running">
                    <span class="spinner"></span>
                    "Running… "
                    <button on:click=move |_| app.cancel(sheet)>"Cancel"</button>
                    <span class="hint">" (Esc)"</span>
                </div>
            }
            .into_any();
        }
        if let Some(error) = sheet.run_error.get() {
            return view! { <div class="error-box">{error}</div> }.into_any();
        }
        let Some(response) = sheet.result.get() else {
            return view! {
                <div class="empty">"Run a statement to see its result here."</div>
            }
            .into_any();
        };
        let tabs = response.results.len() + usize::from(response.error.is_some());
        let tab = sheet.tab.get().min(tabs.saturating_sub(1));
        let tab_bar = (0..tabs)
            .map(|i| {
                let label = match response.results.get(i) {
                    Some(r) if r.kind == "rows" => format!("Result {}", i + 1),
                    Some(_) => format!("Statement {}", i + 1),
                    None => "Error".to_string(),
                };
                let is_error = i >= response.results.len();
                view! {
                    <button
                        class="rtab"
                        class:active=i == tab
                        class:bad=is_error
                        on:click=move |_| {
                            sheet.tab.set(i);
                            sheet.cell.set(None);
                        }
                    >
                        {label}
                    </button>
                }
            })
            .collect_view();
        let content = match response.results.get(tab).cloned() {
            Some(result) => view! { <ResultBody sheet result /> }.into_any(),
            None => {
                let error = response.error.clone();
                view! {
                    <div class="error-box">
                        {error
                            .map(|e| {
                                view! {
                                    <div class="error-title">
                                        {if e.cancelled { "Cancelled" } else { "Error" }}
                                        {format!(" in statement {}", e.index + 1)}
                                    </div>
                                    <pre class="error-sql">{e.statement}</pre>
                                    <pre class="error-message">{e.message}</pre>
                                }
                            })}
                    </div>
                }
                .into_any()
            }
        };
        view! {
            <div class="rtabs">
                {tab_bar}
                <span class="elapsed">{format!("total {}", millis(response.elapsed_ms))}</span>
            </div>
            {content}
        }
        .into_any()
    };
    view! { <section class="results">{body}</section> }
}

#[component]
fn ResultBody(sheet: Sheet, result: StatementResult) -> impl IntoView {
    let app = expect_context::<App>();
    if result.kind != "rows" {
        let message = result.message.clone();
        let total = millis(result.profile.total_ms);
        return view! {
            <div class="ok-box">
                <div class="ok-message">{message}</div>
                <div class="hint">{total}</div>
                <pre class="ok-sql">{result.statement.clone()}</pre>
            </div>
        }
        .into_any();
    }
    let explain = result
        .statement
        .trim_start()
        .to_ascii_lowercase()
        .starts_with("explain")
        && result.columns.len() == 1;
    let info = if result.truncated {
        format!(
            "showing {} of {} rows · {}",
            count(result.rows.len() as u64),
            count(result.row_count),
            millis(result.profile.total_ms)
        )
    } else {
        format!(
            "{} rows · {}",
            count(result.row_count),
            millis(result.profile.total_ms)
        )
    };
    let csv_result = result.clone();
    let json_result = result.clone();
    let view_button = move |label: &'static str, target: ResultView| {
        view! {
            <button
                class="seg"
                class:active=move || sheet.view.get() == target
                on:click=move |_| sheet.view.set(target)
            >
                {label}
            </button>
        }
    };
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
        <div class="rbar">
            <span class="segs">
                {view_button(if explain { "Plan" } else { "Grid" }, ResultView::Grid)}
                {view_button("Chart", ResultView::Chart)}
                {view_button("Profile", ResultView::Profile)}
            </span>
            <span class="info">{info}</span>
            <button on:click=move |_| download(app, csv_result.clone(), "csv")>"CSV"</button>
            <button on:click=move |_| download(app, json_result.clone(), "json")>"JSON"</button>
        </div>
        {main}
    }
    .into_any()
}

#[component]
fn Grid(sheet: Sheet, result: StatementResult) -> impl IntoView {
    let scroller: NodeRef<html::Div> = NodeRef::new();
    let top = RwSignal::new(0.0f64);
    let height = RwSignal::new(400.0f64);
    let widths = widths(&result);
    let total_width: f64 = widths.iter().sum::<f64>() + 56.0;
    let numeric: Vec<bool> = result.columns.iter().map(|c| numeric(&c.dtype)).collect();
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
    let header_widths = widths.clone();
    let header = result
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let w = header_widths[i];
            view! {
                <div class="th" class:num=numeric[i] style:width=format!("{w}px")>
                    <span class="name">{c.name.clone()}</span>
                    <span class="type">{c.dtype.clone()}</span>
                </div>
            }
        })
        .collect_view();
    let body_widths = widths.clone();
    let body_numeric = numeric.clone();
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
                            let text = cell.clone().unwrap_or_else(|| "NULL".to_string());
                            view! {
                                <div
                                    class="td"
                                    class:num=body_numeric.get(c).copied().unwrap_or(false)
                                    class:null=is_null
                                    class:sel=selected == Some((r, c))
                                    style:width=format!("{w}px")
                                    on:click=move |_| sheet.cell.set(Some((r, c)))
                                >
                                    {text}
                                </div>
                            }
                        })
                        .collect_view();
                    view! {
                        <div class="tr" style:top=format!("{}px", r as f64 * ROW_HEIGHT)>
                            <div class="rn">{r + 1}</div>
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
        let column = result.columns.get(c)?.name.clone();
        let shown = text.as_deref().map_or_else(|| "NULL".to_string(), pretty);
        Some(view! {
            <div class="viewer">
                <div class="viewer-head">
                    <span>{format!("row {}, {}", r + 1, column)}</span>
                    <button on:click=move |_| sheet.cell.set(None)>"×"</button>
                </div>
                <pre>{shown}</pre>
            </div>
        })
    };
    view! {
        <div class="grid-wrap">
            <div class="grid" node_ref=scroller on:scroll=on_scroll>
                <div class="thead" style:width=format!("{total_width}px")>
                    <div class="rn">"#"</div>
                    {header}
                </div>
                <div
                    class="tbody"
                    style:height=format!("{}px", row_count as f64 * ROW_HEIGHT)
                    style:width=format!("{total_width}px")
                >
                    {visible}
                </div>
            </div>
            {viewer}
        </div>
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
                <div class="node" style:padding-left=format!("{}px", 12 + depth * 18)>
                    <span class="branch">{if depth > 0 { "└ " } else { "" }}</span>
                    <span class="op">{op}</span>
                    <span class="args">{rest}</span>
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
        ("parse", p.parse_ms),
        ("bind", p.bind_ms),
        ("optimize", p.optimize_ms),
        ("execute", p.execute_ms),
        ("log", p.log_ms),
    ];
    let rows = phases
        .into_iter()
        .map(|(name, ms)| {
            let pct = (ms / total * 100.0).clamp(0.0, 100.0);
            view! {
                <tr>
                    <td class="phase">{name}</td>
                    <td class="bar-cell">
                        <div class="bar" style:width=format!("{pct:.1}%")></div>
                    </td>
                    <td class="num">{millis(ms)}</td>
                </tr>
            }
        })
        .collect_view();
    view! {
        <div class="profile">
            <table>
                {rows}
                <tr class="total">
                    <td class="phase">"total"</td>
                    <td></td>
                    <td class="num">{millis(p.total_ms)}</td>
                </tr>
            </table>
            <div class="hint">{format!("{} · {} rows", p.kind, count(p.rows))}</div>
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
