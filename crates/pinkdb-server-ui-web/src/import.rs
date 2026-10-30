//! The Import dialog: a file from a path or URL (`https://`, `s3://`, a server
//! path), or an upload from this browser, into a table.
//!
//! A path or URL import is plain SQL run as the session user (the engine
//! keeps file and URL reads for administrators); an upload goes to
//! `POST /api/upload`, where the server parses the file and inserts the rows
//! as the session user.

use leptos::html;
use leptos::prelude::*;
use leptos::task::spawn_local;
use pinkdb_server_ui_api::sql::{quote_ident, quote_string};
use pinkdb_server_ui_api::{QueryRequest, QueryResponse, StatementResult, Storage, UploadResult};
use wasm_bindgen::JsCast;

use crate::api;
use crate::state::{App, encode};

/// The reader function for a source, from its extension or the choice.
pub fn reader_for(source: &str, choice: &str) -> &'static str {
    let lower = source.to_ascii_lowercase();
    let lower = lower.split(['?', '#']).next().unwrap_or_default();
    match choice {
        "csv" => "read_csv",
        "parquet" => "read_parquet",
        _ if lower.ends_with(".parquet") || lower.ends_with(".pq") || lower.ends_with('/') => {
            "read_parquet"
        }
        _ => "read_csv",
    }
}

/// A table name from a source (`…/orders_2024.csv` → `orders_2024`).
pub fn table_from(source: &str) -> String {
    let path = source.split(['?', '#']).next().unwrap_or_default();
    let file = path.rsplit(['/', '\\']).next().unwrap_or_default();
    let stem = file.split('.').next().unwrap_or_default();
    let mut name: String = stem
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    if name.is_empty() || name.starts_with(|c: char| c.is_ascii_digit()) {
        name.insert_str(0, "t_");
    }
    name
}

/// The statements that load `source` into `db.table`.
pub fn import_sql(source: &str, reader: &str, db: &str, table: &str, mode: &str) -> String {
    let target = format!("{}.{}", quote_ident(db), quote_ident(table));
    let select = format!("SELECT * FROM {reader}({})", quote_string(source));
    match mode {
        "append" => format!("INSERT INTO {target} {select};"),
        "replace" => format!("DROP TABLE IF EXISTS {target};\nCREATE TABLE {target} AS {select};"),
        _ => format!("CREATE TABLE {target} AS {select};"),
    }
}

#[component]
pub fn ImportDialog() -> impl IntoView {
    let app = expect_context::<App>();
    let tab = RwSignal::new("url");
    let source = RwSignal::new(String::new());
    let format = RwSignal::new("auto".to_string());
    let table = RwSignal::new(String::new());
    let mode = RwSignal::new("create".to_string());
    let database = RwSignal::new(app.db.get_untracked());
    let preview = RwSignal::new(None::<StatementResult>);
    let busy = RwSignal::new(false);
    let message = RwSignal::new(None::<Result<String, String>>);
    let file_input: NodeRef<html::Input> = NodeRef::new();
    let file_name = RwSignal::new(String::new());
    // A durable store logs a path or URL import as the statement, which a
    // replay would answer by reading the source again: a checkpoint right
    // after makes the table independent of the source.
    let durable = RwSignal::new(false);
    spawn_local(async move {
        if let Ok(storage) = api::get::<Storage>("/api/storage").await {
            durable.set(storage.mode == "durable");
        }
    });

    let close = move || app.import_open.set(false);
    let finished = move |outcome: Result<String, String>| {
        busy.set(false);
        let ok = outcome.is_ok();
        message.set(Some(outcome));
        if ok {
            app.load_catalog();
        }
    };

    let do_preview = move |_| {
        let src = source.get_untracked();
        if src.trim().is_empty() {
            return;
        }
        let reader = reader_for(&src, &format.get_untracked());
        let request = QueryRequest {
            sql: format!(
                "SELECT * FROM {reader}({}) LIMIT 50",
                quote_string(src.trim())
            ),
            db: Some(app.db.get_untracked()),
            run_id: api::run_id(),
            worksheet_id: String::new(),
        };
        busy.set(true);
        message.set(None);
        if table.get_untracked().is_empty() {
            table.set(table_from(&src));
        }
        let csrf = app.csrf();
        spawn_local(async move {
            match api::post::<_, QueryResponse>("/api/query", &request, &csrf).await {
                Ok(response) => {
                    busy.set(false);
                    match response.error {
                        Some(e) => message.set(Some(Err(e.message))),
                        None => preview.set(response.results.into_iter().next()),
                    }
                }
                Err(e) => finished(Err(e.message)),
            }
        });
    };
    let do_import_url = move |_| {
        let src = source.get_untracked();
        let name = table.get_untracked();
        if src.trim().is_empty() || name.trim().is_empty() {
            message.set(Some(
                Err("a source and a table name are needed".to_string()),
            ));
            return;
        }
        let reader = reader_for(&src, &format.get_untracked());
        let mut sql = import_sql(
            src.trim(),
            reader,
            &database.get_untracked(),
            name.trim(),
            &mode.get_untracked(),
        );
        if durable.get_untracked() {
            sql.push_str("\nCHECKPOINT;");
        }
        busy.set(true);
        app.run_statement(sql, finished);
    };
    let do_upload = move |_| {
        let Some(input) = file_input.get_untracked() else {
            return;
        };
        let Some(file) = input.files().and_then(|files| files.get(0)) else {
            message.set(Some(Err("choose a file".to_string())));
            return;
        };
        let name = table.get_untracked();
        let name = if name.trim().is_empty() {
            table_from(&file.name())
        } else {
            name
        };
        let fmt = format.get_untracked();
        let path = format!(
            "/api/upload?db={}&table={}&mode={}&name={}{}",
            encode(&database.get_untracked()),
            encode(name.trim()),
            encode(&mode.get_untracked()),
            encode(&file.name()),
            if fmt == "auto" {
                String::new()
            } else {
                format!("&format={}", encode(&fmt))
            }
        );
        busy.set(true);
        message.set(None);
        let csrf = app.csrf();
        spawn_local(async move {
            let outcome = api::post_file::<UploadResult>(&path, &file, &csrf)
                .await
                .map(|r| {
                    format!(
                        "Imported {} rows into {}.{} ({} columns) in {:.0} ms",
                        r.rows,
                        r.database,
                        r.table,
                        r.columns.len(),
                        r.elapsed_ms
                    )
                })
                .map_err(|e| e.message);
            finished(outcome);
        });
    };

    let preview_view = move || {
        preview.get().map(|result| {
            let header = result
                .columns
                .iter()
                .map(|c| view! { <th>{c.name.clone()}<span class="type">{c.dtype.clone()}</span></th> })
                .collect_view();
            let rows = result
                .rows
                .iter()
                .take(20)
                .map(|row| {
                    let cells = row
                        .iter()
                        .map(|cell| match cell {
                            Some(text) => view! { <td>{text.clone()}</td> }.into_any(),
                            None => view! { <td class="null">"NULL"</td> }.into_any(),
                        })
                        .collect_view();
                    view! { <tr>{cells}</tr> }
                })
                .collect_view();
            view! {
                <div class="preview-scroll">
                    <table class="preview-table">
                        <tr>{header}</tr>
                        {rows}
                    </table>
                </div>
                <div class="hint">{format!("{} rows previewed", result.row_count)}</div>
            }
        })
    };

    let databases = move || {
        let current = database.get();
        app.catalog
            .get()
            .databases
            .into_iter()
            .map(|db| {
                let selected = db.eq_ignore_ascii_case(&current);
                let value = db.clone();
                view! { <option value=value selected=selected>{db}</option> }
            })
            .collect_view()
    };

    view! {
        <div
            class="modal-backdrop"
            tabindex="-1"
            on:click=move |_| close()
            on:keydown=move |ev: web_sys::KeyboardEvent| {
                if ev.key() == "Escape" {
                    close();
                }
            }
        >
            <div class="modal" on:click=|ev| ev.stop_propagation()>
                <div class="modal-head">
                    <span>"Import data"</span>
                    <button class="mini" on:click=move |_| close()>"×"</button>
                </div>
                <div class="segs">
                    <button class="seg" class:active=move || tab.get() == "url" on:click=move |_| tab.set("url")>
                        "From a path or URL"
                    </button>
                    <button class="seg" class:active=move || tab.get() == "upload" on:click=move |_| tab.set("upload")>
                        "Upload a file"
                    </button>
                </div>
                {move || {
                    if tab.get() == "url" {
                        view! {
                            <label class="field">
                                "Source"
                                <input
                                    class="wide-input"
                                    placeholder="https://…/data.csv, s3://bucket/key.parquet or a server path"
                                    prop:value=move || source.get()
                                    on:input=move |ev| source.set(event_target_value(&ev))
                                />
                            </label>
                            <p class="hint">
                                "The server reads the source as you: in a console with logins, file and URL reads need ADMIN."
                            </p>
                        }
                            .into_any()
                    } else {
                        view! {
                            <label class="field">
                                "File (CSV or Parquet)"
                                <input
                                    type="file"
                                    accept=".csv,.tsv,.txt,.parquet"
                                    node_ref=file_input
                                    on:change=move |ev| {
                                        let name = ev
                                            .target()
                                            .and_then(|t| t.dyn_into::<web_sys::HtmlInputElement>().ok())
                                            .and_then(|i| i.files())
                                            .and_then(|f| f.get(0))
                                            .map(|f| f.name())
                                            .unwrap_or_default();
                                        if table.get_untracked().is_empty() {
                                            table.set(table_from(&name));
                                        }
                                        file_name.set(name);
                                    }
                                />
                            </label>
                            <p class="hint">
                                "The server parses the file and inserts its rows as you (CREATE and INSERT grants apply)."
                            </p>
                        }
                            .into_any()
                    }
                }}
                <div class="form-row">
                    <label class="field">
                        "Format"
                        <select on:change=move |ev| format.set(event_target_value(&ev))>
                            <option value="auto">"from the name"</option>
                            <option value="csv">"CSV"</option>
                            <option value="parquet">"Parquet"</option>
                        </select>
                    </label>
                    <label class="field">
                        "Database"
                        <select on:change=move |ev| database.set(event_target_value(&ev))>{databases}</select>
                    </label>
                    <label class="field">
                        "Table"
                        <input prop:value=move || table.get() on:input=move |ev| table.set(event_target_value(&ev)) />
                    </label>
                    <label class="field">
                        "If it exists"
                        <select on:change=move |ev| mode.set(event_target_value(&ev))>
                            <option value="create">"fail (create)"</option>
                            <option value="replace">"replace"</option>
                            <option value="append">"append"</option>
                        </select>
                    </label>
                </div>
                <div class="modal-actions">
                    {move || {
                        if tab.get() == "url" {
                            view! {
                                <button disabled=move || busy.get() on:click=do_preview>"Preview"</button>
                                <button class="primary" disabled=move || busy.get() on:click=do_import_url>"Import"</button>
                            }
                                .into_any()
                        } else {
                            view! {
                                <button class="primary" disabled=move || busy.get() on:click=do_upload>"Upload and import"</button>
                            }
                                .into_any()
                        }
                    }}
                    {move || busy.get().then(|| view! { <span class="spinner"></span> })}
                </div>
                {move || {
                    message
                        .get()
                        .map(|m| match m {
                            Ok(text) => view! { <div class="ok-line">{text}</div> }.into_any(),
                            Err(text) => view! { <div class="error-box">{text}</div> }.into_any(),
                        })
                }}
                {preview_view}
            </div>
        </div>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readers_and_names_come_from_the_source() {
        assert_eq!(
            reader_for("https://x/y/data.parquet?sig=1", "auto"),
            "read_parquet"
        );
        assert_eq!(reader_for("s3://b/k.csv", "auto"), "read_csv");
        assert_eq!(reader_for("s3://b/k.csv", "parquet"), "read_parquet");
        assert_eq!(table_from("https://x/orders-2024.csv"), "orders_2024");
        assert_eq!(table_from("2024.csv"), "t_2024");
    }

    #[test]
    fn import_statements_quote_everything() {
        assert_eq!(
            import_sql("s3://b/o'k.csv", "read_csv", "main", "Orders", "create"),
            "CREATE TABLE main.\"Orders\" AS SELECT * FROM read_csv('s3://b/o''k.csv');"
        );
        assert!(
            import_sql("x.csv", "read_csv", "main", "t", "replace")
                .starts_with("DROP TABLE IF EXISTS main.t;")
        );
        assert!(
            import_sql("x.csv", "read_csv", "main", "t", "append")
                .starts_with("INSERT INTO main.t SELECT")
        );
    }
}
