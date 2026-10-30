//! The History section: every run from this console, filterable; a run
//! opens its SQL in a new worksheet.

use leptos::prelude::*;
use pinkdb_server_ui_api::HistoryEntry;

use crate::icon::Icon;
use crate::state::{App, Section, count, millis};
use crate::ui::Empty;

/// A run's status: its label and its tone.
pub fn status(status: &str) -> (&'static str, &'static str) {
    match status {
        "ok" => ("Succeeded", "success"),
        "error" => ("Failed", "danger"),
        "cancelled" => ("Cancelled", "warning"),
        "running" => ("Running", "info"),
        _ => ("Unknown", ""),
    }
}

/// The statement's first non-empty line.
fn first_line(sql: &str) -> String {
    sql.lines()
        .map(str::trim)
        .find(|l| !l.is_empty() && !l.starts_with("--"))
        .or_else(|| sql.lines().map(str::trim).find(|l| !l.is_empty()))
        .unwrap_or("")
        .to_string()
}

#[component]
pub fn HistoryPage() -> impl IntoView {
    let app = expect_context::<App>();
    let search = RwSignal::new(String::new());
    let only = RwSignal::new("");
    let open = move |entry: &HistoryEntry| {
        app.go(Section::Query);
        let title = app
            .saved
            .with_untracked(|s| {
                s.iter()
                    .find(|w| w.id == entry.worksheet_id)
                    .map(|w| w.title.clone())
            })
            .unwrap_or_else(|| "From history".to_string());
        app.open(&title, &entry.sql);
    };
    let filter = move |label: &'static str, value: &'static str| {
        view! {
            <button class="seg__item" class:is-active=move || only.get() == value on:click=move |_| only.set(value)>
                {label}
            </button>
        }
    };
    let table = move || {
        let needle = search.get().to_ascii_lowercase();
        let wanted = only.get();
        let all = app.history.get();
        let total = all.len();
        let entries: Vec<HistoryEntry> = all
            .into_iter()
            .filter(|e| wanted.is_empty() || e.status == wanted)
            .filter(|e| needle.is_empty() || e.sql.to_ascii_lowercase().contains(&needle))
            .collect();
        if entries.is_empty() {
            return if total == 0 {
                view! { <Empty icon="history" title="No runs yet" body="Statements you run in worksheets appear here." /> }
                    .into_any()
            } else {
                view! { <Empty icon="search" title="No runs match" body="Clear the search or the status filter." /> }
                    .into_any()
            };
        }
        let titles = app.saved.get();
        let rows = entries
            .into_iter()
            .map(|entry| {
                let (label, tone) = status(&entry.status);
                let sheet = titles
                    .iter()
                    .find(|w| w.id == entry.worksheet_id)
                    .map(|w| w.title.clone());
                let rows = if entry.status == "ok" {
                    count(entry.rows.max(0) as u64)
                } else {
                    "—".to_string()
                };
                let tip = if entry.error.is_empty() {
                    entry.sql.clone()
                } else {
                    format!("{}\n\n{}", entry.sql, entry.error)
                };
                let line = first_line(&entry.sql);
                let error = entry.error.clone();
                let for_row = entry.clone();
                let for_button = entry.clone();
                view! {
                    <tr class="is-clickable" title=tip on:click=move |_| open(&for_row)>
                        <td><span class=format!("status status--{tone}")>{label}</span></td>
                        <td class="t-ellipsis">
                            <div class="mono history__stmt">{line}</div>
                            {(!error.is_empty())
                                .then(|| {
                                    view! { <div class="history__error">{error}</div> }
                                })}
                        </td>
                        <td class="muted t-ellipsis">{sheet.unwrap_or_else(|| "—".to_string())}</td>
                        <td class="t-num">{rows}</td>
                        <td class="t-num">{millis(entry.elapsed_ms as f64)}</td>
                        <td class="muted mono history__time" title=entry.started_at.clone()>{entry.started_at.chars().take(19).collect::<String>()}</td>
                        <td class="t-actions">
                            <button class="btn btn--ghost btn--sm" on:click=move |ev| {
                                ev.stop_propagation();
                                open(&for_button);
                            }>
                                <Icon name="file-code" size=14 />
                                "Open"
                            </button>
                        </td>
                    </tr>
                }
            })
            .collect_view();
        view! {
            <table class="table table--fixed">
                <thead>
                    <tr>
                        <th class="col-120">"Status"</th>
                        <th>"Statement"</th>
                        <th class="col-180">"Worksheet"</th>
                        <th class="t-num col-90">"Rows"</th>
                        <th class="t-num col-90">"Duration"</th>
                        <th class="col-170">"Started"</th>
                        <th class="t-actions col-90"></th>
                    </tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
        }
        .into_any()
    };
    view! {
        <div class="page">
            <div class="page__inner page__inner--wide">
                <div class="page-head">
                    <div class="page-head__text">
                        <h1 class="page-head__title">"Query history"</h1>
                        <p class="page-head__desc">
                            "Every statement run from this console. Click a run to open its SQL in a new worksheet."
                        </p>
                    </div>
                    <div class="page-head__actions">
                        <button class="btn" on:click=move |_| app.load_history()>
                            <Icon name="refresh" size=14 />
                            "Refresh"
                        </button>
                    </div>
                </div>
                <div class="toolbar">
                    <label class="search search--md">
                        <Icon name="search" size=14 />
                        <input
                            class="input"
                            type="search"
                            placeholder="Search statements"
                            aria-label="Search statements"
                            prop:value=move || search.get()
                            on:input=move |ev| search.set(event_target_value(&ev))
                        />
                    </label>
                    <div class="seg" aria-label="Status">
                        {filter("All", "")}
                        {filter("Succeeded", "ok")}
                        {filter("Failed", "error")}
                        {filter("Cancelled", "cancelled")}
                    </div>
                </div>
                <div class="card card--flush">{table}</div>
            </div>
        </div>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_and_first_lines() {
        assert_eq!(status("ok"), ("Succeeded", "success"));
        assert_eq!(status("error").1, "danger");
        assert_eq!(first_line("-- note\n\n  SELECT 1;\n"), "SELECT 1;");
        assert_eq!(first_line("-- only a comment"), "-- only a comment");
    }
}
