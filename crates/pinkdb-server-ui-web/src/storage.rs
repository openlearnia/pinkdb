//! The Storage section: where the warehouse keeps its data, what it can
//! import, and the checkpoint.

use leptos::prelude::*;

use crate::icon::Icon;
use crate::state::{App, Tone, count, is_relation};
use crate::ui::{Callout, Empty};

/// A fact label in sentence case (`lake version` → `Lake version`).
fn label(text: &str) -> String {
    let mut chars = text.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

/// The import formats by where they come from: `(upload, url)`.
pub fn formats(list: &[String]) -> (Vec<String>, Vec<String>) {
    let pick = |prefix: &str| -> Vec<String> {
        list.iter()
            .filter_map(|f| f.strip_prefix(prefix))
            .map(|f| match f {
                "csv" => "CSV".to_string(),
                "parquet" => "Parquet".to_string(),
                other => other.to_string(),
            })
            .collect()
    };
    (pick("upload:"), pick("url:"))
}

#[component]
pub fn StoragePage() -> impl IntoView {
    let app = expect_context::<App>();
    let busy = RwSignal::new(false);
    let checkpoint = move |_| {
        busy.set(true);
        app.run_statement("CHECKPOINT".to_string(), move |outcome| {
            busy.set(false);
            match outcome {
                Ok(_) => app.toast(
                    Tone::Success,
                    "Checkpoint complete",
                    "The WAL is folded into the store file.",
                ),
                Err(e) => app.toast(Tone::Danger, "Checkpoint failed", e),
            }
            app.load_storage();
        });
    };
    let body = move || {
        let Some(s) = app.storage.get() else {
            return view! { <div class="empty"><span class="spinner"></span></div> }.into_any();
        };
        let (dbs, objects, rows) = app.catalog.with(|c| {
            let rows: i64 = c
                .objects
                .iter()
                .filter(|o| o.kind == "TABLE" || o.kind == "LAKE TABLE")
                .filter_map(|o| o.rows)
                .sum();
            let relations = c.objects.iter().filter(|o| is_relation(&o.kind)).count();
            (c.databases.len(), relations, rows)
        });
        let mode = label(&s.mode);
        let mode_caption = match s.mode.as_str() {
            "durable" => "a .pink store file and its WAL",
            "lake" => "a catalog and data files",
            "in-memory" => "gone when the server stops",
            _ => "",
        };
        let facts = s
            .facts
            .iter()
            .map(|f| {
                view! {
                    <dt>{label(&f.label)}</dt>
                    <dd class="mono">{f.value.clone()}</dd>
                }
            })
            .collect_view();
        let (upload, url) = formats(&s.import_formats);
        let badges = |list: Vec<String>| {
            list.into_iter()
                .map(|f| view! { <span class="badge">{f}</span> })
                .collect_view()
        };
        let logins = app.session.get().is_some_and(|s| s.auth != "none");
        view! {
            <div class="metrics">
                <div class="card metric">
                    <span class="overline">"Mode"</span>
                    <span class="metric__value">{mode}</span>
                    <span class="metric__caption">{mode_caption}</span>
                </div>
                <div class="card metric">
                    <span class="overline">"Databases"</span>
                    <span class="metric__value">{dbs}</span>
                    <span class="metric__caption">{format!("{objects} tables and views")}</span>
                </div>
                <div class="card metric">
                    <span class="overline">"Rows"</span>
                    <span class="metric__value">{count(rows.max(0) as u64)}</span>
                    <span class="metric__caption">"across the tables"</span>
                </div>
            </div>
            <div class="cards-2">
                <div class="card">
                    <div class="card__head">
                        <span class="card__title">"Details"</span>
                    </div>
                    <div class="card__body card__body--kv">
                        <dl class="kv">
                            <dt>"Location"</dt>
                            <dd class="mono">{s.location.clone()}</dd>
                            {facts}
                        </dl>
                    </div>
                </div>
                <div class="card">
                    <div class="card__head">
                        <span class="card__title">"Import formats"</span>
                    </div>
                    <div class="card__body stack stack--sm">
                        {if upload.is_empty() && url.is_empty() {
                            view! { <Empty icon="upload" title="No import formats" /> }.into_any()
                        } else {
                            view! {
                                <ul class="list-plain">
                                    {(!upload.is_empty())
                                        .then(|| {
                                            view! {
                                                <li class="row">
                                                    {badges(upload.clone())}
                                                    <span class="muted">"upload from this browser"</span>
                                                </li>
                                            }
                                        })}
                                    {(!url.is_empty())
                                        .then(|| {
                                            view! {
                                                <li class="row">
                                                    {badges(url.clone())}
                                                    <span class="muted">"from a path or URL"</span>
                                                </li>
                                            }
                                        })}
                                </ul>
                            }
                                .into_any()
                        }}
                        {(logins && !url.is_empty())
                            .then(|| {
                                view! {
                                    <Callout tone=Tone::Info>
                                        "Path and URL reads ("
                                        <span class="mono">"https://"</span>
                                        ", "
                                        <span class="mono">"s3://"</span>
                                        ", server paths) need ADMIN in a console with sign-in."
                                    </Callout>
                                }
                            })}
                    </div>
                </div>
            </div>
        }
        .into_any()
    };
    let can_checkpoint =
        move || app.admin() && app.storage.get().is_some_and(|s| s.mode == "durable");
    view! {
        <div class="page">
            <div class="page__inner">
                <div class="page-head">
                    <div class="page-head__text">
                        <h1 class="page-head__title">"Storage"</h1>
                        <p class="page-head__desc">"Where this warehouse keeps its data, and how to load more."</p>
                    </div>
                    <div class="page-head__actions">
                        {move || {
                            can_checkpoint()
                                .then(|| {
                                    view! {
                                        <button
                                            class="btn"
                                            disabled=move || busy.get()
                                            title="Fold the WAL into the store file"
                                            on:click=checkpoint
                                        >
                                            {move || {
                                                if busy.get() {
                                                    view! { <span class="spinner"></span> }.into_any()
                                                } else {
                                                    view! { <Icon name="refresh" size=14 /> }.into_any()
                                                }
                                            }}
                                            "Checkpoint"
                                        </button>
                                    }
                                })
                        }}
                        {move || {
                            (!app.read_only())
                                .then(|| {
                                    view! {
                                        <button class="btn btn--primary" on:click=move |_| app.import_open.set(true)>
                                            <Icon name="upload" size=14 />
                                            "Import data"
                                        </button>
                                    }
                                })
                        }}
                    </div>
                </div>
                {body}
            </div>
        </div>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_split_by_source() {
        let list = vec![
            "upload:csv".to_string(),
            "upload:parquet".to_string(),
            "url:csv".to_string(),
        ];
        assert_eq!(
            formats(&list),
            (
                vec!["CSV".to_string(), "Parquet".to_string()],
                vec!["CSV".to_string()]
            )
        );
        assert_eq!(label("lake version"), "Lake version");
    }
}
