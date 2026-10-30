//! The Storage section: where the warehouse keeps its data, what it can
//! import, and the checkpoint.

use leptos::prelude::*;
use pinkdb_server_ui_api::DatabaseInfo;
use pinkdb_server_ui_api::sql::{quote_ident, quote_string};

use crate::icon::Icon;
use crate::state::{App, Tone, count, is_relation};
use crate::ui::{Callout, Dialog, Empty};

/// `CREATE DATABASE` for the New database dialog (wave 124): a local
/// database, or a lake — managed under the server's data directory when no
/// catalog is given.
pub fn create_database_sql(name: &str, lake: bool, catalog: &str, data: &str) -> String {
    let mut sql = format!("CREATE DATABASE {}", quote_ident(name.trim()));
    if lake {
        let mut options = vec!["TYPE LAKE".to_string()];
        if !catalog.trim().is_empty() {
            options.push(format!("CATALOG {}", quote_string(catalog.trim())));
        }
        if !data.trim().is_empty() {
            options.push(format!("DATA {}", quote_string(data.trim())));
        }
        sql.push_str(&format!(" ({})", options.join(", ")));
    }
    sql
}

/// `ATTACH` for the Attach lake dialog (wave 124). The location is a lake
/// catalog (`sqlite:…`, `pink:…`, `postgres://…`); the `lake:` prefix is
/// added unless it is there.
pub fn attach_sql(location: &str, name: &str, read_only: bool, data: &str) -> String {
    let location = location.trim();
    let location = if location.starts_with("lake:") {
        location.to_string()
    } else {
        format!("lake:{location}")
    };
    let mut options = Vec::new();
    if read_only {
        options.push("READ_ONLY".to_string());
    }
    if !data.trim().is_empty() {
        options.push(format!("DATA {}", quote_string(data.trim())));
    }
    let mut sql = format!(
        "ATTACH {} AS {}",
        quote_string(&location),
        quote_ident(name.trim())
    );
    if !options.is_empty() {
        sql.push_str(&format!(" ({})", options.join(", ")));
    }
    sql
}

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
            "durable" => "the server's data directory",
            "in-memory" => "nothing is saved: gone when the server stops",
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
                    <span class="metric__caption">{format!("{objects} tables and materialized views")}</span>
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
    // Wave 124: databases are created, attached and detached here by
    // administrators (the statements are ADMIN-only anyway).
    let can_manage = move || app.admin() && !app.read_only();
    let dialog: RwSignal<Option<&'static str>> = RwSignal::new(None);
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
                            can_manage()
                                .then(|| {
                                    view! {
                                        <button class="btn" on:click=move |_| dialog.set(Some("attach"))>
                                            <Icon name="lake" size=14 />
                                            "Attach lake"
                                        </button>
                                        <button class="btn" on:click=move |_| dialog.set(Some("new"))>
                                            <Icon name="plus" size=14 />
                                            "New database"
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
                <DatabasesCard manage=Signal::derive(can_manage) />
            </div>
            {move || match dialog.get() {
                Some("new") => Some(view! { <NewDatabaseDialog open=dialog /> }.into_any()),
                Some("attach") => Some(view! { <AttachLakeDialog open=dialog /> }.into_any()),
                _ => None,
            }}
        </div>
    }
}

/// The databases and their storage (wave 124), with Detach for lakes.
#[component]
fn DatabasesCard(manage: Signal<bool>) -> impl IntoView {
    let app = expect_context::<App>();
    let detach = move |name: String| {
        let sql = format!("DETACH {}", quote_ident(&name));
        let title = format!("Detach {name}?");
        let shown = name.clone();
        app.ask_first(
            title,
            "The server forgets this lake database. The lake keeps every table and file; attach it again to use it.",
            sql.clone(),
            "Detach",
            move |_| {
                let shown = shown.clone();
                app.run_statement(sql.clone(), move |outcome| {
                    match outcome {
                        Ok(_) => app.toast(Tone::Success, "Detached", format!("{shown} is no longer attached.")),
                        Err(e) => app.toast(Tone::Danger, "Detach failed", e),
                    }
                    app.load_catalog();
                });
            },
        );
    };
    move || {
        let databases = app.catalog.with(|c| c.databases.clone());
        let rows = databases
            .into_iter()
            .map(|d: DatabaseInfo| {
                let (badge, tone) = if d.is_lake() {
                    (if d.read_only { "Lake · read-only" } else { "Lake" }, "badge badge--info")
                } else {
                    ("Local", "badge badge--outline")
                };
                let status = match d.unavailable() {
                    Some(reason) => view! { <span class="badge badge--danger" title=reason.to_string()>"Unavailable"</span> <span class="muted">{reason.to_string()}</span> }.into_any(),
                    None => view! { <span class="badge badge--success">"OK"</span> }.into_any(),
                };
                let name = d.name.clone();
                let lake = d.is_lake();
                view! {
                    <tr>
                        <td class="mono">{d.name.clone()}</td>
                        <td><span class=tone>{badge}</span></td>
                        <td class="mono t-clip" title=d.location.clone().unwrap_or_default()>{d.location.clone().unwrap_or_else(|| "this server's store".to_string())}</td>
                        <td class="t-num">{d.version.map(|v| v.to_string()).unwrap_or_default()}</td>
                        <td>{status}</td>
                        <td class="t-actions">
                            {(manage.get() && lake)
                                .then(|| {
                                    let name = name.clone();
                                    view! {
                                        <button class="btn btn--ghost btn--sm" title="Forget this lake (its data stays)" on:click=move |_| detach(name.clone())>
                                            "Detach"
                                        </button>
                                    }
                                })}
                        </td>
                    </tr>
                }
            })
            .collect_view();
        view! {
            <div class="card">
                <div class="card__head">
                    <span class="card__title">"Databases"</span>
                </div>
                <table class="table">
                    <thead>
                        <tr>
                            <th>"Name"</th>
                            <th class="col-130">"Storage"</th>
                            <th>"Location"</th>
                            <th class="col-100 t-num">"Version"</th>
                            <th class="col-130">"Status"</th>
                            <th class="t-actions"></th>
                        </tr>
                    </thead>
                    <tbody>{rows}</tbody>
                </table>
            </div>
        }
    }
}

/// Run a database statement from a dialog: on success, a toast, the catalog
/// reloaded and the dialog closed; on failure, the message in the dialog.
fn run_from_dialog(
    app: App,
    sql: String,
    done_title: &'static str,
    busy: RwSignal<bool>,
    error: RwSignal<Option<String>>,
    open: RwSignal<Option<&'static str>>,
) {
    busy.set(true);
    error.set(None);
    app.run_statement(sql, move |outcome| {
        busy.set(false);
        match outcome {
            Ok(message) => {
                app.toast(Tone::Success, done_title, message);
                open.set(None);
                app.load_catalog();
                app.load_storage();
            }
            Err(e) => error.set(Some(e)),
        }
    });
}

#[component]
fn NewDatabaseDialog(open: RwSignal<Option<&'static str>>) -> impl IntoView {
    let app = expect_context::<App>();
    let name = RwSignal::new(String::new());
    let storage = RwSignal::new("local".to_string());
    let catalog = RwSignal::new(String::new());
    let data = RwSignal::new(String::new());
    let busy = RwSignal::new(false);
    let error: RwSignal<Option<String>> = RwSignal::new(None);
    let sql = move || {
        create_database_sql(
            &name.get(),
            storage.get() == "lake",
            &catalog.get(),
            &data.get(),
        )
    };
    let create = move |_| {
        if name.get_untracked().trim().is_empty() {
            error.set(Some("Name the database.".to_string()));
            return;
        }
        run_from_dialog(app, sql(), "Database created", busy, error, open);
    };
    view! {
        <Dialog title="New database" on_close=Callback::new(move |_: ()| open.set(None))>
            <div class="dialog__body">
                <div class="fields">
                    <label class="field">
                        <span class="field__label">"Name"</span>
                        <input class="input mono" prop:value=move || name.get() on:input=move |ev| name.set(event_target_value(&ev)) />
                    </label>
                    <label class="field">
                        <span class="field__label">"Storage"</span>
                        <select class="input" on:change=move |ev| storage.set(event_target_value(&ev))>
                            <option value="local">"Local (this server's store)"</option>
                            <option value="lake">"Lake (its own catalog and data files)"</option>
                        </select>
                    </label>
                </div>
                {move || {
                    (storage.get() == "lake")
                        .then(|| {
                            view! {
                                <div class="fields">
                                    <label class="field">
                                        <span class="field__label">"Catalog (optional)"</span>
                                        <input class="input mono" placeholder="sqlite:sales.sqlite or postgres://host/db" prop:value=move || catalog.get() on:input=move |ev| catalog.set(event_target_value(&ev)) />
                                        <span class="field__help">"Empty: a lake under the server's data directory."</span>
                                    </label>
                                    <label class="field">
                                        <span class="field__label">"Data (optional)"</span>
                                        <input class="input mono" placeholder="a directory or s3://bucket/prefix" prop:value=move || data.get() on:input=move |ev| data.set(event_target_value(&ev)) />
                                        <span class="field__help">"Credentials come from the server's environment, never the URL."</span>
                                    </label>
                                </div>
                            }
                        })
                }}
                {move || error.get().map(|e| view! { <Callout tone=Tone::Danger title="Not created">{e}</Callout> })}
                <details class="sql-preview">
                    <summary>"SQL that will run"</summary>
                    <pre class="code code--wrap">{sql}</pre>
                </details>
            </div>
            <div class="dialog__foot">
                {move || busy.get().then(|| view! { <span class="spinner"></span> })}
                <button class="btn" on:click=move |_| open.set(None)>"Cancel"</button>
                <button class="btn btn--primary" disabled=move || busy.get() on:click=create>
                    <Icon name="plus" size=14 />
                    "Create"
                </button>
            </div>
        </Dialog>
    }
}

#[component]
fn AttachLakeDialog(open: RwSignal<Option<&'static str>>) -> impl IntoView {
    let app = expect_context::<App>();
    let location = RwSignal::new(String::new());
    let name = RwSignal::new(String::new());
    let access = RwSignal::new("read_only".to_string());
    let data = RwSignal::new(String::new());
    let busy = RwSignal::new(false);
    let error: RwSignal<Option<String>> = RwSignal::new(None);
    let sql = move || {
        attach_sql(
            &location.get(),
            &name.get(),
            access.get() == "read_only",
            &data.get(),
        )
    };
    let attach = move |_| {
        if location.get_untracked().trim().is_empty() || name.get_untracked().trim().is_empty() {
            error.set(Some(
                "Give the lake's catalog and a database name.".to_string(),
            ));
            return;
        }
        run_from_dialog(app, sql(), "Lake attached", busy, error, open);
    };
    view! {
        <Dialog title="Attach lake" on_close=Callback::new(move |_: ()| open.set(None))>
            <div class="dialog__body">
                <label class="field">
                    <span class="field__label">"Catalog"</span>
                    <input class="input input--block mono" placeholder="sqlite:/srv/archive.sqlite, pink:… or postgres://host/db" prop:value=move || location.get() on:input=move |ev| location.set(event_target_value(&ev)) />
                    <span class="field__help">"A local path must be in the server's lake directories; a password belongs in the server's environment, not here."</span>
                </label>
                <div class="fields">
                    <label class="field">
                        <span class="field__label">"Database name"</span>
                        <input class="input mono" prop:value=move || name.get() on:input=move |ev| name.set(event_target_value(&ev)) />
                    </label>
                    <label class="field">
                        <span class="field__label">"Access"</span>
                        <select class="input" on:change=move |ev| access.set(event_target_value(&ev))>
                            <option value="read_only">"Read-only"</option>
                            <option value="write">"Writable (one writer per lake)"</option>
                        </select>
                    </label>
                    <label class="field">
                        <span class="field__label">"Data (optional)"</span>
                        <input class="input mono" placeholder="the catalog's recorded location" prop:value=move || data.get() on:input=move |ev| data.set(event_target_value(&ev)) />
                    </label>
                </div>
                {move || error.get().map(|e| view! { <Callout tone=Tone::Danger title="Not attached">{e}</Callout> })}
                <details class="sql-preview">
                    <summary>"SQL that will run"</summary>
                    <pre class="code code--wrap">{sql}</pre>
                </details>
            </div>
            <div class="dialog__foot">
                {move || busy.get().then(|| view! { <span class="spinner"></span> })}
                <button class="btn" on:click=move |_| open.set(None)>"Cancel"</button>
                <button class="btn btn--primary" disabled=move || busy.get() on:click=attach>
                    <Icon name="lake" size=14 />
                    "Attach"
                </button>
            </div>
        </Dialog>
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

    #[test]
    fn database_statements_quote_their_parts() {
        assert_eq!(
            create_database_sql("sales", false, "x", "y"),
            "CREATE DATABASE sales"
        );
        assert_eq!(
            create_database_sql("Events", true, "", ""),
            "CREATE DATABASE \"Events\" (TYPE LAKE)"
        );
        assert_eq!(
            create_database_sql("ev", true, "sqlite:o'k.db", "s3://b/p"),
            "CREATE DATABASE ev (TYPE LAKE, CATALOG 'sqlite:o''k.db', DATA 's3://b/p')"
        );
        assert_eq!(
            attach_sql("sqlite:/a.db", "arch", true, ""),
            "ATTACH 'lake:sqlite:/a.db' AS arch (READ_ONLY)"
        );
        assert_eq!(
            attach_sql("lake:pink:/l.pink", "w", false, "/d"),
            "ATTACH 'lake:pink:/l.pink' AS w (DATA '/d')"
        );
    }
}
