//! The Access panel (users, roles, grants) and the Storage panel.
//!
//! Every change is a statement run as the session user, so the engine's RBAC
//! decides; the forms only build the SQL (names quoted). A new user's
//! password goes through its own endpoint, never a worksheet run.

use leptos::prelude::*;
use leptos::task::spawn_local;
use pinkdb_server_ui_api::sql::quote_ident;
use pinkdb_server_ui_api::{Access, GrantRow, Storage, UserRequest};

use crate::api;
use crate::state::App;

/// `GRANT`/`REVOKE` text for a grant row (the inverse of `pinkdb_grants()`).
pub fn grant_sql(revoke: bool, g: &GrantRow) -> String {
    let (verb, prep) = if revoke {
        ("REVOKE", "FROM")
    } else {
        ("GRANT", "TO")
    };
    let to = quote_ident(&g.principal);
    match g.object_type.as_str() {
        "GLOBAL" => format!("{verb} ADMIN {prep} {to}"),
        "ROLE" => format!(
            "{verb} {} {prep} {to}",
            quote_ident(g.name.as_deref().unwrap_or_default())
        ),
        "DATABASE" => format!(
            "{verb} {} ON DATABASE {} {prep} {to}",
            g.privilege,
            quote_ident(g.database.as_deref().unwrap_or_default())
        ),
        _ => format!(
            "{verb} {} ON TABLE {}.{} {prep} {to}",
            g.privilege,
            quote_ident(g.database.as_deref().unwrap_or_default()),
            quote_ident(g.name.as_deref().unwrap_or_default())
        ),
    }
}

/// Split `db.table` (or `table`, in `current`) for a grant form.
fn split_object(text: &str, current: &str) -> (String, String) {
    match text.trim().split_once('.') {
        Some((db, table)) => (db.trim().to_string(), table.trim().to_string()),
        None => (current.to_string(), text.trim().to_string()),
    }
}

#[component]
pub fn AccessPanel() -> impl IntoView {
    let app = expect_context::<App>();
    let access = RwSignal::new(None::<Access>);
    let message = RwSignal::new(None::<Result<String, String>>);
    Effect::new(move |_| {
        app.access_epoch.track();
        spawn_local(async move {
            match api::get::<Access>("/api/access").await {
                Ok(a) => access.set(Some(a)),
                Err(e) => message.set(Some(Err(e.message))),
            }
        });
    });
    let run = move |sql: String| {
        app.run_statement(sql, move |outcome| {
            message.set(Some(outcome));
            app.access_epoch.update(|e| *e += 1);
        });
    };

    // Form state.
    let new_user = RwSignal::new(String::new());
    let new_password = RwSignal::new(String::new());
    let new_role = RwSignal::new(String::new());
    let privilege = RwSignal::new("SELECT".to_string());
    let object = RwSignal::new(String::new());
    let grantee = RwSignal::new(String::new());

    let create_user = move |_| {
        let request = UserRequest {
            name: new_user.get_untracked().trim().to_string(),
            password: new_password.get_untracked(),
            alter: false,
        };
        new_password.set(String::new());
        let csrf = app.csrf();
        spawn_local(async move {
            let outcome = api::post::<_, serde_json::Value>("/api/access/user", &request, &csrf)
                .await
                .map(|v| v["message"].as_str().unwrap_or("done").to_string())
                .map_err(|e| e.message);
            message.set(Some(outcome));
            app.access_epoch.update(|e| *e += 1);
        });
        new_user.set(String::new());
    };
    let grant = move |_| {
        let what = privilege.get_untracked();
        let to = quote_ident(grantee.get_untracked().trim());
        let target = object.get_untracked();
        let current = app.db.get_untracked();
        let sql = match what.as_str() {
            "ADMIN" => format!("GRANT ADMIN TO {to}"),
            "ROLE" => format!("GRANT {} TO {to}", quote_ident(target.trim())),
            "CREATE" => format!(
                "GRANT CREATE ON DATABASE {} TO {to}",
                quote_ident(if target.trim().is_empty() {
                    &current
                } else {
                    target.trim()
                })
            ),
            privilege => {
                let (db, table) = split_object(&target, &current);
                format!(
                    "GRANT {privilege} ON TABLE {}.{} TO {to}",
                    quote_ident(&db),
                    quote_ident(&table)
                )
            }
        };
        run(sql);
    };

    let lists = move || {
        let a = access.get()?;
        let admin = a.admin;
        let principals = a
            .principals
            .iter()
            .map(|p| {
                let drop_sql = format!(
                    "DROP {} {}",
                    if p.kind == "ROLE" { "ROLE" } else { "USER" },
                    quote_ident(&p.name)
                );
                let me = p.kind == "USER" && p.name.eq_ignore_ascii_case(&a.me);
                view! {
                    <li class="principal">
                        <span class="pname">{p.name.clone()}</span>
                        <span class="pkind">{p.kind.to_ascii_lowercase()}</span>
                        {p.is_admin.then(|| view! { <span class="badge">"admin"</span> })}
                        {(!p.roles.is_empty()).then(|| view! { <span class="hint">{format!("in {}", p.roles)}</span> })}
                        {me.then(|| view! { <span class="hint">"(you)"</span> })}
                        {(admin && !me)
                            .then(|| {
                                view! {
                                    <button class="mini danger" title=drop_sql.clone() on:click=move |_| run(drop_sql.clone())>
                                        "drop"
                                    </button>
                                }
                            })}
                    </li>
                }
            })
            .collect_view();
        let grants = a
            .grants
            .iter()
            .map(|g| {
                let object = match g.object_type.as_str() {
                    "GLOBAL" => "everything".to_string(),
                    "ROLE" => format!("role {}", g.name.clone().unwrap_or_default()),
                    "DATABASE" => format!("database {}", g.database.clone().unwrap_or_default()),
                    _ => format!(
                        "{}.{}",
                        g.database.clone().unwrap_or_default(),
                        g.name.clone().unwrap_or_default()
                    ),
                };
                let revoke = grant_sql(true, g);
                view! {
                    <tr>
                        <td>{g.principal.clone()}</td>
                        <td class="priv">{g.privilege.clone()}</td>
                        <td>{object}</td>
                        <td>
                            {admin
                                .then(|| {
                                    view! {
                                        <button class="mini danger" title=revoke.clone() on:click=move |_| run(revoke.clone())>
                                            "revoke"
                                        </button>
                                    }
                                })}
                        </td>
                    </tr>
                }
            })
            .collect_view();
        Some(view! {
            <div class="group-label">{if admin { "Principals" } else { "You and your roles" }}</div>
            <ul class="principals">{principals}</ul>
            <div class="group-label">"Grants"</div>
            <table class="grants">
                <tr>
                    <th>"to"</th>
                    <th>"privilege"</th>
                    <th>"on"</th>
                    <th></th>
                </tr>
                {grants}
            </table>
            {a.grants.is_empty().then(|| view! { <p class="empty">"No grants."</p> })}
        })
    };

    let forms = move || {
        access.get().filter(|a| a.admin).map(|_| {
            view! {
                <div class="group-label">"New user"</div>
                {move || {
                    app.session
                        .get()
                        .filter(|s| s.auth == "none")
                        .map(|_| {
                            view! {
                                <p class="warn">
                                    "This console runs without logins because the store has no users. \
                                     Creating the first user ends it: restart the server with \
                                     --ui-auth basic and log in as that user."
                                </p>
                            }
                        })
                }}
                <div class="form-row">
                    <input placeholder="name" prop:value=move || new_user.get() on:input=move |ev| new_user.set(event_target_value(&ev)) />
                    <input type="password" placeholder="password" autocomplete="new-password" prop:value=move || new_password.get() on:input=move |ev| new_password.set(event_target_value(&ev)) />
                    <button on:click=create_user>"Create"</button>
                </div>
                <div class="group-label">"New role"</div>
                <div class="form-row">
                    <input placeholder="role name" prop:value=move || new_role.get() on:input=move |ev| new_role.set(event_target_value(&ev)) />
                    <button on:click=move |_| {
                        let name = new_role.get_untracked();
                        new_role.set(String::new());
                        run(format!("CREATE ROLE {}", quote_ident(name.trim())));
                    }>"Create"</button>
                </div>
                <div class="group-label">"Grant"</div>
                <div class="form-col">
                    <select on:change=move |ev| privilege.set(event_target_value(&ev))>
                        <option value="SELECT">"SELECT on a table"</option>
                        <option value="INSERT">"INSERT on a table"</option>
                        <option value="UPDATE">"UPDATE on a table"</option>
                        <option value="DELETE">"DELETE on a table"</option>
                        <option value="ALL">"ALL on a table"</option>
                        <option value="CREATE">"CREATE on a database"</option>
                        <option value="ROLE">"membership of a role"</option>
                        <option value="ADMIN">"ADMIN (everything)"</option>
                    </select>
                    <input
                        placeholder=move || match privilege.get().as_str() {
                            "ROLE" => "role",
                            "CREATE" => "database",
                            "ADMIN" => "(nothing)",
                            _ => "db.table or table",
                        }
                        prop:value=move || object.get()
                        on:input=move |ev| object.set(event_target_value(&ev))
                    />
                    <input placeholder="to user or role" prop:value=move || grantee.get() on:input=move |ev| grantee.set(event_target_value(&ev)) />
                    <button on:click=grant>"Grant"</button>
                </div>
            }
        })
    };

    view! {
        <div class="panel access">
            {move || {
                message
                    .get()
                    .map(|m| match m {
                        Ok(text) => view! { <div class="ok-line">{text}</div> }.into_any(),
                        Err(text) => view! { <div class="error-box">{text}</div> }.into_any(),
                    })
            }}
            {lists}
            {forms}
        </div>
    }
}

#[component]
pub fn StoragePanel() -> impl IntoView {
    let app = expect_context::<App>();
    let storage = RwSignal::new(None::<Storage>);
    let message = RwSignal::new(None::<Result<String, String>>);
    let load = move || {
        spawn_local(async move {
            match api::get::<Storage>("/api/storage").await {
                Ok(s) => storage.set(Some(s)),
                Err(e) => message.set(Some(Err(e.message))),
            }
        });
    };
    load();
    let body = move || {
        let s = storage.get()?;
        let facts = s
            .facts
            .iter()
            .map(|f| {
                view! {
                    <tr>
                        <td class="label">{f.label.clone()}</td>
                        <td>{f.value.clone()}</td>
                    </tr>
                }
            })
            .collect_view();
        let formats: Vec<String> = s
            .import_formats
            .iter()
            .map(|f| {
                f.replace("url:", "from a path or URL: ")
                    .replace("upload:", "upload: ")
            })
            .collect();
        let durable = s.mode != "in-memory";
        Some(view! {
            <table class="facts">
                <tr>
                    <td class="label">"mode"</td>
                    <td><strong>{s.mode.clone()}</strong></td>
                </tr>
                <tr>
                    <td class="label">"location"</td>
                    <td class="mono">{s.location.clone()}</td>
                </tr>
                {facts}
            </table>
            <div class="group-label">"Imports"</div>
            <ul class="formats">{formats.into_iter().map(|f| view! { <li>{f}</li> }).collect_view()}</ul>
            <button class="wide" on:click=move |_| app.import_open.set(true)>"Import data…"</button>
            {(durable && app.admin())
                .then(|| {
                    view! {
                        <button
                            class="wide"
                            title="Fold the WAL into the store file (an administrator action)"
                            on:click=move |_| {
                                app.run_statement("CHECKPOINT".to_string(), move |outcome| {
                                    message.set(Some(outcome));
                                    load();
                                });
                            }
                        >
                            "Checkpoint"
                        </button>
                    }
                })}
        })
    };
    view! {
        <div class="panel storage">
            {move || {
                message
                    .get()
                    .map(|m| match m {
                        Ok(text) => view! { <div class="ok-line">{text}</div> }.into_any(),
                        Err(text) => view! { <div class="error-box">{text}</div> }.into_any(),
                    })
            }}
            {body}
        </div>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(object_type: &str, privilege: &str, db: Option<&str>, name: Option<&str>) -> GrantRow {
        GrantRow {
            principal: "Bob".into(),
            privilege: privilege.into(),
            object_type: object_type.into(),
            database: db.map(str::to_string),
            name: name.map(str::to_string),
        }
    }

    #[test]
    fn grant_rows_invert_to_statements() {
        assert_eq!(
            grant_sql(true, &grant("TABLE", "SELECT", Some("main"), Some("t"))),
            "REVOKE SELECT ON TABLE main.t FROM \"Bob\""
        );
        assert_eq!(
            grant_sql(false, &grant("DATABASE", "CREATE", Some("main"), None)),
            "GRANT CREATE ON DATABASE main TO \"Bob\""
        );
        assert_eq!(
            grant_sql(true, &grant("GLOBAL", "ADMIN", None, None)),
            "REVOKE ADMIN FROM \"Bob\""
        );
        assert_eq!(
            grant_sql(true, &grant("ROLE", "MEMBER", None, Some("analyst"))),
            "REVOKE analyst FROM \"Bob\""
        );
        assert_eq!(
            split_object("sales.orders", "main"),
            ("sales".into(), "orders".into())
        );
        assert_eq!(
            split_object("orders", "main"),
            ("main".into(), "orders".into())
        );
    }
}
