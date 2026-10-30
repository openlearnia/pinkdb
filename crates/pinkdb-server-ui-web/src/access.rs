//! The Access section: users, roles and grants.
//!
//! Every change is a statement run as the session user, so the engine's RBAC
//! decides; the forms only build the SQL (names quoted). A new user's
//! password goes through its own endpoint, never a worksheet run.

use leptos::prelude::*;
use leptos::task::spawn_local;
use pinkdb_server_ui_api::sql::quote_ident;
use pinkdb_server_ui_api::{Access, GrantRow, UserRequest};

use crate::api;
use crate::icon::Icon;
use crate::state::{App, Tone};
use crate::ui::{Callout, Dialog, Empty};

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

/// The `GRANT` statement the grant form builds.
pub fn grant_form_sql(privilege: &str, target: &str, grantee: &str, current: &str) -> String {
    let to = quote_ident(grantee.trim());
    let target = target.trim();
    match privilege {
        "ADMIN" => format!("GRANT ADMIN TO {to}"),
        "ROLE" => format!("GRANT {} TO {to}", quote_ident(target)),
        "CREATE" => format!(
            "GRANT CREATE ON DATABASE {} TO {to}",
            quote_ident(if target.is_empty() { current } else { target })
        ),
        privilege => {
            let (db, table) = split_object(target, current);
            format!(
                "GRANT {privilege} ON TABLE {}.{} TO {to}",
                quote_ident(&db),
                quote_ident(&table)
            )
        }
    }
}

/// The badge tone of a privilege.
fn privilege_tone(privilege: &str) -> &'static str {
    match privilege {
        "SELECT" => "badge badge--info mono",
        "INSERT" | "UPDATE" | "DELETE" | "ALL" => "badge badge--warning mono",
        "CREATE" => "badge badge--success mono",
        "ADMIN" => "badge badge--danger mono",
        _ => "badge badge--accent mono",
    }
}

/// Which dialog is open.
#[derive(Clone, PartialEq)]
enum Open {
    NewUser,
    NewRole,
    Grant,
    Password(String),
}

#[component]
pub fn AccessPage() -> impl IntoView {
    let app = expect_context::<App>();
    let access = RwSignal::new(None::<Access>);
    let failure = RwSignal::new(None::<String>);
    let dialog = RwSignal::new(None::<Open>);
    let filter = RwSignal::new(String::new());
    Effect::new(move |_| {
        app.access_epoch.track();
        spawn_local(async move {
            match api::get::<Access>("/api/access").await {
                Ok(a) => {
                    access.set(Some(a));
                    failure.set(None);
                }
                Err(e) => failure.set(Some(e.message)),
            }
        });
    });
    // Run a statement; report it as a toast and reload the lists.
    let run = move |sql: String, done: &'static str| {
        app.run_statement(sql, move |outcome| {
            match outcome {
                Ok(_) => app.toast(Tone::Success, done, ""),
                Err(e) => app.toast(Tone::Danger, "The change was refused", e),
            }
            app.access_epoch.update(|e| *e += 1);
        });
    };

    let principals = move || {
        let a = access.get()?;
        let admin = a.admin;
        let needle = filter.get().to_ascii_lowercase();
        let grants_of = |name: &str| {
            a.grants
                .iter()
                .filter(|g| g.principal.eq_ignore_ascii_case(name))
                .count()
        };
        let rows = a
            .principals
            .iter()
            .filter(|p| needle.is_empty() || p.name.to_ascii_lowercase().contains(&needle))
            .map(|p| {
                let role = p.kind == "ROLE";
                let drop_sql = format!("DROP {} {}", if role { "ROLE" } else { "USER" }, quote_ident(&p.name));
                let me = !role && p.name.eq_ignore_ascii_case(&a.me);
                let name = p.name.clone();
                let reset_name = p.name.clone();
                let n = grants_of(&p.name);
                let memberships = p
                    .roles
                    .split(',')
                    .map(str::trim)
                    .filter(|r| !r.is_empty())
                    .map(|r| view! { <span class="badge mono">{r.to_string()}</span> " " })
                    .collect_view();
                view! {
                    <tr>
                        <td>
                            <span class="row">
                                <Icon name=if role { "users" } else { "user" } class="subtle" />
                                <span class="mono">{p.name.clone()}</span>
                                {me.then(|| view! { <span class="badge badge--outline">"You"</span> })}
                            </span>
                        </td>
                        <td class="muted">{if role { "Role" } else { "User" }}</td>
                        <td>
                            {if p.roles.trim().is_empty() {
                                view! { <span class="subtle">"—"</span> }.into_any()
                            } else {
                                memberships.into_any()
                            }}
                        </td>
                        <td>
                            {if p.is_admin {
                                view! { <span class="badge badge--accent">"Admin"</span> }.into_any()
                            } else {
                                view! { <span class="muted">{format!("{n} grant{}", if n == 1 { "" } else { "s" })}</span> }.into_any()
                            }}
                        </td>
                        <td class="t-actions">
                            {(admin && !role)
                                .then(|| {
                                    view! {
                                        <button
                                            class="btn btn--ghost btn--sm"
                                            on:click=move |_| dialog.set(Some(Open::Password(reset_name.clone())))
                                        >
                                            <Icon name="key" size=14 />
                                            "Reset password"
                                        </button>
                                    }
                                })}
                            {(admin && !me)
                                .then(|| {
                                    view! {
                                        <button
                                            class="btn btn--ghost btn--sm btn--danger"
                                            on:click=move |_| {
                                                let sql = drop_sql.clone();
                                                let what = if role { "role" } else { "user" };
                                                app.ask_first(
                                                    format!("Drop {what} {name}?"),
                                                    format!("{name} loses its grants at once. This cannot be undone."),
                                                    sql.clone(),
                                                    if role { "Drop role" } else { "Drop user" },
                                                    move |_| run(sql.clone(), "Dropped"),
                                                );
                                            }
                                        >
                                            <Icon name="trash" size=14 />
                                            "Drop"
                                        </button>
                                    }
                                })}
                        </td>
                    </tr>
                }
            })
            .collect_view();
        let n = a.principals.len();
        Some(view! {
            <div class="card card--flush">
                <div class="card__head">
                    <span class="card__title">{if admin { "Users and roles" } else { "You and your roles" }}</span>
                    <span class="badge">{n}</span>
                    <span class="spacer"></span>
                    <label class="search search--sm">
                        <Icon name="search" size=14 />
                        <input
                            class="input input--sm"
                            type="search"
                            placeholder="Filter"
                            aria-label="Filter users and roles"
                            prop:value=move || filter.get()
                            on:input=move |ev| filter.set(event_target_value(&ev))
                        />
                    </label>
                </div>
                {if a.principals.is_empty() {
                    view! {
                        <Empty
                            icon="users"
                            title="No users or roles yet"
                            body="Without users the console needs no sign-in. Create one to require it."
                        />
                    }
                    .into_any()
                } else {
                view! {
                <table class="table">
                    <thead>
                        <tr>
                            <th>"Name"</th>
                            <th class="col-100">"Kind"</th>
                            <th>"Member of"</th>
                            <th class="col-130">"Privileges"</th>
                            <th class="t-actions"></th>
                        </tr>
                    </thead>
                    <tbody>{rows}</tbody>
                </table>
                }
                .into_any()
                }}
            </div>
        })
    };

    let grants = move || {
        let a = access.get()?;
        let admin = a.admin;
        let rows = a
            .grants
            .iter()
            .map(|g| {
                let (kind, target) = match g.object_type.as_str() {
                    "GLOBAL" => (String::new(), "Everything".to_string()),
                    "ROLE" => ("Role".to_string(), g.name.clone().unwrap_or_default()),
                    "DATABASE" => ("Database".to_string(), g.database.clone().unwrap_or_default()),
                    _ => (
                        "Table".to_string(),
                        format!("{}.{}", g.database.clone().unwrap_or_default(), g.name.clone().unwrap_or_default()),
                    ),
                };
                let revoke = grant_sql(true, g);
                let who = g.principal.clone();
                let what = g.privilege.clone();
                view! {
                    <tr>
                        <td class="mono">{g.principal.clone()}</td>
                        <td><span class=privilege_tone(&g.privilege)>{g.privilege.clone()}</span></td>
                        <td>
                            {if kind.is_empty() {
                                view! { <span class="muted">{target}</span> }.into_any()
                            } else {
                                view! { <span class="muted">{kind}</span> " " <span class="mono">{target}</span> }.into_any()
                            }}
                        </td>
                        <td class="t-actions">
                            {admin
                                .then(|| {
                                    view! {
                                        <button
                                            class="btn btn--ghost btn--sm btn--danger"
                                            on:click=move |_| {
                                                let sql = revoke.clone();
                                                app.ask_first(
                                                    format!("Revoke {what} from {who}?"),
                                                    "The privilege is gone for every session at once.",
                                                    sql.clone(),
                                                    "Revoke",
                                                    move |_| run(sql.clone(), "Revoked"),
                                                );
                                            }
                                        >
                                            "Revoke"
                                        </button>
                                    }
                                })}
                        </td>
                    </tr>
                }
            })
            .collect_view();
        let n = a.grants.len();
        Some(view! {
            <div class="card card--flush">
                <div class="card__head">
                    <span class="card__title">"Grants"</span>
                    <span class="badge">{n}</span>
                    <span class="spacer"></span>
                    {admin
                        .then(|| {
                            view! {
                                <button class="btn btn--sm" on:click=move |_| dialog.set(Some(Open::Grant))>
                                    <Icon name="shield" size=14 />
                                    "Grant"
                                </button>
                            }
                        })}
                </div>
                {if a.grants.is_empty() {
                    view! { <Empty icon="shield" title="No grants" body="Administrators can do everything; others need grants." /> }.into_any()
                } else {
                    view! {
                        <table class="table">
                            <thead>
                                <tr>
                                    <th>"Grantee"</th>
                                    <th class="col-140">"Privilege"</th>
                                    <th>"On"</th>
                                    <th class="t-actions"></th>
                                </tr>
                            </thead>
                            <tbody>{rows}</tbody>
                        </table>
                    }
                        .into_any()
                }}
            </div>
        })
    };

    let admin = move || access.get().is_some_and(|a| a.admin);
    view! {
        <div class="page">
            <div class="page__inner">
                <div class="page-head">
                    <div class="page-head__text">
                        <h1 class="page-head__title">"Access"</h1>
                        <p class="page-head__desc">
                            "Users, roles and what they may read and write. Every change runs as SQL under your session."
                        </p>
                    </div>
                    <div class="page-head__actions">
                        {move || {
                            admin()
                                .then(|| {
                                    view! {
                                        <button class="btn" on:click=move |_| dialog.set(Some(Open::NewRole))>
                                            <Icon name="plus" size=14 />
                                            "New role"
                                        </button>
                                        <button class="btn btn--primary" on:click=move |_| dialog.set(Some(Open::NewUser))>
                                            <Icon name="plus" size=14 />
                                            "New user"
                                        </button>
                                    }
                                })
                        }}
                    </div>
                </div>
                {move || failure.get().map(|f| view! { <Callout tone=Tone::Danger title="Could not read the access catalog">{f}</Callout> })}
                {move || {
                    (access.get().is_none() && failure.get().is_none())
                        .then(|| view! { <div class="empty"><span class="spinner"></span></div> })
                }}
                {principals}
                {grants}
            </div>
        </div>
        {move || {
            dialog
                .get()
                .map(|open| {
                    let close = Callback::new(move |_: ()| dialog.set(None));
                    match open {
                        Open::NewUser => view! { <UserDialog name=None close /> }.into_any(),
                        Open::Password(name) => view! { <UserDialog name=Some(name) close /> }.into_any(),
                        Open::NewRole => view! { <RoleDialog close run /> }.into_any(),
                        Open::Grant => view! { <GrantDialog close run /> }.into_any(),
                    }
                })
        }}
    }
}

/// Create a user, or set a user's password (`name` given). The password
/// goes through its own endpoint, never a worksheet run.
#[component]
fn UserDialog(name: Option<String>, close: Callback<()>) -> impl IntoView {
    let app = expect_context::<App>();
    let alter = name.is_some();
    let user = RwSignal::new(name.clone().unwrap_or_default());
    let password = RwSignal::new(String::new());
    let error = RwSignal::new(None::<String>);
    let busy = RwSignal::new(false);
    let submit = move |ev: leptos::ev::SubmitEvent| {
        ev.prevent_default();
        let request = UserRequest {
            name: user.get_untracked().trim().to_string(),
            password: password.get_untracked(),
            alter,
        };
        if request.name.is_empty() || request.password.is_empty() {
            error.set(Some("A name and a password are needed.".to_string()));
            return;
        }
        busy.set(true);
        let csrf = app.csrf();
        spawn_local(async move {
            let who = request.name.clone();
            match api::post::<_, serde_json::Value>("/api/access/user", &request, &csrf).await {
                Ok(_) => {
                    password.set(String::new());
                    app.toast(
                        Tone::Success,
                        if alter {
                            "Password changed"
                        } else {
                            "User created"
                        },
                        who,
                    );
                    app.access_epoch.update(|e| *e += 1);
                    close.run(());
                }
                Err(e) => {
                    busy.set(false);
                    error.set(Some(e.message));
                }
            }
        });
    };
    let title = match &name {
        Some(n) => format!("Reset the password of {n}"),
        None => "New user".to_string(),
    };
    let no_logins = move || app.session.get().is_some_and(|s| s.auth == "none");
    view! {
        <Dialog title size="sm" on_close=close>
            <form on:submit=submit>
                <div class="dialog__body">
                    {move || {
                        (!alter && no_logins())
                            .then(|| {
                                view! {
                                    <Callout tone=Tone::Warning title="This console runs without sign-in">
                                        "The store has no users yet. Creating the first one ends that: restart the server with "
                                        <span class="mono">"--ui-auth basic"</span>
                                        " and sign in as that user."
                                    </Callout>
                                }
                            })
                    }}
                    {(!alter)
                        .then(|| {
                            view! {
                                <label class="field">
                                    <span class="field__label">"Name"</span>
                                    <input
                                        class="input mono"
                                        autocomplete="off"
                                        prop:value=move || user.get()
                                        on:input=move |ev| user.set(event_target_value(&ev))
                                    />
                                </label>
                            }
                        })}
                    <label class="field">
                        <span class="field__label">{if alter { "New password" } else { "Password" }}</span>
                        <input
                            class="input"
                            type="password"
                            autocomplete="new-password"
                            prop:value=move || password.get()
                            on:input=move |ev| password.set(event_target_value(&ev))
                        />
                    </label>
                    {move || error.get().map(|e| view! { <Callout tone=Tone::Danger>{e}</Callout> })}
                </div>
                <div class="dialog__foot">
                    <button class="btn" type="button" on:click=move |_| close.run(())>"Cancel"</button>
                    <button class="btn btn--primary" type="submit" disabled=move || busy.get()>
                        {if alter { "Set password" } else { "Create user" }}
                    </button>
                </div>
            </form>
        </Dialog>
    }
}

#[component]
fn RoleDialog(
    close: Callback<()>,
    run: impl Fn(String, &'static str) + Copy + Send + Sync + 'static,
) -> impl IntoView {
    let name = RwSignal::new(String::new());
    let sql = move || format!("CREATE ROLE {}", quote_ident(name.get().trim()));
    let submit = move |ev: leptos::ev::SubmitEvent| {
        ev.prevent_default();
        if name.get_untracked().trim().is_empty() {
            return;
        }
        run(sql(), "Role created");
        close.run(());
    };
    view! {
        <Dialog title="New role" size="sm" on_close=close>
            <form on:submit=submit>
                <div class="dialog__body">
                    <label class="field">
                        <span class="field__label">"Name"</span>
                        <input
                            class="input mono"
                            autocomplete="off"
                            prop:value=move || name.get()
                            on:input=move |ev| name.set(event_target_value(&ev))
                        />
                        <span class="field__help">"Grant privileges to the role, then make users members of it."</span>
                    </label>
                    <pre class="code">{sql}</pre>
                </div>
                <div class="dialog__foot">
                    <button class="btn" type="button" on:click=move |_| close.run(())>"Cancel"</button>
                    <button class="btn btn--primary" type="submit">"Create role"</button>
                </div>
            </form>
        </Dialog>
    }
}

#[component]
fn GrantDialog(
    close: Callback<()>,
    run: impl Fn(String, &'static str) + Copy + Send + Sync + 'static,
) -> impl IntoView {
    let app = expect_context::<App>();
    let privilege = RwSignal::new("SELECT".to_string());
    let target = RwSignal::new(String::new());
    let grantee = RwSignal::new(String::new());
    let sql = move || {
        grant_form_sql(
            &privilege.get(),
            &target.get(),
            &grantee.get(),
            &app.db.get(),
        )
    };
    let submit = move |ev: leptos::ev::SubmitEvent| {
        ev.prevent_default();
        if grantee.get_untracked().trim().is_empty() {
            return;
        }
        run(sql(), "Granted");
        close.run(());
    };
    let target_label = move || match privilege.get().as_str() {
        "ROLE" => "Role",
        "CREATE" => "Database",
        "ADMIN" => "",
        _ => "Table",
    };
    view! {
        <Dialog title="Grant a privilege" on_close=close>
            <form on:submit=submit>
                <div class="dialog__body">
                    <label class="field">
                        <span class="field__label">"Privilege"</span>
                        <select class="input" on:change=move |ev| privilege.set(event_target_value(&ev))>
                            <option value="SELECT">"SELECT on a table"</option>
                            <option value="INSERT">"INSERT on a table"</option>
                            <option value="UPDATE">"UPDATE on a table"</option>
                            <option value="DELETE">"DELETE on a table"</option>
                            <option value="ALL">"ALL on a table"</option>
                            <option value="CREATE">"CREATE on a database"</option>
                            <option value="ROLE">"Membership of a role"</option>
                            <option value="ADMIN">"ADMIN (everything)"</option>
                        </select>
                    </label>
                    <div class="fields">
                        {move || {
                            let label = target_label();
                            (!label.is_empty())
                                .then(|| {
                                    view! {
                                        <label class="field">
                                            <span class="field__label">{label}</span>
                                            <input
                                                class="input mono"
                                                placeholder=move || match privilege.get().as_str() {
                                                    "ROLE" => "analyst".to_string(),
                                                    "CREATE" => app.db.get(),
                                                    _ => "db.table or table".to_string(),
                                                }
                                                prop:value=move || target.get()
                                                on:input=move |ev| target.set(event_target_value(&ev))
                                            />
                                        </label>
                                    }
                                })
                        }}
                        <label class="field">
                            <span class="field__label">"To user or role"</span>
                            <input
                                class="input mono"
                                prop:value=move || grantee.get()
                                on:input=move |ev| grantee.set(event_target_value(&ev))
                            />
                        </label>
                    </div>
                    <pre class="code code--wrap">{sql}</pre>
                </div>
                <div class="dialog__foot">
                    <button class="btn" type="button" on:click=move |_| close.run(())>"Cancel"</button>
                    <button class="btn btn--primary" type="submit">
                        <Icon name="shield" size=14 />
                        "Grant"
                    </button>
                </div>
            </form>
        </Dialog>
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
        assert_eq!(
            grant_form_sql("SELECT", "sales.orders", "Ana", "main"),
            "GRANT SELECT ON TABLE sales.orders TO \"Ana\""
        );
        assert_eq!(
            grant_form_sql("CREATE", "", "bob", "main"),
            "GRANT CREATE ON DATABASE main TO bob"
        );
        assert_eq!(
            grant_form_sql("ADMIN", "x", "bob", "main"),
            "GRANT ADMIN TO bob"
        );
    }
}
