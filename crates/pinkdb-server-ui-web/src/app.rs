//! The app shell: login, the top bar, the worksheet tabs and the layout.

use leptos::prelude::*;
use leptos::task::spawn_local;
use pinkdb_server_ui_api::{LoginRequest, LoginRequired, SessionInfo};

use crate::api;
use crate::ask::AskPanel;
use crate::editor::Editor;
use crate::results::Results;
use crate::sidebar::Sidebar;
use crate::state::{App, Sheet};

/// Start the app once a session exists.
fn start(app: App, session: SessionInfo) {
    app.session.set(Some(session));
    app.login_mode.set(None);
    app.load_catalog();
    app.load_worksheets(true);
}

#[component]
pub fn Root() -> impl IntoView {
    let app = App::new();
    provide_context(app);
    // Ask the server whether this browser has a session; a console without
    // authentication logs in by itself.
    spawn_local(async move {
        match api::send("GET", "/api/session", None, None, None).await {
            Ok(response) if response.ok() => {
                let body = api::text(&response).await.unwrap_or_default();
                if let Ok(session) = serde_json::from_str::<SessionInfo>(&body) {
                    start(app, session);
                }
            }
            Ok(response) => {
                let body = api::text(&response).await.unwrap_or_default();
                let mode = serde_json::from_str::<LoginRequired>(&body)
                    .map(|l| l.auth)
                    .unwrap_or_else(|_| "basic".to_string());
                if mode == "none" {
                    login(app, LoginRequest::default());
                } else {
                    app.login_mode.set(Some(mode));
                }
            }
            Err(e) => app.notice.set(format!("cannot reach the console: {e}")),
        }
    });
    view! {
        {move || {
            if app.session.get().is_some() {
                view! { <Workspace /> }.into_any()
            } else if app.login_mode.get().is_some() {
                view! { <Login /> }.into_any()
            } else {
                view! { <div class="splash">{move || {
                    let n = app.notice.get();
                    if n.is_empty() { "Loading…".to_string() } else { n }
                }}</div> }.into_any()
            }
        }}
    }
}

/// Log in and start (the CSRF token arrives with the session).
fn login(app: App, request: LoginRequest) {
    spawn_local(async move {
        match api::post::<_, SessionInfo>("/api/login", &request, "").await {
            Ok(session) => start(app, session),
            Err(e) => {
                app.notice.set(e.message);
                if app.login_mode.get_untracked().is_none() {
                    app.login_mode.set(Some("none".to_string()));
                }
            }
        }
    });
}

#[component]
fn Login() -> impl IntoView {
    let app = expect_context::<App>();
    let user = RwSignal::new(String::new());
    let secret = RwSignal::new(String::new());
    let mode = move || app.login_mode.get().unwrap_or_default();
    let submit = move |ev: leptos::ev::SubmitEvent| {
        ev.prevent_default();
        let request = if mode() == "token" {
            LoginRequest {
                token: Some(secret.get_untracked()),
                ..LoginRequest::default()
            }
        } else {
            LoginRequest {
                user: Some(user.get_untracked()),
                password: Some(secret.get_untracked()),
                ..LoginRequest::default()
            }
        };
        secret.set(String::new());
        app.notice.set(String::new());
        login(app, request);
    };
    view! {
        <div class="login">
            <form class="login-box" on:submit=submit>
                <h1>"pinkdb"</h1>
                <p class="hint">"warehouse console"</p>
                {move || {
                    (mode() == "basic")
                        .then(|| {
                            view! {
                                <label>
                                    "User"
                                    <input
                                        autocomplete="username"
                                        prop:value=move || user.get()
                                        on:input=move |ev| user.set(event_target_value(&ev))
                                    />
                                </label>
                            }
                        })
                }}
                <label>
                    {move || if mode() == "token" { "Console token" } else { "Password" }}
                    <input
                        type="password"
                        autocomplete="current-password"
                        prop:value=move || secret.get()
                        on:input=move |ev| secret.set(event_target_value(&ev))
                    />
                </label>
                <button class="primary" type="submit">"Log in"</button>
                <div class="error-text">{move || app.notice.get()}</div>
            </form>
        </div>
    }
}

#[component]
fn TopBar() -> impl IntoView {
    let app = expect_context::<App>();
    let logout = move |_| {
        let csrf = app.csrf();
        spawn_local(async move {
            let _ = api::post::<_, serde_json::Value>("/api/logout", &serde_json::json!({}), &csrf)
                .await;
            app.session.set(None);
            let _ = leptos::prelude::window().location().reload();
        });
    };
    let databases = move || {
        let current = app.db.get();
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
        <header class="topbar">
            <span class="brand">"pinkdb"</span>
            <label class="dbsel" title="The database statements run in">
                "database "
                <select on:change=move |ev| {
                    app.db.set(event_target_value(&ev));
                    app.load_catalog();
                }>{databases}</select>
            </label>
            {move || app.read_only().then(|| view! { <span class="badge">"read-only"</span> })}
            {move || {
                (!app.read_only())
                    .then(|| view! { <button on:click=move |_| app.import_open.set(true)>"Import"</button> })
            }}
            <span class="notice">{move || app.notice.get()}</span>
            <span class="spacer"></span>
            {move || {
                app.session
                    .get()
                    .filter(|s| s.agent)
                    .map(|_| {
                        view! {
                            <button
                                class:active=move || app.ask_open.get()
                                on:click=move |_| app.ask_open.update(|o| *o = !*o)
                            >
                                "Ask"
                            </button>
                        }
                    })
            }}
            <span class="user">{move || app.session.get().map(|s| s.user).unwrap_or_default()}</span>
            <button on:click=logout>"Log out"</button>
        </header>
    }
}

#[component]
fn Tabs() -> impl IntoView {
    let app = expect_context::<App>();
    view! {
        <nav class="tabs">
            <For
                each=move || app.sheets.get()
                key=|sheet| sheet.key
                children=move |sheet: Sheet| {
                    view! {
                        <span class="tab" class:active=move || app.active.get() == sheet.key>
                            <span class="tab-title" on:click=move |_| app.active.set(sheet.key)>
                                {move || {
                                    let t = sheet.title.get();
                                    if t.is_empty() { "Untitled".to_string() } else { t }
                                }}
                                {move || sheet.dirty.get().then_some(" •")}
                                {move || sheet.running.get().map(|_| " ⏳")}
                            </span>
                            <button
                                class="close"
                                title="Close the tab (the worksheet stays saved)"
                                on:click=move |_| app.close(sheet.key)
                            >
                                "×"
                            </button>
                        </span>
                    }
                }
            />
            <button class="tab-new" title="New worksheet" on:click=move |_| {
                app.open("Untitled", "");
            }>"+"</button>
        </nav>
    }
}

#[component]
fn SheetBar(sheet: Sheet) -> impl IntoView {
    let app = expect_context::<App>();
    view! {
        <div class="sheetbar">
            <input
                class="title"
                aria-label="Worksheet title"
                prop:value=move || sheet.title.get()
                prop:readOnly=move || !sheet.mine.get()
                on:input=move |ev| {
                    sheet.title.set(event_target_value(&ev));
                    app.edited(sheet);
                }
            />
            {move || {
                if sheet.running.get().is_some() {
                    view! { <button on:click=move |_| app.cancel(sheet)>"Cancel"</button> }.into_any()
                } else {
                    view! {
                        <button
                            class="primary"
                            title="Shift+Ctrl/Cmd+Enter; Ctrl/Cmd+Enter runs the statement at the cursor"
                            on:click=move |_| app.run(sheet, sheet.sql.get_untracked())
                        >
                            "Run all"
                        </button>
                        <span class="hint">"Ctrl/Cmd+Enter: statement at cursor"</span>
                    }
                    .into_any()
                }
            }}
            {move || {
                if sheet.mine.get() {
                    view! {
                        <label class="share" title="Comma-separated user names, or * for everyone">
                            "share with "
                            <input
                                prop:value=move || sheet.shared_with.get()
                                on:change=move |ev| {
                                    sheet.shared_with.set(event_target_value(&ev));
                                    app.save(sheet);
                                }
                            />
                        </label>
                        <span class="saved-state">
                            {move || {
                                if sheet.dirty.get() {
                                    "unsaved"
                                } else if sheet.id.get().is_some() {
                                    "saved"
                                } else {
                                    ""
                                }
                            }}
                        </span>
                    }
                    .into_any()
                } else {
                    view! {
                        <span class="hint">"shared with you (read-only)"</span>
                        <button on:click=move |_| {
                            let copy = app.open(
                                &format!("{} (copy)", sheet.title.get_untracked()),
                                &sheet.sql.get_untracked(),
                            );
                            app.save(copy);
                        }>"Duplicate"</button>
                    }
                    .into_any()
                }
            }}
        </div>
    }
}

#[component]
fn Workspace() -> impl IntoView {
    let app = expect_context::<App>();
    let main = move || {
        app.current().map(|sheet| {
            view! {
                <SheetBar sheet />
                <Editor sheet />
                <Results sheet />
            }
        })
    };
    view! {
        <TopBar />
        <main class="layout" class:asking=move || app.ask_open.get()>
            <Sidebar />
            <section class="work">
                <Tabs />
                {main}
            </section>
            {move || app.ask_open.get().then(|| view! { <AskPanel /> })}
        </main>
        {move || app.import_open.get().then(|| view! { <crate::import::ImportDialog /> })}
    }
}
