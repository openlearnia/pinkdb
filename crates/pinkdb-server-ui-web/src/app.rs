//! The app shell (docs/console-design/README.md §2): sign-in, then the top
//! bar, the section rail, the explorer, the workspace, the inspector and the
//! status bar.

use leptos::ev;
use leptos::html;
use leptos::prelude::*;
use leptos::task::spawn_local;
use pinkdb_server_ui_api::{
    LoginAnswer, LoginRequest, LoginRequired, SessionInfo, SetupRequest, VerifyRequest, Worksheet,
};
use wasm_bindgen::JsCast;

use crate::api;
use crate::ask::AskPanel;
use crate::editor::Editor;
use crate::explorer::Explorer;
use crate::icon::Icon;
use crate::results::Results;
use crate::route;
use crate::state::{
    App, EDITOR_PX, RunKind, Section, Sheet, apply_theme, count, kind_icon, millis, shortcut,
};
use crate::ui::{ConfirmDialog, Scrim, Toasts};

/// A drag in progress: the explorer's edge or the editor's splitter.
#[derive(Clone, Copy)]
pub enum Drag {
    Explorer { x0: f64, w0: f64 },
    Editor { sheet: Sheet, y0: f64, h0: f64 },
}

/// Start the app once a session exists.
fn start(app: App, session: SessionInfo) {
    app.session.set(Some(session));
    app.login_mode.set(None);
    app.notice.set(String::new());
    // The URL's place (a reload or a link), then the data.
    route::apply(app, route::parse(&route::location_path()));
    app.load_catalog();
    app.load_worksheets(true);
    app.load_storage();
}

#[component]
pub fn Root() -> impl IntoView {
    let app = App::new();
    provide_context(app);
    apply_theme(&app.theme.get_untracked());
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
                    .unwrap_or_else(|_| "password".to_string());
                if mode == "none" {
                    login_open(app);
                } else {
                    app.login_mode.set(Some(mode));
                }
            }
            Err(e) => app.notice.set(format!("Cannot reach the console: {e}")),
        }
    });
    view! {
        {move || {
            if app.session.get().is_some() {
                view! { <Workspace /> }.into_any()
            } else if app.login_mode.get().is_some() {
                view! { <Login /> }.into_any()
            } else {
                view! {
                    <div class="splash">
                        <div class="splash__inner">
                            {move || {
                                let n = app.notice.get();
                                if n.is_empty() {
                                    view! { <span class="spinner"></span> }.into_any()
                                } else {
                                    view! { <span class="login__error">{n}</span> }.into_any()
                                }
                            }}
                        </div>
                    </div>
                }
                    .into_any()
            }
        }}
    }
}

/// Sign in without credentials: a console with `--ui-auth none`.
fn login_open(app: App) {
    spawn_local(async move {
        match api::post::<_, SessionInfo>("/api/login", &LoginRequest::default(), "").await {
            Ok(session) => start(app, session),
            Err(e) => {
                app.notice.set(e.message);
                app.login_mode.set(Some("none".to_string()));
            }
        }
    });
}

/// The sign-in screen's step.
#[derive(Clone, Debug, PartialEq)]
enum Step {
    /// The first account of a store without users.
    Setup,
    /// User and password.
    Password,
    /// The authenticator (or recovery) code for a challenge.
    Code(String),
}

#[component]
fn Login() -> impl IntoView {
    let app = expect_context::<App>();
    let step = RwSignal::new(match app.login_mode.get_untracked().as_deref() {
        Some("setup") => Step::Setup,
        _ => Step::Password,
    });
    let user = RwSignal::new(String::new());
    let secret = RwSignal::new(String::new());
    let confirm = RwSignal::new(String::new());
    let setup_code = RwSignal::new(String::new());
    let code = RwSignal::new(String::new());
    let busy = RwSignal::new(false);
    let fail = move |message: String| {
        busy.set(false);
        app.notice.set(message);
    };
    let submit = move |ev: leptos::ev::SubmitEvent| {
        ev.prevent_default();
        if busy.get_untracked() {
            return;
        }
        app.notice.set(String::new());
        match step.get_untracked() {
            Step::Setup => {
                if secret.get_untracked() != confirm.get_untracked() {
                    app.notice.set("The two passwords differ.".to_string());
                    return;
                }
                let request = SetupRequest {
                    code: setup_code.get_untracked(),
                    user: user.get_untracked(),
                    password: secret.get_untracked(),
                };
                busy.set(true);
                spawn_local(async move {
                    match api::post::<_, SessionInfo>("/api/setup", &request, "").await {
                        Ok(session) => start(app, session),
                        Err(e) => fail(e.message),
                    }
                });
            }
            Step::Password => {
                let request = LoginRequest {
                    user: Some(user.get_untracked()),
                    password: Some(secret.get_untracked()),
                };
                busy.set(true);
                spawn_local(async move {
                    match api::post::<_, LoginAnswer>("/api/login", &request, "").await {
                        Ok(LoginAnswer::Session(session)) => {
                            secret.set(String::new());
                            start(app, session);
                        }
                        Ok(LoginAnswer::SecondFactor(factor)) => {
                            busy.set(false);
                            secret.set(String::new());
                            code.set(String::new());
                            step.set(Step::Code(factor.challenge));
                        }
                        Err(e) => fail(e.message),
                    }
                });
            }
            Step::Code(challenge) => {
                let request = VerifyRequest {
                    challenge,
                    code: code.get_untracked(),
                };
                busy.set(true);
                spawn_local(async move {
                    match api::post::<_, SessionInfo>("/api/login/verify", &request, "").await {
                        Ok(session) => start(app, session),
                        Err(e) => {
                            // An expired or exhausted challenge starts over.
                            if e.message.contains("expired") {
                                step.set(Step::Password);
                            }
                            code.set(String::new());
                            fail(e.message);
                        }
                    }
                });
            }
        }
    };
    let host = leptos::prelude::window()
        .location()
        .host()
        .unwrap_or_default();
    let field = move |label: &'static str,
                      signal: RwSignal<String>,
                      kind: &'static str,
                      autocomplete: &'static str| {
        view! {
            <label class="field">
                <span class="field__label">{label}</span>
                <input
                    class="input input--lg"
                    type=kind
                    autocomplete=autocomplete
                    prop:value=move || signal.get()
                    on:input=move |ev| signal.set(event_target_value(&ev))
                />
            </label>
        }
    };
    let body = move || {
        match step.get() {
        Step::Setup => view! {
            <div>
                <h1 class="login__title">"Create the administrator"</h1>
                <p class="muted">"This warehouse has no users yet. The first account can do everything."</p>
            </div>
            <label class="field">
                <span class="field__label">"Setup code"</span>
                <input
                    class="input input--lg mono"
                    autocomplete="off"
                    placeholder="ABCD-EFGH-IJKL"
                    prop:value=move || setup_code.get()
                    on:input=move |ev| setup_code.set(event_target_value(&ev))
                />
                <span class="field__help">"Printed in the server's log when it started."</span>
            </label>
            {field("User name", user, "text", "username")}
            {field("Password", secret, "password", "new-password")}
            {field("Confirm the password", confirm, "password", "new-password")}
            <p class="field__help">"At least 10 characters."</p>
        }
        .into_any(),
        Step::Password => view! {
            <div>
                <h1 class="login__title">"Sign in"</h1>
                <p class="muted">"to the warehouse console on " <span class="mono">{host.clone()}</span></p>
            </div>
            {field("User", user, "text", "username")}
            {field("Password", secret, "password", "current-password")}
        }
        .into_any(),
        Step::Code(_) => view! {
            <div>
                <h1 class="login__title">"Two-factor sign-in"</h1>
                <p class="muted">"Enter the 6-digit code from your authenticator app."</p>
            </div>
            <label class="field">
                <span class="field__label">"Code"</span>
                <input
                    class="input input--lg mono login__code"
                    autocomplete="one-time-code"
                    inputmode="numeric"
                    placeholder="123 456"
                    prop:value=move || code.get()
                    on:input=move |ev| code.set(event_target_value(&ev))
                />
                <span class="field__help">"Lost your phone? A recovery code works here too, once."</span>
            </label>
        }
        .into_any(),
    }
    };
    let action = move || match step.get() {
        Step::Setup => "Create and sign in",
        Step::Password => "Sign in",
        Step::Code(_) => "Verify",
    };
    view! {
        <div class="login">
            <form class="card login__card" on:submit=submit>
                <div class="login__brand">
                    <span class="brand__mark" aria-hidden="true">"p"</span>
                    <span class="brand__name">"pinkdb"</span>
                </div>
                {body}
                <button class="btn btn--primary btn--lg btn--block" type="submit" disabled=move || busy.get()>
                    {move || busy.get().then(|| view! { <span class="spinner"></span> })}
                    {action}
                </button>
                <div class="login__error" role="alert">{move || app.notice.get()}</div>
                {move || {
                    matches!(step.get(), Step::Code(_))
                        .then(|| {
                            view! {
                                <button
                                    class="btn btn--ghost btn--sm"
                                    type="button"
                                    on:click=move |_| {
                                        app.notice.set(String::new());
                                        step.set(Step::Password);
                                    }
                                >
                                    "Use another account"
                                </button>
                            }
                        })
                }}
            </form>
        </div>
    }
}

/// Whether a key event comes from a text field (shortcuts leave it alone).
fn typing(ev: &web_sys::KeyboardEvent) -> bool {
    ev.target()
        .and_then(|t| t.dyn_into::<web_sys::Element>().ok())
        .is_some_and(|el| matches!(el.tag_name().as_str(), "INPUT" | "TEXTAREA" | "SELECT"))
}

/// Focus an element by id.
fn focus(id: &str) {
    if let Some(el) = leptos::prelude::document()
        .get_element_by_id(id)
        .and_then(|e| e.dyn_into::<web_sys::HtmlElement>().ok())
    {
        let _ = el.focus();
    }
}

#[component]
fn Workspace() -> impl IntoView {
    let app = expect_context::<App>();
    let drag = RwSignal::new(None::<Drag>);
    route::install(app);

    let keys = window_event_listener(ev::keydown, move |e: web_sys::KeyboardEvent| {
        let command = e.ctrl_key() || e.meta_key();
        let key = e.key();
        if command && key.eq_ignore_ascii_case("k") {
            e.prevent_default();
            focus("console-search");
        } else if command && key.eq_ignore_ascii_case("b") {
            e.prevent_default();
            app.explorer_open.update(|o| *o = !*o);
        } else if key == "/" && !typing(&e) && app.section.get_untracked().has_explorer() {
            e.prevent_default();
            app.explorer_open.set(true);
            focus("explorer-filter");
        }
    });
    let moves = window_event_listener(ev::mousemove, move |e: web_sys::MouseEvent| {
        match drag.get_untracked() {
            Some(Drag::Explorer { x0, w0 }) => app
                .explorer_width
                .set((w0 + f64::from(e.client_x()) - x0).clamp(200.0, 480.0)),
            Some(Drag::Editor { sheet, y0, h0 }) => {
                // Keep room for the results below.
                let max = leptos::prelude::window()
                    .inner_height()
                    .ok()
                    .and_then(|h| h.as_f64())
                    .unwrap_or(900.0)
                    - 280.0;
                sheet
                    .editor_px
                    .set((h0 + f64::from(e.client_y()) - y0).clamp(96.0, max.max(120.0)));
            }
            None => {}
        }
    });
    let ups = window_event_listener(ev::mouseup, move |_| {
        if drag.get_untracked().is_some() {
            drag.set(None);
        }
    });
    on_cleanup(move || {
        keys.remove();
        moves.remove();
        ups.remove();
    });

    let main = move || match app.section.get() {
        Section::Query => view! { <QueryView drag /> }.into_any(),
        Section::Catalog => view! { <crate::catalog::CatalogPage /> }.into_any(),
        Section::History => view! { <crate::history::HistoryPage /> }.into_any(),
        Section::Access => view! { <crate::access::AccessPage /> }.into_any(),
        Section::Storage => view! { <crate::storage::StoragePage /> }.into_any(),
        Section::Account => view! { <crate::account::AccountPage /> }.into_any(),
    };
    view! {
        <div
            class="app"
            class:is-resizing=move || matches!(drag.get(), Some(Drag::Explorer { .. }))
            class:is-splitting=move || matches!(drag.get(), Some(Drag::Editor { .. }))
        >
            <TopBar />
            <Rail />
            {move || {
                (app.section.get().has_explorer() && app.explorer_open.get())
                    .then(|| view! { <Explorer drag /> })
            }}
            <main class="work">{main}</main>
            {move || app.ask_open.get().then(|| view! { <AskPanel /> })}
            <StatusBar />
        </div>
        {move || app.import_open.get().then(|| view! { <crate::import::ImportDialog /> })}
        <ConfirmDialog />
        <Toasts />
    }
}

#[component]
fn TopBar() -> impl IntoView {
    let app = expect_context::<App>();
    let menu = RwSignal::new(false);
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
    let user = move || app.session.get().map(|s| s.user).unwrap_or_default();
    let initials = move || {
        let u = user();
        let letters: String = u.chars().filter(|c| c.is_alphanumeric()).take(2).collect();
        if letters.is_empty() {
            "?".to_string()
        } else {
            letters.to_uppercase()
        }
    };
    let toggle_theme = move || app.set_theme(if app.is_dark() { "light" } else { "dark" });
    view! {
        <header class="topbar">
            <div class="brand">
                <span class="brand__mark" aria-hidden="true">"p"</span>
            </div>
            <span class="brand__name">"pinkdb " <span class="brand__sub">"Warehouse"</span></span>
            <span class="topbar__sep"></span>
            <label class="row context-label" title="The database statements run in">
                <Icon name="database" class="subtle" />
                <select
                    class="input context-select"
                    aria-label="Database"
                    on:change=move |ev| {
                        app.db.set(event_target_value(&ev));
                        app.load_catalog();
                    }
                >
                    {databases}
                </select>
            </label>
            <Search />
            {move || {
                (!app.read_only())
                    .then(|| {
                        view! {
                            <button class="btn btn--ghost" on:click=move |_| app.import_open.set(true)>
                                <Icon name="upload" />
                                "Import"
                            </button>
                        }
                    })
            }}
            {move || {
                app.session
                    .get()
                    .filter(|s| s.agent)
                    .map(|_| {
                        view! {
                            <button
                                class="btn btn--ghost"
                                class:is-active=move || app.ask_open.get()
                                aria-pressed=move || app.ask_open.get().to_string()
                                on:click=move |_| app.ask_open.update(|o| *o = !*o)
                            >
                                <Icon name="sparkles" />
                                "Ask"
                            </button>
                        }
                    })
            }}
            <span class="topbar__sep"></span>
            <button
                class="btn btn--ghost btn--icon"
                aria-label="Switch the theme"
                title="Switch between the light and dark themes"
                on:click=move |_| toggle_theme()
            >
                {move || {
                    if app.is_dark() {
                        view! { <Icon name="sun" /> }.into_any()
                    } else {
                        view! { <Icon name="moon" /> }.into_any()
                    }
                }}
            </button>
            <div class="pop-anchor">
                <button
                    class="avatar"
                    aria-label="Account"
                    aria-expanded=move || menu.get().to_string()
                    on:click=move |_| menu.update(|m| *m = !*m)
                >
                    {initials}
                </button>
                {move || {
                    menu.get()
                        .then(|| {
                            let session = app.session.get();
                            let role = session
                                .as_ref()
                                .map(|s| if s.admin { "Administrator" } else { "User" })
                                .unwrap_or_default();
                            let auth = session
                                .as_ref()
                                .map(|s| match (s.auth.as_str(), s.two_factor) {
                                    ("none", _) => "no sign-in",
                                    (_, true) => "two-factor sign-in",
                                    _ => "password sign-in",
                                })
                                .unwrap_or_default();
                            view! {
                                <Scrim open=menu />
                                <div class="menu pop pop--right menu--account" role="menu">
                                    <div class="menu__label">
                                        <div class="mono menu__name">{user()}</div>
                                        <div class="subtle text-sm">{format!("{role} · {auth}")}</div>
                                    </div>
                                    <div class="menu__sep"></div>
                                    <a
                                        class="menu__item"
                                        role="menuitem"
                                        href="/account"
                                        on:click=move |ev: web_sys::MouseEvent| {
                                            ev.prevent_default();
                                            menu.set(false);
                                            app.go(Section::Account);
                                        }
                                    >
                                        <Icon name="user" />
                                        "Account"
                                    </a>
                                    <button
                                        class="menu__item"
                                        role="menuitem"
                                        on:click=move |_| {
                                            menu.set(false);
                                            toggle_theme();
                                        }
                                    >
                                        {move || {
                                            if app.is_dark() {
                                                view! { <Icon name="sun" /> "Light theme" }.into_any()
                                            } else {
                                                view! { <Icon name="moon" /> "Dark theme" }.into_any()
                                            }
                                        }}
                                    </button>
                                    {(app.session.get().is_some_and(|s| s.auth != "none"))
                                        .then(|| {
                                            view! {
                                                <div class="menu__sep"></div>
                                                <button class="menu__item" role="menuitem" on:click=logout>
                                                    <Icon name="log-out" />
                                                    "Sign out"
                                                </button>
                                            }
                                        })}
                                </div>
                            }
                        })
                }}
            </div>
        </header>
    }
}

/// One ⌘K search result.
#[derive(Clone)]
enum Hit {
    Object {
        database: String,
        name: String,
        kind: String,
    },
    Sheet(Worksheet),
}

/// The ⌘K search over the catalog and the saved worksheets.
#[component]
fn Search() -> impl IntoView {
    let app = expect_context::<App>();
    let query = RwSignal::new(String::new());
    let open = RwSignal::new(false);
    let active = RwSignal::new(0usize);
    let input: NodeRef<html::Input> = NodeRef::new();
    let hits = move || -> Vec<Hit> {
        let q = query.get().trim().to_ascii_lowercase();
        if q.is_empty() {
            return Vec::new();
        }
        let mut out: Vec<Hit> = app.catalog.with(|c| {
            c.objects
                .iter()
                .filter(|o| o.name.to_ascii_lowercase().contains(&q))
                .take(8)
                .map(|o| Hit::Object {
                    database: o.database.clone(),
                    name: o.name.clone(),
                    kind: o.kind.clone(),
                })
                .collect()
        });
        out.extend(app.saved.with(|s| {
            s.iter()
                .filter(|w| w.title.to_ascii_lowercase().contains(&q))
                .take(6)
                .cloned()
                .map(Hit::Sheet)
                .collect::<Vec<_>>()
        }));
        out
    };
    let choose = move |hit: Hit| {
        match hit {
            Hit::Object { database, name, .. } => {
                app.selected.set(Some((database, name)));
                app.go(Section::Catalog);
            }
            Hit::Sheet(w) => {
                app.go(Section::Query);
                app.open_saved(&w);
            }
        }
        query.set(String::new());
        open.set(false);
        if let Some(el) = input.get_untracked() {
            let _ = el.blur();
        }
    };
    let on_key = move |ev: web_sys::KeyboardEvent| {
        let n = hits().len();
        match ev.key().as_str() {
            "ArrowDown" if n > 0 => {
                ev.prevent_default();
                active.update(|a| *a = (*a + 1) % n);
            }
            "ArrowUp" if n > 0 => {
                ev.prevent_default();
                active.update(|a| *a = (*a + n - 1) % n);
            }
            "Enter" => {
                if let Some(hit) = hits().get(active.get_untracked()).cloned() {
                    ev.prevent_default();
                    choose(hit);
                }
            }
            "Escape" => {
                open.set(false);
                if let Some(el) = input.get_untracked() {
                    let _ = el.blur();
                }
            }
            _ => {}
        }
    };
    let list = move || {
        (open.get() && !query.get().trim().is_empty()).then(|| {
            let hits = hits();
            let selected = active.get();
            if hits.is_empty() {
                return view! {
                    <div class="menu cmdk__list">
                        <div class="cmdk__empty">"Nothing matches."</div>
                    </div>
                }
                .into_any();
            }
            let first_sheet = hits.iter().position(|h| matches!(h, Hit::Sheet(_)));
            let rows = hits
                .into_iter()
                .enumerate()
                .map(|(i, hit)| {
                    let header = if i == 0 && matches!(hit, Hit::Object { .. }) {
                        Some("Objects")
                    } else if Some(i) == first_sheet {
                        Some("Worksheets")
                    } else {
                        None
                    };
                    let (icon, label, detail) = match &hit {
                        Hit::Object {
                            database,
                            name,
                            kind,
                        } => (
                            kind_icon(kind),
                            name.clone(),
                            format!("{database} · {}", kind.to_ascii_lowercase()),
                        ),
                        Hit::Sheet(w) => (
                            "file-code",
                            w.title.clone(),
                            if w.mine {
                                String::new()
                            } else {
                                w.owner.clone()
                            },
                        ),
                    };
                    let mono = matches!(hit, Hit::Object { .. });
                    view! {
                        {header.map(|h| view! { <div class="overline cmdk__group">{h}</div> })}
                        <button
                            class="menu__item"
                            class:is-active=i == selected
                            on:mousedown=move |ev| {
                                ev.prevent_default();
                                choose(hit.clone());
                            }
                        >
                            <Icon name=icon />
                            <span class:mono=mono>{label}</span>
                            <span class="cmdk__detail">{detail}</span>
                        </button>
                    }
                })
                .collect_view();
            view! { <div class="menu cmdk__list" role="listbox">{rows}</div> }.into_any()
        })
    };
    view! {
        <div class="cmdk">
            <label class="search">
                <Icon name="search" />
                <input
                    id="console-search"
                    class="input"
                    node_ref=input
                    placeholder="Search tables, views, worksheets…"
                    autocomplete="off"
                    aria-label="Search"
                    prop:value=move || query.get()
                    on:input=move |ev| {
                        query.set(event_target_value(&ev));
                        active.set(0);
                        open.set(true);
                    }
                    on:focus=move |_| open.set(true)
                    on:blur=move |_| open.set(false)
                    on:keydown=on_key
                />
                <span class="kbd">{shortcut("K")}</span>
            </label>
            {list}
        </div>
    }
}

#[component]
fn Rail() -> impl IntoView {
    let app = expect_context::<App>();
    // Links, so a modified click opens the section in a new tab.
    let item = move |section: Section, icon: &'static str, label: &'static str| {
        view! {
            <a
                class="rail__item"
                href=route::section_path(section)
                class:is-active=move || app.section.get() == section
                aria-current=move || (app.section.get() == section).then_some("page")
                on:click=move |ev: web_sys::MouseEvent| {
                    // A plain click returns to the section as it was left
                    // (the open worksheet, the open object); a modified one
                    // is the browser's (a new tab).
                    if ev.button() == 0
                        && !(ev.meta_key() || ev.ctrl_key() || ev.shift_key() || ev.alt_key())
                    {
                        ev.prevent_default();
                        app.go(section);
                    }
                }
            >
                <Icon name=icon size=18 />
                {label}
            </a>
        }
    };
    view! {
        <nav class="rail" aria-label="Sections">
            {item(Section::Query, "file-code", "Query")}
            {item(Section::Catalog, "database", "Catalog")}
            {item(Section::History, "history", "History")}
            <span class="rail__sep"></span>
            {item(Section::Access, "users", "Access")}
            {item(Section::Storage, "hard-drive", "Storage")}
            <span class="spacer"></span>
            {move || {
                app.section
                    .get()
                    .has_explorer()
                    .then(|| {
                        view! {
                            <button
                                class="btn btn--ghost btn--icon"
                                class:is-active=move || app.explorer_open.get()
                                aria-label="Show or hide the explorer"
                                title=format!("Show or hide the explorer ({})", shortcut("B"))
                                on:click=move |_| app.explorer_open.update(|o| *o = !*o)
                            >
                                <Icon name="panel-left" />
                            </button>
                        }
                    })
            }}
        </nav>
    }
}

#[component]
fn StatusBar() -> impl IntoView {
    let app = expect_context::<App>();
    let mode = move || {
        app.storage.get().map(|s| {
            let mut m = s.mode.clone();
            if let Some(first) = m.get_mut(0..1) {
                first.make_ascii_uppercase();
            }
            m
        })
    };
    let run_state = move || {
        let sheet = app.current()?;
        if sheet.running.get().is_some() {
            return Some(view! { <span class="status status--info">"Running"</span> }.into_any());
        }
        let response = sheet.result.get()?;
        if let Some(e) = &response.error {
            let label = if e.cancelled { "Cancelled" } else { "Failed" };
            let class = if e.cancelled {
                "status status--warning"
            } else {
                "status status--danger"
            };
            return Some(
                view! { <span class=class>{format!("{label} · statement {}", e.index + 1)}</span> }
                    .into_any(),
            );
        }
        let rows: u64 = response
            .results
            .iter()
            .filter(|r| r.kind == "rows")
            .map(|r| r.row_count)
            .sum();
        let statements = response.results.len();
        let text = if statements == 1 {
            format!("{} rows · {}", count(rows), millis(response.elapsed_ms))
        } else {
            format!("{statements} statements · {}", millis(response.elapsed_ms))
        };
        Some(view! { <span>{text}</span> }.into_any())
    };
    view! {
        <footer class="statusbar">
            <span class="statusbar__item">
                <span class="status status--success">"Connected"</span>
            </span>
            <span class="statusbar__sep"></span>
            <span class="statusbar__item mono" title="The database statements run in">
                <Icon name="database" />
                {move || app.db.get()}
            </span>
            <span class="statusbar__item">
                <Icon name="user" />
                <span class="mono">{move || app.session.get().map(|s| s.user).unwrap_or_default()}</span>
                {move || app.admin().then_some(" · admin")}
            </span>
            {move || {
                mode()
                    .map(|m| {
                        view! {
                            <span class="statusbar__item">
                                <Icon name="hard-drive" />
                                {m}
                            </span>
                        }
                    })
            }}
            {move || {
                app.read_only()
                    .then(|| view! { <span class="badge badge--warning">"Read-only"</span> })
            }}
            <span class="spacer"></span>
            {move || {
                let notice = app.notice.get();
                (!notice.is_empty())
                    .then(|| {
                        view! {
                            <button
                                class="btn btn--ghost btn--sm statusbar__notice"
                                title="Dismiss"
                                on:click=move |_| app.notice.set(String::new())
                            >
                                <Icon name="alert-circle" size=14 />
                                {notice}
                            </button>
                        }
                    })
            }}
            {move || {
                (app.section.get() == Section::Query)
                    .then(|| {
                        view! {
                            <span class="statusbar__item">{run_state}</span>
                            <span class="statusbar__sep"></span>
                            <span class="statusbar__item">
                                {move || {
                                    let (line, column) = app.cursor.get();
                                    format!("Ln {line}, Col {column}")
                                }}
                            </span>
                        }
                    })
            }}
        </footer>
    }
}

#[component]
fn QueryView(drag: RwSignal<Option<Drag>>) -> impl IntoView {
    let app = expect_context::<App>();
    let sheet_view = move || {
        app.current().map(|sheet| {
            view! {
                <div class="sheet">
                    <SheetBar sheet />
                    <Editor sheet />
                    <div
                        class="splitter"
                        role="separator"
                        aria-orientation="horizontal"
                        aria-label="Resize the editor"
                        title="Drag to resize; double-click resets"
                        on:mousedown=move |ev: web_sys::MouseEvent| {
                            ev.prevent_default();
                            drag.set(
                                Some(Drag::Editor {
                                    sheet,
                                    y0: f64::from(ev.client_y()),
                                    h0: sheet.editor_px.get_untracked(),
                                }),
                            );
                        }
                        on:dblclick=move |_| sheet.editor_px.set(EDITOR_PX)
                    ></div>
                    <Results sheet />
                </div>
            }
        })
    };
    view! {
        <div class="query">
            <DocTabs />
            {sheet_view}
        </div>
    }
}

#[component]
fn DocTabs() -> impl IntoView {
    let app = expect_context::<App>();
    view! {
        <nav class="doctabs" aria-label="Open worksheets">
            <For
                each=move || app.sheets.get()
                key=|sheet| sheet.key
                children=move |sheet: Sheet| {
                    let title = move || {
                        let t = sheet.title.get();
                        if t.is_empty() { "Untitled".to_string() } else { t }
                    };
                    view! {
                        <span
                            class="doctab"
                            class:is-active=move || app.active.get() == sheet.key
                            title=title
                            on:click=move |_| app.active.set(sheet.key)
                            on:mousedown=move |ev: web_sys::MouseEvent| {
                                if ev.button() == 1 {
                                    ev.prevent_default();
                                    app.close(sheet.key);
                                }
                            }
                        >
                            {move || {
                                if sheet.running.get().is_some() {
                                    view! { <span class="spinner spinner--sm" title="Running"></span> }.into_any()
                                } else {
                                    view! { <Icon name="file-code" size=14 /> }.into_any()
                                }
                            }}
                            <span class="doctab__title">{title}</span>
                            {move || {
                                sheet.dirty.get().then(|| view! { <span class="doctab__dirty" title="Unsaved changes"></span> })
                            }}
                            <button
                                class="doctab__close"
                                aria-label="Close the tab"
                                title="Close the tab (the worksheet stays saved)"
                                on:click=move |ev| {
                                    ev.stop_propagation();
                                    app.close(sheet.key);
                                }
                            >
                                <Icon name="x" size=14 />
                            </button>
                        </span>
                    }
                }
            />
            <button
                class="btn btn--ghost btn--icon btn--sm doctabs__new"
                aria-label="New worksheet"
                title="New worksheet"
                on:click=move |_| {
                    app.open("Untitled", "");
                }
            >
                <Icon name="plus" size=14 />
            </button>
        </nav>
    }
}

#[component]
fn SheetBar(sheet: Sheet) -> impl IntoView {
    let app = expect_context::<App>();
    let run_menu = RwSignal::new(false);
    let share_open = RwSignal::new(false);
    let state = move || {
        if !sheet.mine.get() {
            view! { <span class="sheet__state">"Shared with you · read-only"</span> }.into_any()
        } else if sheet.dirty.get() {
            view! {
                <span class="sheet__state">
                    <span class="doctab__dirty"></span>
                    "Edited · saves as you type"
                </span>
            }
            .into_any()
        } else if sheet.id.get().is_some() {
            view! {
                <span class="sheet__state">
                    <Icon name="check" size=14 />
                    "Saved"
                </span>
            }
            .into_any()
        } else {
            ().into_any()
        }
    };
    let share = move || {
        if sheet.mine.get() {
            view! {
                <div class="pop-anchor">
                    <button
                        class="btn btn--sm"
                        aria-expanded=move || share_open.get().to_string()
                        on:click=move |_| share_open.update(|o| *o = !*o)
                    >
                        <Icon name="share" size=14 />
                        "Share"
                        {move || {
                            let n = sheet.shared_with.get();
                            (!n.trim().is_empty()).then(|| {
                                let label = if n.trim() == "*" {
                                    "Everyone".to_string()
                                } else {
                                    n.split(',').filter(|p| !p.trim().is_empty()).count().to_string()
                                };
                                view! { <span class="badge">{label}</span> }
                            })
                        }}
                    </button>
                    {move || {
                        share_open
                            .get()
                            .then(|| {
                                view! {
                                    <Scrim open=share_open />
                                    <div class="menu pop pop--right menu--share">
                                        <label class="field">
                                            <span class="field__label">"Share with"</span>
                                            <input
                                                class="input mono"
                                                placeholder="ana, sam — or * for everyone"
                                                prop:value=move || sheet.shared_with.get()
                                                on:change=move |ev| {
                                                    sheet.shared_with.set(event_target_value(&ev));
                                                    app.save(sheet);
                                                }
                                            />
                                            <span class="field__help">
                                                "Comma-separated user names. They can read and duplicate it, not edit it."
                                            </span>
                                        </label>
                                    </div>
                                }
                            })
                    }}
                </div>
            }
            .into_any()
        } else {
            view! {
                <button
                    class="btn btn--sm"
                    on:click=move |_| {
                        let copy = app.open(
                            &format!("{} (copy)", sheet.title.get_untracked()),
                            &sheet.sql.get_untracked(),
                        );
                        app.save(copy);
                    }
                >
                    <Icon name="copy" size=14 />
                    "Duplicate"
                </button>
            }
            .into_any()
        }
    };
    let ask = move |kind: RunKind| {
        run_menu.set(false);
        app.run_request.set(Some(kind));
    };
    let run = move || {
        if sheet.running.get().is_some() {
            view! {
                <button class="btn" on:click=move |_| app.cancel(sheet)>
                    <span class="spinner"></span>
                    "Cancel"
                    <span class="kbd">"Esc"</span>
                </button>
            }
            .into_any()
        } else {
            view! {
                <div class="pop-anchor">
                    <div class="split">
                        <button
                            class="btn btn--primary"
                            title="Run the selection, or the statement at the cursor"
                            on:click=move |_| ask(RunKind::Cursor)
                        >
                            <Icon name="play" size=14 />
                            "Run"
                            <span class="kbd">{shortcut("↵")}</span>
                        </button>
                        <button
                            class="btn btn--primary"
                            aria-label="More ways to run"
                            aria-expanded=move || run_menu.get().to_string()
                            on:click=move |_| run_menu.update(|o| *o = !*o)
                        >
                            <Icon name="chevron-down" size=14 />
                        </button>
                    </div>
                    {move || {
                        run_menu
                            .get()
                            .then(|| {
                                view! {
                                    <Scrim open=run_menu />
                                    <div class="menu pop pop--right menu--run" role="menu">
                                        <button class="menu__item" role="menuitem" on:click=move |_| ask(RunKind::Cursor)>
                                            <Icon name="play" />
                                            "Run statement at cursor"
                                            <span class="kbd">{shortcut("↵")}</span>
                                        </button>
                                        <button class="menu__item" role="menuitem" on:click=move |_| ask(RunKind::All)>
                                            <Icon name="play" />
                                            "Run all"
                                            <span class="kbd">{shortcut("⇧↵")}</span>
                                        </button>
                                        <div class="menu__sep"></div>
                                        <button class="menu__item" role="menuitem" on:click=move |_| ask(RunKind::Explain)>
                                            <Icon name="plan" />
                                            "Explain statement at cursor"
                                        </button>
                                    </div>
                                }
                            })
                    }}
                </div>
            }
            .into_any()
        }
    };
    view! {
        <div class="sheet__bar">
            <input
                class="input input--ghost sheet__title"
                aria-label="Worksheet title"
                placeholder="Untitled"
                prop:value=move || sheet.title.get()
                prop:readOnly=move || !sheet.mine.get()
                on:input=move |ev| {
                    sheet.title.set(event_target_value(&ev));
                    app.edited(sheet);
                }
            />
            {state}
            <span class="spacer"></span>
            {share}
            {run}
        </div>
    }
}
