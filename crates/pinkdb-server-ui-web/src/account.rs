//! The Account page (wave 121d): the signed-in user's password, two-factor
//! sign-in, sessions and personal access tokens.

use leptos::prelude::*;
use leptos::task::spawn_local;
use pinkdb_server_ui_api::{
    AccountInfo, CreatedToken, EndSessions, NewToken, PasswordChange, PasswordCheck, RecoveryCodes,
    RevokeToken, TokenRow, TotpConfirm, TotpEnrolment,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::api;
use crate::icon::Icon;
use crate::state::{App, Tone, ago, date, device};
use crate::ui::{Callout, Dialog, Empty};

/// POST as the signed-in browser; errors become a toast.
fn send<B, T>(app: App, path: &'static str, body: B, done: impl FnOnce(T) + 'static)
where
    B: Serialize + 'static,
    T: DeserializeOwned + 'static,
{
    let csrf = app.csrf();
    spawn_local(async move {
        match api::post::<_, T>(path, &body, &csrf).await {
            Ok(answer) => done(answer),
            Err(e) => app.toast(Tone::Danger, "That did not work", e.message),
        }
    });
}

/// Which dialog is open.
#[derive(Clone, PartialEq)]
enum Open {
    Enrol(TotpEnrolment),
    Codes(Vec<String>),
    /// A password prompt for turning two-factor off (`false`) or for new
    /// recovery codes (`true`).
    Confirm(bool),
    NewToken,
    Created(CreatedToken),
}

#[component]
pub fn AccountPage() -> impl IntoView {
    let info = RwSignal::new(None::<AccountInfo>);
    let failure = RwSignal::new(None::<String>);
    let epoch = RwSignal::new(0u32);
    let dialog = RwSignal::new(None::<Open>);
    Effect::new(move |_| {
        epoch.track();
        spawn_local(async move {
            match api::get::<AccountInfo>("/api/account").await {
                Ok(a) => info.set(Some(a)),
                Err(e) => failure.set(Some(e.message)),
            }
        });
    });
    let reload = move || epoch.update(|e| *e += 1);

    let body = move || {
        let a = info.get()?;
        Some(view! {
            {(!a.has_password)
                .then(|| {
                    view! {
                        <Callout tone=Tone::Info title="This console runs without sign-in">
                            "There is no password, second factor or access token to manage until the store has users."
                        </Callout>
                    }
                })}
            {a.has_password.then(|| view! { <PasswordCard /> })}
            {a.has_password.then(|| view! { <TwoFactorCard info=a.clone() dialog /> })}
            <SessionsCard info=a.clone() reload />
            {a.has_password.then(|| view! { <TokensCard info=a.clone() dialog reload /> })}
        })
    };
    let user = move || info.get().map(|a| a.user).unwrap_or_default();
    let admin = move || info.get().is_some_and(|a| a.admin);
    view! {
        <div class="page">
            <div class="page__inner page__inner--narrow">
                <div class="page-head">
                    <div class="page-head__text">
                        <h1 class="page-head__title">
                            "Account"
                            {move || admin().then(|| view! { <span class="badge badge--accent">"Admin"</span> })}
                        </h1>
                        <p class="page-head__desc">
                            "Signed in as " <span class="mono">{user}</span>
                            ". How you sign in, where you are signed in, and the tokens your scripts use."
                        </p>
                    </div>
                </div>
                {move || failure.get().map(|f| view! { <Callout tone=Tone::Danger>{f}</Callout> })}
                {move || {
                    (info.get().is_none() && failure.get().is_none())
                        .then(|| view! { <div class="empty"><span class="spinner"></span></div> })
                }}
                {body}
            </div>
        </div>
        {move || dialog.get().map(|open| view! { <AccountDialog open dialog reload /> })}
    }
}

#[component]
fn PasswordCard() -> impl IntoView {
    let app = expect_context::<App>();
    let current = RwSignal::new(String::new());
    let new = RwSignal::new(String::new());
    let confirm = RwSignal::new(String::new());
    let problem = RwSignal::new(None::<String>);
    let submit = move |ev: leptos::ev::SubmitEvent| {
        ev.prevent_default();
        if new.get_untracked() != confirm.get_untracked() {
            problem.set(Some("The two new passwords differ.".to_string()));
            return;
        }
        if new.get_untracked().chars().count() < 10 {
            problem.set(Some("A password needs at least 10 characters.".to_string()));
            return;
        }
        problem.set(None);
        let body = PasswordChange {
            current: current.get_untracked(),
            new: new.get_untracked(),
        };
        send(
            app,
            "/api/account/password",
            body,
            move |answer: serde_json::Value| {
                for signal in [current, new, confirm] {
                    signal.set(String::new());
                }
                let ended = answer["ended"].as_u64().unwrap_or(0);
                app.toast(
                    Tone::Success,
                    "Password changed",
                    if ended > 0 {
                        format!("{ended} other session(s) were signed out.")
                    } else {
                        String::new()
                    },
                );
            },
        );
    };
    let field = move |label: &'static str, signal: RwSignal<String>, autocomplete: &'static str| {
        view! {
            <label class="field">
                <span class="field__label">{label}</span>
                <input
                    class="input"
                    type="password"
                    autocomplete=autocomplete
                    prop:value=move || signal.get()
                    on:input=move |ev| signal.set(event_target_value(&ev))
                />
            </label>
        }
    };
    view! {
        <form class="card" on:submit=submit>
            <div class="card__head">
                <Icon name="key" class="subtle" />
                <span class="card__title">"Password"</span>
            </div>
            <div class="card__body stack">
                <div class="fields">
                    {field("Current password", current, "current-password")}
                    {field("New password", new, "new-password")}
                    {field("Confirm the new password", confirm, "new-password")}
                </div>
                {move || problem.get().map(|p| view! { <div class="field__error">{p}</div> })}
            </div>
            <div class="card__foot">
                <span class="subtle text-sm card__note">"At least 10 characters. Your other sessions are signed out."</span>
                <button class="btn btn--primary" type="submit">"Change password"</button>
            </div>
        </form>
    }
}

#[component]
fn TwoFactorCard(info: AccountInfo, dialog: RwSignal<Option<Open>>) -> impl IntoView {
    let app = expect_context::<App>();
    let start = move |_| {
        send(
            app,
            "/api/account/totp/start",
            serde_json::json!({}),
            move |e: TotpEnrolment| {
                dialog.set(Some(Open::Enrol(e)));
            },
        );
    };
    let on = info.two_factor;
    let left = info.recovery_left;
    view! {
        <div class="card">
            <div class="card__head">
                <Icon name="shield" class="subtle" />
                <span class="card__title">"Two-factor sign-in"</span>
                {if on {
                    view! { <span class="badge badge--success">"On"</span> }.into_any()
                } else {
                    view! { <span class="badge">"Off"</span> }.into_any()
                }}
            </div>
            <div class="card__body stack">
                {if on {
                    view! {
                        <p>
                            "Signing in asks for a code from your authenticator app after the password. "
                            {format!("{left} of 10 recovery codes are left.")}
                        </p>
                        {(left <= 3)
                            .then(|| {
                                view! {
                                    <Callout tone=Tone::Warning>
                                        "You are running out of recovery codes: make new ones and keep them somewhere safe."
                                    </Callout>
                                }
                            })}
                    }
                        .into_any()
                } else {
                    view! {
                        <p>
                            "Add a second step to signing in: a 6-digit code from an authenticator app \
                             (1Password, Google Authenticator, Authy…) that changes every 30 seconds."
                        </p>
                    }
                        .into_any()
                }}
            </div>
            <div class="card__foot">
                {if on {
                    view! {
                        <button class="btn" on:click=move |_| dialog.set(Some(Open::Confirm(true)))>
                            "New recovery codes"
                        </button>
                        <button class="btn btn--danger" on:click=move |_| dialog.set(Some(Open::Confirm(false)))>
                            "Turn off"
                        </button>
                    }
                        .into_any()
                } else {
                    view! {
                        <button class="btn btn--primary" on:click=start>
                            <Icon name="shield" size=14 />
                            "Set up"
                        </button>
                    }
                        .into_any()
                }}
            </div>
        </div>
    }
}

#[component]
fn SessionsCard(
    info: AccountInfo,
    reload: impl Fn() + Copy + Send + Sync + 'static,
) -> impl IntoView {
    let app = expect_context::<App>();
    let end = move |body: EndSessions| {
        send(
            app,
            "/api/account/sessions/end",
            body,
            move |answer: serde_json::Value| {
                let ended = answer["ended"].as_u64().unwrap_or(0);
                app.toast(Tone::Success, format!("Signed out {ended} session(s)"), "");
                reload();
            },
        );
    };
    let others = info.sessions.iter().filter(|s| !s.current).count();
    let rows = info
        .sessions
        .iter()
        .map(|s| {
            let id = s.id.clone();
            view! {
                <tr>
                    <td>
                        <span class="row">
                            <Icon name="user" class="subtle" />
                            {device(&s.user_agent)}
                            {s.current.then(|| view! { <span class="badge badge--accent">"This browser"</span> })}
                        </span>
                    </td>
                    <td class="mono muted">{s.ip.clone()}</td>
                    <td class="muted">{ago(s.started)}</td>
                    <td class="muted">
                        {if s.idle_secs < 60 { "now".to_string() } else { format!("{} min ago", s.idle_secs / 60) }}
                    </td>
                    <td class="t-actions">
                        {(!s.current)
                            .then(|| {
                                view! {
                                    <button
                                        class="btn btn--ghost btn--sm btn--danger"
                                        on:click=move |_| end(EndSessions { id: Some(id.clone()), others: false })
                                    >
                                        "Sign out"
                                    </button>
                                }
                            })}
                    </td>
                </tr>
            }
        })
        .collect_view();
    view! {
        <div class="card card--flush">
            <div class="card__head">
                <Icon name="clock" class="subtle" />
                <span class="card__title">"Sessions"</span>
                <span class="badge">{info.sessions.len()}</span>
                <span class="spacer"></span>
                {(others > 0)
                    .then(|| {
                        view! {
                            <button class="btn btn--sm" on:click=move |_| end(EndSessions { id: None, others: true })>
                                <Icon name="log-out" size=14 />
                                "Sign out other sessions"
                            </button>
                        }
                    })}
            </div>
            <table class="table">
                <thead>
                    <tr>
                        <th>"Device"</th>
                        <th class="col-140">"Address"</th>
                        <th class="col-140">"Signed in"</th>
                        <th class="col-120">"Last active"</th>
                        <th class="t-actions"></th>
                    </tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
        </div>
    }
}

#[component]
fn TokensCard(
    info: AccountInfo,
    dialog: RwSignal<Option<Open>>,
    reload: impl Fn() + Copy + Send + Sync + 'static,
) -> impl IntoView {
    let app = expect_context::<App>();
    let revoke = move |token: TokenRow| {
        let name = token.name.clone();
        app.ask_first(
            format!("Revoke “{name}”?"),
            "Scripts that use this token stop working at once.",
            "",
            "Revoke",
            move |_| {
                send(
                    app,
                    "/api/account/tokens/revoke",
                    RevokeToken {
                        id: token.id.clone(),
                    },
                    move |_: serde_json::Value| {
                        app.toast(Tone::Success, "Token revoked", "");
                        reload();
                    },
                );
            },
        );
    };
    let table = if info.tokens.is_empty() {
        view! {
            <Empty
                icon="key"
                title="No access tokens"
                body="A token lets a script call the console's API as you, without your password or second factor."
            />
        }
        .into_any()
    } else {
        let rows = info
            .tokens
            .iter()
            .map(|t| {
                let token = t.clone();
                let expired = t.expires_at != 0 && t.expires_at <= (js_sys::Date::now() / 1000.0) as u64;
                view! {
                    <tr>
                        <td>{t.name.clone()}</td>
                        <td class="mono muted">{format!("{}…", t.hint)}</td>
                        <td class="muted">{date(t.created_at)}</td>
                        <td>
                            {if expired {
                                view! { <span class="badge badge--danger">"Expired"</span> }.into_any()
                            } else if t.expires_at == 0 {
                                view! { <span class="muted">"Never"</span> }.into_any()
                            } else {
                                view! { <span class="muted">{date(t.expires_at)}</span> }.into_any()
                            }}
                        </td>
                        <td class="muted">{ago(t.last_used_at)}</td>
                        <td class="t-actions">
                            <button class="btn btn--ghost btn--sm btn--danger" on:click=move |_| revoke(token.clone())>
                                "Revoke"
                            </button>
                        </td>
                    </tr>
                }
            })
            .collect_view();
        view! {
            <table class="table">
                <thead>
                    <tr>
                        <th>"Name"</th>
                        <th class="col-140">"Token"</th>
                        <th class="col-120">"Created"</th>
                        <th class="col-120">"Expires"</th>
                        <th class="col-120">"Last used"</th>
                        <th class="t-actions"></th>
                    </tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
        }
        .into_any()
    };
    view! {
        <div class="card card--flush">
            <div class="card__head">
                <Icon name="key" class="subtle" />
                <span class="card__title">"Access tokens"</span>
                <span class="badge">{info.tokens.len()}</span>
                <span class="spacer"></span>
                <button class="btn btn--sm" on:click=move |_| dialog.set(Some(Open::NewToken))>
                    <Icon name="plus" size=14 />
                    "New token"
                </button>
            </div>
            {table}
        </div>
    }
}

/// Codes in a grid, with Copy and Download.
#[component]
fn CodeList(codes: Vec<String>) -> impl IntoView {
    let app = expect_context::<App>();
    let text = codes.join("\n");
    let for_copy = text.clone();
    let for_file = text;
    view! {
        <ul class="codes">{codes.into_iter().map(|c| view! { <li class="mono">{c}</li> }).collect_view()}</ul>
        <div class="row">
            <button
                class="btn btn--sm"
                type="button"
                on:click=move |_| {
                    if api::copy_text(&for_copy) {
                        app.toast(Tone::Success, "Copied the recovery codes", "");
                    }
                }
            >
                <Icon name="copy" size=14 />
                "Copy"
            </button>
            <button
                class="btn btn--sm"
                type="button"
                on:click=move |_| {
                    let parts = js_sys::Array::of1(&wasm_bindgen::JsValue::from_str(&format!("{for_file}\n")));
                    api::download("pinkdb-recovery-codes.txt", "text/plain", &parts);
                }
            >
                <Icon name="download" size=14 />
                "Download"
            </button>
        </div>
    }
}

#[component]
fn AccountDialog(
    open: Open,
    dialog: RwSignal<Option<Open>>,
    reload: impl Fn() + Copy + Send + Sync + 'static,
) -> impl IntoView {
    let app = expect_context::<App>();
    let close = Callback::new(move |_: ()| {
        dialog.set(None);
        reload();
    });
    let input = RwSignal::new(String::new());
    let days = RwSignal::new("90".to_string());
    match open {
        Open::Enrol(e) => {
            let size = e.qr_size;
            let confirm = move |ev: leptos::ev::SubmitEvent| {
                ev.prevent_default();
                send(
                    app,
                    "/api/account/totp/confirm",
                    TotpConfirm { code: input.get_untracked() },
                    move |codes: RecoveryCodes| {
                        app.toast(Tone::Success, "Two-factor sign-in is on", "");
                        dialog.set(Some(Open::Codes(codes.codes)));
                    },
                );
            };
            let grouped: String = e
                .secret
                .chars()
                .collect::<Vec<_>>()
                .chunks(4)
                .map(|c| c.iter().collect::<String>())
                .collect::<Vec<_>>()
                .join(" ");
            view! {
                <Dialog title="Set up two-factor sign-in" on_close=close>
                    <form on:submit=confirm>
                        <div class="dialog__body">
                            <div class="enrol">
                                <svg class="qr" viewBox=format!("0 0 {size} {size}") role="img" aria-label="QR code for your authenticator app">
                                    <rect width=size height=size fill="#ffffff"></rect>
                                    <path d=e.qr_path.clone() fill="#000000"></path>
                                </svg>
                                <div class="stack">
                                    <p>"1. Scan the code with your authenticator app."</p>
                                    <p class="muted text-sm">"Can't scan it? Enter this key instead:"</p>
                                    <code class="code code--wrap">{grouped}</code>
                                    <p>"2. Enter the 6-digit code the app shows."</p>
                                    <input
                                        class="input input--lg mono login__code"
                                        autocomplete="one-time-code"
                                        inputmode="numeric"
                                        placeholder="123 456"
                                        prop:value=move || input.get()
                                        on:input=move |ev| input.set(event_target_value(&ev))
                                    />
                                </div>
                            </div>
                        </div>
                        <div class="dialog__foot">
                            <button class="btn" type="button" on:click=move |_| close.run(())>"Cancel"</button>
                            <button class="btn btn--primary" type="submit">"Turn on"</button>
                        </div>
                    </form>
                </Dialog>
            }
            .into_any()
        }
        Open::Codes(codes) => view! {
            <Dialog title="Your recovery codes" on_close=close>
                <div class="dialog__body">
                    <Callout tone=Tone::Warning title="Save these now">
                        "Each code signs you in once if you lose your phone. They are not shown again."
                    </Callout>
                    <CodeList codes />
                </div>
                <div class="dialog__foot">
                    <button class="btn btn--primary" on:click=move |_| close.run(())>"I saved them"</button>
                </div>
            </Dialog>
        }
        .into_any(),
        Open::Confirm(new_codes) => {
            let submit = move |ev: leptos::ev::SubmitEvent| {
                ev.prevent_default();
                let body = PasswordCheck { password: input.get_untracked() };
                if new_codes {
                    send(app, "/api/account/recovery", body, move |codes: RecoveryCodes| {
                        dialog.set(Some(Open::Codes(codes.codes)));
                    });
                } else {
                    send(app, "/api/account/totp/disable", body, move |_: serde_json::Value| {
                        app.toast(Tone::Success, "Two-factor sign-in is off", "");
                        close.run(());
                    });
                }
            };
            view! {
                <Dialog
                    title=if new_codes { "New recovery codes" } else { "Turn off two-factor sign-in?" }
                    size="sm"
                    on_close=close
                >
                    <form on:submit=submit>
                        <div class="dialog__body">
                            <p class="muted">
                                {if new_codes {
                                    "Your old recovery codes stop working."
                                } else {
                                    "Signing in will need only your password."
                                }}
                            </p>
                            <label class="field">
                                <span class="field__label">"Password"</span>
                                <input
                                    class="input"
                                    type="password"
                                    autocomplete="current-password"
                                    prop:value=move || input.get()
                                    on:input=move |ev| input.set(event_target_value(&ev))
                                />
                            </label>
                        </div>
                        <div class="dialog__foot">
                            <button class="btn" type="button" on:click=move |_| close.run(())>"Cancel"</button>
                            {if new_codes {
                                view! { <button class="btn btn--primary" type="submit">"Make new codes"</button> }.into_any()
                            } else {
                                view! { <button class="btn btn--danger-solid" type="submit">"Turn off"</button> }.into_any()
                            }}
                        </div>
                    </form>
                </Dialog>
            }
            .into_any()
        }
        Open::NewToken => {
            let submit = move |ev: leptos::ev::SubmitEvent| {
                ev.prevent_default();
                let body = NewToken {
                    name: input.get_untracked(),
                    days: days.get_untracked().parse().unwrap_or(90),
                };
                send(app, "/api/account/tokens", body, move |created: CreatedToken| {
                    dialog.set(Some(Open::Created(created)));
                });
            };
            view! {
                <Dialog title="New access token" size="sm" on_close=close>
                    <form on:submit=submit>
                        <div class="dialog__body">
                            <label class="field">
                                <span class="field__label">"Name"</span>
                                <input
                                    class="input"
                                    placeholder="nightly export"
                                    prop:value=move || input.get()
                                    on:input=move |ev| input.set(event_target_value(&ev))
                                />
                                <span class="field__help">"What uses it, so you know what breaks if you revoke it."</span>
                            </label>
                            <label class="field">
                                <span class="field__label">"Expires"</span>
                                <select class="input" on:change=move |ev| days.set(event_target_value(&ev))>
                                    <option value="7">"In 7 days"</option>
                                    <option value="30">"In 30 days"</option>
                                    <option value="90" selected=true>"In 90 days"</option>
                                    <option value="365">"In a year"</option>
                                    <option value="0">"Never"</option>
                                </select>
                            </label>
                            <p class="muted text-sm">"The token acts as you, with your grants. It cannot change your password, two-factor or tokens."</p>
                        </div>
                        <div class="dialog__foot">
                            <button class="btn" type="button" on:click=move |_| close.run(())>"Cancel"</button>
                            <button class="btn btn--primary" type="submit">"Create token"</button>
                        </div>
                    </form>
                </Dialog>
            }
            .into_any()
        }
        Open::Created(created) => {
            let secret = created.secret.clone();
            let for_copy = secret.clone();
            let origin = leptos::prelude::window().location().origin().unwrap_or_default();
            let example = format!(
                "curl -H 'Authorization: Bearer {secret}' {origin}/api/databases"
            );
            view! {
                <Dialog title=format!("Token “{}” created", created.token.name) on_close=close>
                    <div class="dialog__body">
                        <Callout tone=Tone::Warning title="Copy it now">
                            "This is the only time the token is shown. Store it like a password."
                        </Callout>
                        <div class="row">
                            <code class="code code--wrap token">{secret}</code>
                            <button
                                class="btn"
                                on:click=move |_| {
                                    if api::copy_text(&for_copy) {
                                        app.toast(Tone::Success, "Copied the token", "");
                                    }
                                }
                            >
                                <Icon name="copy" size=14 />
                                "Copy"
                            </button>
                        </div>
                        <div class="stack stack--tight">
                            <span class="overline">"Use it"</span>
                            <pre class="code code--wrap">{example}</pre>
                        </div>
                    </div>
                    <div class="dialog__foot">
                        <button class="btn btn--primary" on:click=move |_| close.run(())>"Done"</button>
                    </div>
                </Dialog>
            }
            .into_any()
        }
    }
}
