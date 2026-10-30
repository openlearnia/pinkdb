//! Shared components of the design system (docs/console-design/README.md
//! §4): the dialog frame, callouts, empty states, the confirm dialog and the
//! toasts. Buttons, badges and inputs are plain elements with the system's
//! classes.

use leptos::html;
use leptos::prelude::*;

use crate::icon::Icon;
use crate::state::{App, Tone};

/// A modal dialog: the backdrop, the frame and its head. The caller renders
/// the body (`dialog__body`) and the footer (`dialog__foot`). `Esc` and a
/// click on the backdrop close it.
#[component]
pub fn Dialog(
    #[prop(into)] title: String,
    /// `sm` (420 px), `lg` (760 px) or empty (560 px).
    #[prop(optional)]
    size: &'static str,
    #[prop(into)] on_close: Callback<()>,
    children: Children,
) -> impl IntoView {
    let overlay: NodeRef<html::Div> = NodeRef::new();
    // Focus the dialog so Esc reaches it (its first field takes over once
    // the user tabs in).
    Effect::new(move |_| {
        if let Some(el) = overlay.get() {
            let _ = el.focus();
        }
    });
    let label = title.clone();
    let class = match size {
        "sm" => "dialog dialog--sm",
        "lg" => "dialog dialog--lg",
        _ => "dialog",
    };
    view! {
        <div
            class="overlay"
            tabindex="-1"
            node_ref=overlay
            on:mousedown=move |ev| {
                if ev.target() == ev.current_target() {
                    on_close.run(());
                }
            }
            on:keydown=move |ev: web_sys::KeyboardEvent| {
                if ev.key() == "Escape" {
                    ev.stop_propagation();
                    on_close.run(());
                }
            }
        >
            <div class=class role="dialog" aria-modal="true" aria-label=label>
                <div class="dialog__head">
                    <span class="dialog__title">{title}</span>
                    <button
                        class="btn btn--ghost btn--icon btn--sm"
                        aria-label="Close"
                        on:click=move |_| on_close.run(())
                    >
                        <Icon name="x" size=14 />
                    </button>
                </div>
                {children()}
            </div>
        </div>
    }
}

/// A callout: a tinted box with an icon, an optional title and a body.
#[component]
pub fn Callout(
    tone: Tone,
    #[prop(optional, into)] title: String,
    children: Children,
) -> impl IntoView {
    let class = if tone == Tone::Info {
        "callout".to_string()
    } else {
        format!("callout callout--{}", tone.name())
    };
    view! {
        <div class=class role=if tone == Tone::Danger { "alert" } else { "status" }>
            <Icon name=tone.icon() />
            <div class="callout__body">
                {(!title.is_empty()).then(|| view! { <div class="callout__title">{title}</div> })}
                {children()}
            </div>
        </div>
    }
}

/// An empty state: icon, title, a line of help, an optional action.
#[component]
pub fn Empty(
    icon: &'static str,
    #[prop(into)] title: String,
    #[prop(optional, into)] body: String,
    #[prop(optional)] children: Option<Children>,
) -> impl IntoView {
    view! {
        <div class="empty">
            <Icon name=icon size=32 />
            <div class="empty__title">{title}</div>
            {(!body.is_empty()).then(|| view! { <div class="empty__body">{body}</div> })}
            {children.map(|c| c())}
        </div>
    }
}

/// The confirm dialog of a destructive action (`App::ask_first`).
#[component]
pub fn ConfirmDialog() -> impl IntoView {
    let app = expect_context::<App>();
    move || {
        app.confirm.get().map(|c| {
            let close = Callback::new(move |_: ()| app.confirm.set(None));
            let on_confirm = c.on_confirm;
            view! {
                <Dialog title=c.title.clone() size="sm" on_close=close>
                    <div class="dialog__body">
                        <p class="muted">{c.body.clone()}</p>
                        {(!c.sql.is_empty()).then(|| view! { <pre class="code code--wrap">{c.sql.clone()}</pre> })}
                    </div>
                    <div class="dialog__foot">
                        <button class="btn" on:click=move |_| app.confirm.set(None)>"Cancel"</button>
                        <button
                            class="btn btn--danger-solid"
                            on:click=move |_| {
                                app.confirm.set(None);
                                on_confirm.run(());
                            }
                        >
                            <Icon name="trash" size=14 />
                            {c.action.clone()}
                        </button>
                    </div>
                </Dialog>
            }
        })
    }
}

/// The toasts in the bottom-right corner.
#[component]
pub fn Toasts() -> impl IntoView {
    let app = expect_context::<App>();
    view! {
        <div class="toasts" aria-live="polite">
            <For
                each=move || app.toasts.get()
                key=|t| t.id
                children=move |t| {
                    let id = t.id;
                    view! {
                        <div class=format!("toast toast--{}", t.tone.name())>
                            <Icon name=t.tone.icon() />
                            <div class="toast__body">
                                <div class="toast__title">{t.title}</div>
                                {(!t.body.is_empty()).then(|| view! { <div class="muted">{t.body}</div> })}
                            </div>
                            <button
                                class="btn btn--ghost btn--icon btn--sm"
                                aria-label="Dismiss"
                                on:click=move |_| app.dismiss_toast(id)
                            >
                                <Icon name="x" size=14 />
                            </button>
                        </div>
                    }
                }
            />
        </div>
    }
}

/// A full-screen catcher behind an open menu: a click outside closes it.
#[component]
pub fn Scrim(open: RwSignal<bool>) -> impl IntoView {
    view! { <div class="scrim" on:mousedown=move |_| open.set(false)></div> }
}
