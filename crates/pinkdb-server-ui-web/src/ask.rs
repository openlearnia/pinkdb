//! The ask box: a question to the data agent, answered live — every
//! statement the model ran (with a "copy into worksheet" button), its
//! progress lines and its answer.

use leptos::prelude::*;
use leptos::task::spawn_local;
use pinkdb_server_ui_api::{
    AgentInfo, AskEvent, AskRequest, ErrorEvent, TextEvent, ToolEvent, take_sse_frames,
};

use crate::api;
use crate::icon::Icon;
use crate::state::{App, Tone};
use crate::ui::{Callout, Empty};

/// Decode one frame of the stream.
pub fn decode(name: &str, data: &str) -> Option<AskEvent> {
    Some(match name {
        "progress" => AskEvent::Progress(serde_json::from_str::<TextEvent>(data).ok()?.text),
        "answer" => AskEvent::Answer(serde_json::from_str::<TextEvent>(data).ok()?.text),
        "tool" => {
            let t: ToolEvent = serde_json::from_str(data).ok()?;
            AskEvent::Tool {
                sql: t.sql,
                ok: t.ok,
                reply: t.reply,
            }
        }
        "done" => AskEvent::Done,
        "error" => AskEvent::Error(serde_json::from_str::<ErrorEvent>(data).ok()?.message),
        _ => return None,
    })
}

#[component]
pub fn AskPanel() -> impl IntoView {
    let app = expect_context::<App>();
    let info = RwSignal::new(None::<AgentInfo>);
    let question = RwSignal::new(String::new());
    let provider = RwSignal::new(String::new());
    let model = RwSignal::new(String::new());
    let events = RwSignal::new(Vec::<AskEvent>::new());
    let asking = RwSignal::new_local(None::<web_sys::AbortController>);
    let failure = RwSignal::new(None::<String>);
    // The question being answered (the box clears once it is asked).
    let asked = RwSignal::new(String::new());

    spawn_local(async move {
        match api::get::<AgentInfo>("/api/agent").await {
            Ok(agent) => {
                // No default provider (wave 106b): only the server's own
                // PINKDB_AGENT_PROVIDER preselects one; otherwise the user
                // picks.
                if let Some(p) = &agent.providers
                    && let Some(default) = &p.default
                {
                    provider.set(default.clone());
                }
                info.set(Some(agent));
            }
            Err(e) => failure.set(Some(e.message)),
        }
    });

    let ask = move || {
        let q = question.get_untracked();
        if q.trim().is_empty() || asking.get_untracked().is_some() {
            return;
        }
        if provider.get_untracked().is_empty() {
            failure.set(Some("choose a provider first".to_string()));
            return;
        }
        let Ok(controller) = web_sys::AbortController::new() else {
            return;
        };
        let signal = controller.signal();
        asking.set(Some(controller));
        events.set(Vec::new());
        failure.set(None);
        asked.set(q.clone());
        question.set(String::new());
        let request = AskRequest {
            question: q,
            provider: Some(provider.get_untracked()).filter(|p| !p.is_empty()),
            model: Some(model.get_untracked()).filter(|m| !m.trim().is_empty()),
        };
        let csrf = app.csrf();
        spawn_local(async move {
            match api::post_raw("/api/ask", &request, &csrf, Some(&signal)).await {
                Ok(response) => {
                    let mut buffer = String::new();
                    let streamed = api::read_stream(&response, |text| {
                        buffer.push_str(text);
                        for (name, data) in take_sse_frames(&mut buffer) {
                            if let Some(event) = decode(&name, &data) {
                                events.update(|list| match (list.last_mut(), &event) {
                                    // Answer text arrives in pieces: one block.
                                    (Some(AskEvent::Answer(prev)), AskEvent::Answer(more)) => {
                                        prev.push_str(more)
                                    }
                                    _ => list.push(event),
                                });
                            }
                        }
                    })
                    .await;
                    if let Err(e) = streamed
                        && asking.get_untracked().is_some()
                    {
                        failure.set(Some(e.message));
                    }
                }
                Err(e) => failure.set(Some(e.message)),
            }
            asking.set(None);
        });
    };
    let stop = move || {
        if let Some(controller) = asking.get_untracked() {
            controller.abort();
        }
        asking.set(None);
        events.update(|l| l.push(AskEvent::Error("stopped".to_string())));
    };

    let setup = move || {
        let agent = info.get()?;
        if !agent.loaded {
            return Some(
                view! {
                    <Callout tone=Tone::Warning title="The data agent is not loaded">
                        {agent.hint.clone().unwrap_or_default()}
                    </Callout>
                }
                .into_any(),
            );
        }
        let providers = agent
            .providers
            .clone()
            .map(|p| p.providers)
            .unwrap_or_default();
        if !providers.iter().any(|p| p.ready) {
            let lines = providers
                .iter()
                .filter(|p| !p.key_variables.is_empty())
                .map(|p| format!("{}: {}", p.name, p.key_variables.join(" or ")))
                .collect::<Vec<_>>()
                .join("\n");
            return Some(
                view! {
                    <Callout tone=Tone::Warning title="No model provider is configured">
                        "pinkdb has no default provider: set PINKDB_AGENT_PROVIDER and the provider's \
                         key variable in the server's environment, then restart the server. Keys stay \
                         on the server; this page never sees them."
                        <pre class="code">{lines}</pre>
                    </Callout>
                }
                .into_any(),
            );
        }
        None
    };

    // The question box and the provider: only once a provider is ready.
    let composer = move || {
        let agent = info.get()?;
        let providers = agent
            .providers
            .clone()
            .map(|p| p.providers)
            .unwrap_or_default();
        if !agent.loaded || !providers.iter().any(|p| p.ready) {
            return None;
        }
        let options = providers
            .into_iter()
            .map(|p| {
                let name = p.name.clone();
                let label = match (p.ready, p.default_model.is_empty()) {
                    (false, _) => format!("{} — not configured", p.name),
                    (true, true) => format!("{} (name a model)", p.name),
                    (true, false) => format!("{} · {}", p.name, p.default_model),
                };
                view! {
                    <option value=name.clone() disabled=!p.ready selected=move || provider.get() == name>
                        {label}
                    </option>
                }
            })
            .collect_view();
        Some(view! {
            <div class="inspector__foot">
                <textarea
                    class="input"
                    rows="3"
                    aria-label="Question"
                    placeholder="Ask about your data…"
                    prop:value=move || question.get()
                    on:input=move |ev| question.set(event_target_value(&ev))
                    on:keydown=move |ev: web_sys::KeyboardEvent| {
                        if (ev.ctrl_key() || ev.meta_key()) && ev.key() == "Enter" {
                            ev.prevent_default();
                            ask();
                        }
                    }
                ></textarea>
                <div class="row">
                    <select
                        class="input input--sm ask__provider"
                        aria-label="Provider"
                        on:change=move |ev| provider.set(event_target_value(&ev))
                    >
                        <option value="" disabled=true selected=move || provider.get().is_empty()>
                            "Choose a provider…"
                        </option>
                        {options}
                    </select>
                    <input
                        class="input input--sm ask__model"
                        placeholder="Model"
                        aria-label="Model (optional)"
                        prop:value=move || model.get()
                        on:input=move |ev| model.set(event_target_value(&ev))
                    />
                    {move || {
                        if asking.get().is_some() {
                            view! { <button class="btn btn--sm" on:click=move |_| stop()>"Stop"</button> }.into_any()
                        } else {
                            view! {
                                <button class="btn btn--primary btn--sm" on:click=move |_| ask()>
                                    <Icon name="send" size=14 />
                                    "Ask"
                                </button>
                            }
                                .into_any()
                        }
                    }}
                </div>
            </div>
        })
    };

    let transcript = move || {
        let events = events.get();
        let question = asked.get();
        if events.is_empty() && question.is_empty() {
            return view! {
                <Empty
                    icon="sparkles"
                    title="Ask about your data"
                    body="The agent reads the catalog, runs queries as you and answers. Every query it runs appears here, ready to copy into a worksheet."
                />
            }
            .into_any();
        }
        let items = events
            .into_iter()
            .map(|event| match event {
                AskEvent::Progress(text) => view! {
                    <div class="progress-line"><Icon name="check" size=14 />{text}</div>
                }
                .into_any(),
                AskEvent::Answer(text) => view! {
                    <div class="msg">
                        <div class="msg__who"><Icon name="sparkles" size=14 />"Agent"</div>
                        <div class="msg__text msg__text--pre">{text}</div>
                    </div>
                }
                .into_any(),
                AskEvent::Tool { sql, ok, reply } => {
                    let copy = format!("{}\n", sql.trim_end());
                    view! {
                        <div class="step" class:step--failed=!ok>
                            <div class="step__head">
                                {if ok {
                                    view! { <Icon name="check-circle" size=14 class="step__ok" /> }.into_any()
                                } else {
                                    view! { <Icon name="alert-circle" size=14 class="step__bad" /> }.into_any()
                                }}
                                {if ok { "Ran a query" } else { "A query failed" }}
                                <span class="spacer"></span>
                                <button
                                    class="btn btn--ghost btn--sm"
                                    title="Insert the SQL into the worksheet"
                                    on:click=move |_| app.insert.set(Some(copy.clone()))
                                >
                                    <Icon name="insert" size=14 />
                                    "Insert"
                                </button>
                            </div>
                            <pre class="code">{sql}</pre>
                            <pre class="code step__reply">{reply}</pre>
                        </div>
                    }
                    .into_any()
                }
                AskEvent::Done => ().into_any(),
                AskEvent::Error(message) => view! {
                    <Callout tone=Tone::Danger>{message}</Callout>
                }
                .into_any(),
            })
            .collect_view();
        view! {
            {(!question.is_empty())
                .then(|| {
                    view! {
                        <div class="msg msg--user">
                            <div class="msg__who"><Icon name="user" size=14 />"You"</div>
                            <div class="msg__text">{question}</div>
                        </div>
                    }
                })}
            {items}
            {move || {
                asking
                    .get()
                    .is_some()
                    .then(|| view! { <div class="progress-line"><span class="spinner spinner--sm"></span>"Working…"</div> })
            }}
        }
        .into_any()
    };

    let mode = move || {
        info.get().filter(|a| a.loaded).map(|agent| {
            let label = if agent.allow_writes {
                "May write"
            } else {
                "Read-only"
            };
            let memory = agent
                .memory
                .then(|| view! { <span class="badge badge--outline">"Memory"</span> });
            view! {
                <span class="badge badge--outline">{label}</span>
                {memory}
            }
        })
    };

    view! {
        <aside class="inspector" aria-label="Ask the data agent">
            <div class="panel-head">
                <span class="panel-head__title"><Icon name="sparkles" />"Ask"</span>
                {mode}
                <button
                    class="btn btn--ghost btn--icon btn--sm"
                    aria-label="New conversation"
                    title="New conversation"
                    on:click=move |_| {
                        if asking.get_untracked().is_none() {
                            events.set(Vec::new());
                            asked.set(String::new());
                            failure.set(None);
                        }
                    }
                >
                    <Icon name="plus" size=14 />
                </button>
                <button class="btn btn--ghost btn--icon btn--sm" aria-label="Close" on:click=move |_| app.ask_open.set(false)>
                    <Icon name="x" size=14 />
                </button>
            </div>
            <div class="inspector__body">
                {setup}
                {transcript}
                {move || failure.get().map(|f| view! { <Callout tone=Tone::Danger>{f}</Callout> })}
            </div>
            {composer}
        </aside>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_decode() {
        assert_eq!(
            decode(
                "tool",
                "{\"sql\":\"SELECT 1\",\"ok\":true,\"reply\":\"| 1 |\"}"
            ),
            Some(AskEvent::Tool {
                sql: "SELECT 1".into(),
                ok: true,
                reply: "| 1 |".into()
            })
        );
        assert_eq!(decode("done", "{\"ok\":true}"), Some(AskEvent::Done));
        assert_eq!(decode("nope", "{}"), None);
        assert_eq!(decode("answer", "not json"), None);
    }
}
