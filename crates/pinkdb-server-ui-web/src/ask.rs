//! The ask box: a question to the data agent, answered live — every
//! statement the model ran (with a "copy into worksheet" button), its
//! progress lines and its answer.

use leptos::prelude::*;
use leptos::task::spawn_local;
use pinkdb_server_ui_api::{
    AgentInfo, AskEvent, AskRequest, ErrorEvent, TextEvent, ToolEvent, take_sse_frames,
};

use crate::api;
use crate::state::App;

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
                    <div class="notice">
                        "The data agent is not loaded on this server. "
                        {agent.hint.clone().unwrap_or_default()}
                    </div>
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
                    <div class="notice">
                        <p>
                            "No model provider is configured. pinkdb has no default provider: \
                             set PINKDB_AGENT_PROVIDER and the provider's key variable in the \
                             server's environment, then restart the server. Keys stay on the \
                             server; this page never sees them."
                        </p>
                        <pre>{lines}</pre>
                    </div>
                }
                .into_any(),
            );
        }
        let options = providers
            .into_iter()
            .map(|p| {
                let name = p.name.clone();
                let label = match (p.ready, p.default_model.is_empty()) {
                    (false, _) => format!("{} — not configured", p.name),
                    (true, true) => format!("{} (name a model)", p.name),
                    (true, false) => format!("{} ({})", p.name, p.default_model),
                };
                view! {
                    <option value=name.clone() disabled=!p.ready selected=move || provider.get() == name>
                        {label}
                    </option>
                }
            })
            .collect_view();
        let mode = if agent.allow_writes {
            "may write"
        } else {
            "read-only"
        };
        let memory = if agent.memory { " · memory on" } else { "" };
        Some(
            view! {
                <div class="ask-config">
                    <select on:change=move |ev| provider.set(event_target_value(&ev))>
                        <option value="" disabled=true selected=move || provider.get().is_empty()>
                            "choose a provider…"
                        </option>
                        {options}
                    </select>
                    <input
                        class="model"
                        placeholder="model (optional)"
                        prop:value=move || model.get()
                        on:input=move |ev| model.set(event_target_value(&ev))
                    />
                    <span class="hint">{format!("{mode}{memory}")}</span>
                </div>
                <textarea
                    class="question"
                    placeholder="Ask about your data… (Ctrl/Cmd+Enter)"
                    prop:value=move || question.get()
                    on:input=move |ev| question.set(event_target_value(&ev))
                    on:keydown=move |ev: web_sys::KeyboardEvent| {
                        if (ev.ctrl_key() || ev.meta_key()) && ev.key() == "Enter" {
                            ev.prevent_default();
                            ask();
                        }
                    }
                ></textarea>
                <div class="ask-actions">
                    {move || {
                        if asking.get().is_some() {
                            view! { <button on:click=move |_| stop()>"Stop"</button> }.into_any()
                        } else {
                            view! { <button class="primary" on:click=move |_| ask()>"Ask"</button> }
                                .into_any()
                        }
                    }}
                </div>
            }
            .into_any(),
        )
    };

    let transcript = move || {
        events
            .get()
            .into_iter()
            .map(|event| match event {
                AskEvent::Progress(text) => view! { <div class="ev progress">{text}</div> }.into_any(),
                AskEvent::Answer(text) => view! { <div class="ev answer">{text}</div> }.into_any(),
                AskEvent::Tool { sql, ok, reply } => {
                    let copy = format!("{}\n", sql.trim_end());
                    view! {
                        <div class="ev tool" class:bad=!ok>
                            <div class="tool-head">
                                <span>{if ok { "ran" } else { "failed" }}</span>
                                <button class="mini" on:click=move |_| app.insert.set(Some(copy.clone()))>
                                    "Copy SQL into worksheet"
                                </button>
                            </div>
                            <pre class="tool-sql">{sql}</pre>
                            <pre class="tool-reply">{reply}</pre>
                        </div>
                    }
                    .into_any()
                }
                AskEvent::Done => view! { <div class="ev done">"done"</div> }.into_any(),
                AskEvent::Error(message) => {
                    view! { <div class="ev error">{message}</div> }.into_any()
                }
            })
            .collect_view()
    };

    view! {
        <aside class="askpanel">
            <div class="ask-head">
                <span>"Ask the data agent"</span>
                <button class="mini" on:click=move |_| app.ask_open.set(false)>"×"</button>
            </div>
            {setup}
            {move || failure.get().map(|f| view! { <div class="error-box">{f}</div> })}
            <div class="transcript">{transcript}</div>
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
