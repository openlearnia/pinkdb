//! The SQL editor: a `<textarea>` over a highlighted `<pre>` (same font and
//! metrics; the textarea's text is transparent, its caret is not), with
//! run-at-cursor and catalog completion. The tokenizer, the statement
//! splitter and the completion context are `pinkdb_server_ui_api::sql`.

use leptos::html;
use leptos::prelude::*;
use pinkdb_server_ui_api::sql::{self, TokenKind};

use crate::state::{App, Sheet};

/// Editor metrics (they match `console.css`).
const LINE_HEIGHT: f64 = 20.0;
const PADDING: f64 = 10.0;

/// A UTF-16 offset (what the DOM reports) as a byte offset into `text`.
pub fn utf16_to_byte(text: &str, units: u32) -> usize {
    let mut seen = 0u32;
    for (i, c) in text.char_indices() {
        if seen >= units {
            return i;
        }
        seen += c.len_utf16() as u32;
    }
    text.len()
}

/// A byte offset as a UTF-16 offset.
pub fn byte_to_utf16(text: &str, byte: usize) -> u32 {
    text[..byte.min(text.len())]
        .chars()
        .map(|c| c.len_utf16() as u32)
        .sum()
}

/// One completion candidate.
#[derive(Clone, Debug, PartialEq)]
struct Item {
    label: String,
    kind: &'static str,
    insert: String,
}

/// The open completion popup.
#[derive(Clone, Debug, PartialEq)]
struct Popup {
    items: Vec<Item>,
    selected: usize,
    /// The byte span the accepted item replaces.
    start: usize,
    end: usize,
    left: f64,
    top: f64,
}

/// The candidates for the word at `pos`.
fn candidates(app: &App, text: &str, pos: usize) -> (usize, Vec<Item>) {
    let word = sql::word_at(text, pos);
    let prefix = word.prefix.to_ascii_lowercase();
    let statement = sql::statement_at(text, pos)
        .map(|s| s.text)
        .unwrap_or_default();
    let refs = sql::table_refs(&statement);
    let catalog = app.catalog.get_untracked();
    let db = app.db.get_untracked();
    let mut items: Vec<Item> = Vec::new();
    let mut push = |label: &str, kind: &'static str, insert: String| {
        if label.to_ascii_lowercase().starts_with(&prefix)
            && !items.iter().any(|i| i.label == label && i.kind == kind)
        {
            items.push(Item {
                label: label.to_string(),
                kind,
                insert,
            });
        }
    };
    if let Some(qualifier) = &word.qualifier {
        // `alias.` / `table.` → its columns; `db.` → its relations.
        let target = refs
            .iter()
            .find(|r| {
                r.alias
                    .as_deref()
                    .is_some_and(|a| a.eq_ignore_ascii_case(qualifier))
                    || r.name.eq_ignore_ascii_case(qualifier)
            })
            .map(|r| (r.db.clone(), r.name.clone()));
        // A qualifier that names no table of the statement and no database
        // (an alias not written yet): offer the current database's columns.
        let names_database = catalog
            .databases
            .iter()
            .any(|d| d.eq_ignore_ascii_case(qualifier));
        if target.is_none() && !names_database {
            for object in &catalog.objects {
                if object.table_name.is_none() && object.database.eq_ignore_ascii_case(&db) {
                    for column in &object.columns {
                        push(&column.name, "column", sql::quote_ident(&column.name));
                    }
                }
            }
        }
        for object in &catalog.objects {
            let is_relation = object.table_name.is_none() && !object.columns.is_empty();
            if let Some((rdb, name)) = &target
                && is_relation
                && object.name.eq_ignore_ascii_case(name)
                && rdb
                    .as_deref()
                    .is_none_or(|d| d.eq_ignore_ascii_case(&object.database))
            {
                for column in &object.columns {
                    push(&column.name, "column", sql::quote_ident(&column.name));
                }
            }
            if object.database.eq_ignore_ascii_case(qualifier)
                && matches!(
                    object.kind.as_str(),
                    "TABLE" | "VIEW" | "MATERIALIZED VIEW" | "LAKE TABLE"
                )
            {
                push(&object.name, "table", sql::quote_ident(&object.name));
            }
        }
    } else {
        for r in &refs {
            for object in &catalog.objects {
                if object.table_name.is_none() && object.name.eq_ignore_ascii_case(&r.name) {
                    for column in &object.columns {
                        push(&column.name, "column", sql::quote_ident(&column.name));
                    }
                }
            }
        }
        for object in &catalog.objects {
            if matches!(
                object.kind.as_str(),
                "TABLE" | "VIEW" | "MATERIALIZED VIEW" | "LAKE TABLE"
            ) {
                let insert = if object.database.eq_ignore_ascii_case(&db) {
                    sql::quote_ident(&object.name)
                } else {
                    format!(
                        "{}.{}",
                        sql::quote_ident(&object.database),
                        sql::quote_ident(&object.name)
                    )
                };
                push(&object.name, "table", insert);
            }
        }
        for name in &catalog.databases {
            push(name, "database", sql::quote_ident(name));
        }
        for keyword in sql::KEYWORDS {
            push(keyword, "keyword", keyword.to_ascii_uppercase());
        }
        for function in sql::FUNCTIONS {
            push(function, "function", format!("{function}("));
        }
        for t in sql::TYPES {
            push(t, "type", t.to_ascii_uppercase());
        }
    }
    items.truncate(60);
    (word.start, items)
}

/// The highlighted text, token by token.
fn highlight(text: &str) -> Vec<AnyView> {
    let mut out: Vec<AnyView> = sql::tokenize(text)
        .into_iter()
        .map(|token| {
            let piece = text[token.start..token.end].to_string();
            if token.kind == TokenKind::Whitespace {
                piece.into_any()
            } else {
                view! { <span class=token.kind.class()>{piece}</span> }.into_any()
            }
        })
        .collect();
    // A trailing newline needs a line of its own to keep the two layers
    // the same height.
    out.push(" ".into_any());
    out
}

#[component]
pub fn Editor(sheet: Sheet) -> impl IntoView {
    let app = expect_context::<App>();
    let area: NodeRef<html::Textarea> = NodeRef::new();
    let layer: NodeRef<html::Pre> = NodeRef::new();
    let measure: NodeRef<html::Span> = NodeRef::new();
    let popup: RwSignal<Option<Popup>> = RwSignal::new(None);

    let selection = move || -> (usize, usize) {
        let Some(el) = area.get_untracked() else {
            return (0, 0);
        };
        let text = el.value();
        let start = el.selection_start().ok().flatten().unwrap_or(0);
        let end = el.selection_end().ok().flatten().unwrap_or(start);
        (utf16_to_byte(&text, start), utf16_to_byte(&text, end))
    };
    let set_caret = move |text: &str, byte: usize| {
        if let Some(el) = area.get_untracked() {
            let pos = byte_to_utf16(text, byte);
            let _ = el.set_selection_range(pos, pos);
        }
    };
    // Replace the bytes `start..end` with `insert` and put the caret after.
    let splice = move |start: usize, end: usize, insert: &str| {
        let text = sheet.sql.get_untracked();
        let (start, end) = (start.min(text.len()), end.min(text.len()));
        let next = format!("{}{insert}{}", &text[..start], &text[end..]);
        if let Some(el) = area.get_untracked() {
            el.set_value(&next);
        }
        sheet.sql.set(next.clone());
        set_caret(&next, start + insert.len());
        app.edited(sheet);
    };

    let open_popup = move |force: bool| {
        let Some(el) = area.get_untracked() else {
            return;
        };
        let text = el.value();
        let (pos, end) = selection();
        if pos != end {
            popup.set(None);
            return;
        }
        let word = sql::word_at(&text, pos);
        if !force && word.prefix.is_empty() && word.qualifier.is_none() {
            popup.set(None);
            return;
        }
        let (start, items) = candidates(&app, &text, pos);
        if items.is_empty() || (items.len() == 1 && items[0].label == word.prefix && !force) {
            popup.set(None);
            return;
        }
        let before = &text[..start];
        let line = before.matches('\n').count() as f64;
        let column = before.rsplit('\n').next().map_or(0, |l| l.chars().count()) as f64;
        let char_width = measure
            .get_untracked()
            .map(|m| m.offset_width() as f64 / 10.0)
            .filter(|w| *w > 0.0)
            .unwrap_or(7.8);
        let left = PADDING + column * char_width - el.scroll_left() as f64;
        let top = PADDING + (line + 1.0) * LINE_HEIGHT - el.scroll_top() as f64;
        popup.set(Some(Popup {
            items,
            selected: 0,
            start,
            end: pos,
            left,
            top,
        }));
    };

    let accept = move |index: usize| {
        let Some(p) = popup.get_untracked() else {
            return;
        };
        if let Some(item) = p.items.get(index) {
            splice(p.start, p.end, &item.insert);
        }
        popup.set(None);
        if let Some(el) = area.get_untracked() {
            let _ = el.focus();
        }
    };

    let run = move |all: bool| {
        let text = sheet.sql.get_untracked();
        let (start, end) = selection();
        let sql = if all {
            text.clone()
        } else if start != end {
            text[start..end].to_string()
        } else {
            sql::statement_at(&text, start)
                .map(|s| s.text)
                .unwrap_or_default()
        };
        app.run(sheet, sql);
    };

    let on_keydown = move |ev: web_sys::KeyboardEvent| {
        let key = ev.key();
        let command = ev.ctrl_key() || ev.meta_key();
        if let Some(p) = popup.get_untracked() {
            match key.as_str() {
                "ArrowDown" => {
                    ev.prevent_default();
                    popup.set(Some(Popup {
                        selected: (p.selected + 1) % p.items.len(),
                        ..p
                    }));
                    return;
                }
                "ArrowUp" => {
                    ev.prevent_default();
                    popup.set(Some(Popup {
                        selected: (p.selected + p.items.len() - 1) % p.items.len(),
                        ..p
                    }));
                    return;
                }
                "Enter" | "Tab" if !command => {
                    ev.prevent_default();
                    accept(p.selected);
                    return;
                }
                "Escape" => {
                    ev.prevent_default();
                    popup.set(None);
                    return;
                }
                _ => {}
            }
        }
        if command && key == "Enter" {
            ev.prevent_default();
            popup.set(None);
            run(ev.shift_key());
        } else if command && (key == " " || key == "Spacebar") {
            ev.prevent_default();
            open_popup(true);
        } else if command && (key == "s" || key == "S") {
            ev.prevent_default();
            app.save(sheet);
        } else if key == "Tab" && !command {
            ev.prevent_default();
            let (start, end) = selection();
            splice(start, end, "  ");
        } else if key == "Escape" && sheet.running.get_untracked().is_some() {
            app.cancel(sheet);
        }
    };

    let on_input = move |_| {
        let Some(el) = area.get_untracked() else {
            return;
        };
        let text = el.value();
        sheet.sql.set(text.clone());
        app.edited(sheet);
        let (pos, _) = selection();
        let typed = text[..pos].chars().last();
        if typed.is_some_and(|c| c.is_alphanumeric() || c == '_' || c == '.') {
            let word = sql::word_at(&text, pos);
            if word.prefix.chars().count() >= 2 || word.qualifier.is_some() {
                open_popup(false);
                return;
            }
        }
        popup.set(None);
    };

    let on_scroll = move |_| {
        if let (Some(el), Some(pre)) = (area.get_untracked(), layer.get_untracked()) {
            pre.set_scroll_top(el.scroll_top());
            pre.set_scroll_left(el.scroll_left());
        }
        popup.set(None);
    };

    // Text another panel asked to insert (a catalog name, the agent's SQL).
    Effect::new(move |_| {
        if let Some(text) = app.insert.get() {
            app.insert.set(None);
            if !sheet.mine.get_untracked() {
                return;
            }
            let (start, end) = selection();
            splice(start, end, &text);
            if let Some(el) = area.get_untracked() {
                let _ = el.focus();
            }
        }
    });

    let read_only = move || !sheet.mine.get();
    view! {
        <div class="editor">
            <pre class="hl" node_ref=layer aria-hidden="true">{move || highlight(&sheet.sql.get())}</pre>
            <textarea
                node_ref=area
                class="code"
                spellcheck="false"
                autocomplete="off"
                autocapitalize="off"
                wrap="off"
                aria-label="SQL"
                prop:value=move || sheet.sql.get()
                prop:readOnly=read_only
                on:input=on_input
                on:keydown=on_keydown
                on:scroll=on_scroll
                on:blur=move |_| {
                    set_timeout(move || popup.set(None), std::time::Duration::from_millis(150))
                }
            ></textarea>
            <span class="measure" node_ref=measure aria-hidden="true">"xxxxxxxxxx"</span>
            {move || {
                popup
                    .get()
                    .map(|p| {
                        let selected = p.selected;
                        view! {
                            <ul class="completion" style:left=format!("{}px", p.left) style:top=format!("{}px", p.top)>
                                {p
                                    .items
                                    .iter()
                                    .enumerate()
                                    .map(|(i, item)| {
                                        let label = item.label.clone();
                                        let kind = item.kind;
                                        view! {
                                            <li
                                                class:selected=i == selected
                                                on:mousedown=move |ev: web_sys::MouseEvent| {
                                                    ev.prevent_default();
                                                    accept(i);
                                                }
                                            >
                                                <span class="label">{label}</span>
                                                <span class="kind">{kind}</span>
                                            </li>
                                        }
                                    })
                                    .collect_view()}
                            </ul>
                        }
                    })
            }}
        </div>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offsets_convert_both_ways() {
        let text = "a€😀b";
        assert_eq!(utf16_to_byte(text, 0), 0);
        assert_eq!(utf16_to_byte(text, 1), 1);
        assert_eq!(utf16_to_byte(text, 2), 4);
        assert_eq!(utf16_to_byte(text, 4), 8);
        assert_eq!(utf16_to_byte(text, 99), text.len());
        assert_eq!(byte_to_utf16(text, 4), 2);
        assert_eq!(byte_to_utf16(text, 8), 4);
    }
}
