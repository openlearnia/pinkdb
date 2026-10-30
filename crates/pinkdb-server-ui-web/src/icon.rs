//! The console's icon set (docs/console-design/README.md §3.6): drawn for
//! the console on a 24-unit grid, stroke only, `currentColor`. Each icon is
//! a list of SVG path data; `docs/console-design/icons.svg` is the source.

use leptos::prelude::*;

/// The path data of an icon (an unknown name draws nothing).
pub fn paths(name: &str) -> &'static [&'static str] {
    match name {
        "play" => {
            &["M7 5.5v13a1 1 0 0 0 1.5.86l10.5-6.5a1 1 0 0 0 0-1.72L8.5 4.64A1 1 0 0 0 7 5.5z"]
        }
        "stop" => &["M8 6h8a2 2 0 0 1 2 2v8a2 2 0 0 1 -2 2h-8a2 2 0 0 1 -2 -2v-8a2 2 0 0 1 2 -2z"],
        "plus" => &["M12 5v14M5 12h14"],
        "x" => &["M6.5 6.5l11 11M17.5 6.5l-11 11"],
        "check" => &["m5 12.5 4.5 4.5L19 7.5"],
        "chevron-right" => &["m9.5 6 6 6-6 6"],
        "chevron-down" => &["m6 9.5 6 6 6-6"],
        "search" => &[
            "M4.5 11a6.5 6.5 0 1 0 13 0a6.5 6.5 0 1 0 -13 0",
            "m20 20-4.2-4.2",
        ],
        "database" => &[
            "M4.5 5.5a7.5 2.5 0 1 0 15 0a7.5 2.5 0 1 0 -15 0",
            "M4.5 5.5v13c0 1.4 3.4 2.5 7.5 2.5s7.5-1.1 7.5-2.5v-13",
            "M4.5 12c0 1.4 3.4 2.5 7.5 2.5s7.5-1.1 7.5-2.5",
        ],
        "table" => &[
            "M5.5 4.5h13a2 2 0 0 1 2 2v11a2 2 0 0 1 -2 2h-13a2 2 0 0 1 -2 -2v-11a2 2 0 0 1 2 -2z",
            "M3.5 9.5h17M3.5 14.5h17M9.5 9.5v10",
        ],
        "view" => &[
            "M2.5 12S6 5.5 12 5.5 21.5 12 21.5 12 18 18.5 12 18.5 2.5 12 2.5 12z",
            "M9.5 12a2.5 2.5 0 1 0 5 0a2.5 2.5 0 1 0 -5 0",
        ],
        "layers" => &[
            "m12 3.5 8.5 4.5-8.5 4.5L3.5 8z",
            "m3.5 12 8.5 4.5 8.5-4.5",
            "m3.5 16 8.5 4.5 8.5-4.5",
        ],
        "lake" => &[
            "M3 7.5c2 0 2-1.5 4.5-1.5S9.5 7.5 12 7.5s2.5-1.5 4.5-1.5S19 7.5 21 7.5",
            "M3 12.5c2 0 2-1.5 4.5-1.5s2 1.5 4.5 1.5 2.5-1.5 4.5-1.5 2.5 1.5 4.5 1.5",
            "M3 17.5c2 0 2-1.5 4.5-1.5s2 1.5 4.5 1.5 2.5-1.5 4.5-1.5 2.5 1.5 4.5 1.5",
        ],
        "index" => &["M9.5 4 7.5 20M16.5 4l-2 16M4.5 9h15.5M4 15h15.5"],
        "bolt" => &["M13 3 5 13.5h6L10.5 21 19 10.5h-6z"],
        "function" => &[
            "M16 4.5h-1.5a3 3 0 0 0-3 3V17a3 3 0 0 1-3 3H7",
            "M8.5 10.5h7",
        ],
        "column" => &[
            "M9.5 3.5h5a1.5 1.5 0 0 1 1.5 1.5v14a1.5 1.5 0 0 1 -1.5 1.5h-5a1.5 1.5 0 0 1 -1.5 -1.5v-14a1.5 1.5 0 0 1 1.5 -1.5z",
            "M8 9h8M8 14.5h8",
        ],
        "file-code" => &[
            "M14 3.5H7a2 2 0 0 0-2 2v13a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2v-10z",
            "M14 3.5v5h5",
            "m10 12.5-2 2 2 2M14 12.5l2 2-2 2",
        ],
        "history" => &[
            "M3.5 12a8.5 8.5 0 1 0 2.5-6",
            "M3.5 4v4.5H8",
            "M12 7.5V12l3 2",
        ],
        "clock" => &[
            "M3.5 12a8.5 8.5 0 1 0 17 0a8.5 8.5 0 1 0 -17 0",
            "M12 7.5V12l3 2",
        ],
        "users" => &[
            "M5.5 8.5a3.5 3.5 0 1 0 7 0a3.5 3.5 0 1 0 -7 0",
            "M2.5 20c0-3.6 2.9-6 6.5-6s6.5 2.4 6.5 6",
            "M15.5 5.2a3.5 3.5 0 0 1 0 6.6M18 14.4c2 .8 3.5 2.8 3.5 5.6",
        ],
        "user" => &[
            "M8 8.5a4 4 0 1 0 8 0a4 4 0 1 0 -8 0",
            "M4.5 20.5c0-4 3.4-6.5 7.5-6.5s7.5 2.5 7.5 6.5",
        ],
        "shield" => &[
            "M12 3.5 4.5 6.5v5.5c0 4.5 3.2 7.6 7.5 9 4.3-1.4 7.5-4.5 7.5-9V6.5z",
            "m9 12 2 2 4-4",
        ],
        "hard-drive" => &[
            "M5.5 13h13a2 2 0 0 1 2 2v3a2 2 0 0 1 -2 2h-13a2 2 0 0 1 -2 -2v-3a2 2 0 0 1 2 -2z",
            "M5.5 13 8 5h8l2.5 8",
            "M7.5 16.5h.01M11 16.5h.01",
        ],
        "upload" => &[
            "M12 15V4M7.5 8.5 12 4l4.5 4.5",
            "M4.5 15v3a2 2 0 0 0 2 2h11a2 2 0 0 0 2-2v-3",
        ],
        "download" => &[
            "M12 4v11M7.5 10.5 12 15l4.5-4.5",
            "M4.5 15v3a2 2 0 0 0 2 2h11a2 2 0 0 0 2-2v-3",
        ],
        "copy" => &[
            "M10.5 8.5h7.5a2 2 0 0 1 2 2v7.5a2 2 0 0 1 -2 2h-7.5a2 2 0 0 1 -2 -2v-7.5a2 2 0 0 1 2 -2z",
            "M15.5 8.5V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v7.5a2 2 0 0 0 2 2h2.5",
        ],
        "sparkles" => &[
            "M10 4.5 11.6 9 16 10.5 11.6 12 10 16.5 8.4 12 4 10.5 8.4 9z",
            "M18 3v3.5M16.25 4.75h3.5M18 15.5V19M16.25 17.25h3.5",
        ],
        "more" => &["M5.5 12h.01M12 12h.01M18.5 12h.01"],
        "grid" => &[
            "M5.5 3.5h13a2 2 0 0 1 2 2v13a2 2 0 0 1 -2 2h-13a2 2 0 0 1 -2 -2v-13a2 2 0 0 1 2 -2z",
            "M3.5 9.5h17M3.5 15h17M9.5 3.5v17",
        ],
        "chart" => &["M4 20h16", "M7 16v-5M12 16V6M17 16v-8"],
        "plan" => &[
            "M4.5 3.5h6a1 1 0 0 1 1 1v3a1 1 0 0 1 -1 1h-6a1 1 0 0 1 -1 -1v-3a1 1 0 0 1 1 -1z",
            "M12.5 10h7a1 1 0 0 1 1 1v2.5a1 1 0 0 1 -1 1h-7a1 1 0 0 1 -1 -1v-2.5a1 1 0 0 1 1 -1z",
            "M12.5 16h7a1 1 0 0 1 1 1v2.5a1 1 0 0 1 -1 1h-7a1 1 0 0 1 -1 -1v-2.5a1 1 0 0 1 1 -1z",
            "M7.5 8.5v9.75h4M7.5 12.25h4",
        ],
        "gauge" => &["M4.5 18a8.5 8.5 0 1 1 15 0", "m12 14 3.5-4"],
        "sun" => &[
            "M8.5 12a3.5 3.5 0 1 0 7 0a3.5 3.5 0 1 0 -7 0",
            "M12 3v1.5M12 19.5V21M3 12h1.5M19.5 12H21M5.6 5.6l1.1 1.1M17.3 17.3l1.1 1.1M5.6 18.4l1.1-1.1M17.3 6.7l1.1-1.1",
        ],
        "moon" => &["M19.5 14.5A8 8 0 0 1 9.5 4.5a8 8 0 1 0 10 10z"],
        "log-out" => &[
            "M9.5 20H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h3.5",
            "M16 16.5 20.5 12 16 7.5M20.5 12H9.5",
        ],
        "alert" => &[
            "M10.3 4.3 2.9 17a2 2 0 0 0 1.7 3h14.8a2 2 0 0 0 1.7-3L13.7 4.3a2 2 0 0 0-3.4 0z",
            "M12 9.5v4M12 17h.01",
        ],
        "alert-circle" => &[
            "M3.5 12a8.5 8.5 0 1 0 17 0a8.5 8.5 0 1 0 -17 0",
            "M12 8v4.5M12 16h.01",
        ],
        "info" => &[
            "M3.5 12a8.5 8.5 0 1 0 17 0a8.5 8.5 0 1 0 -17 0",
            "M12 11v5M12 8h.01",
        ],
        "check-circle" => &[
            "M3.5 12a8.5 8.5 0 1 0 17 0a8.5 8.5 0 1 0 -17 0",
            "m8.5 12 2.5 2.5 4.5-5",
        ],
        "panel-left" => &[
            "M5.5 4.5h13a2 2 0 0 1 2 2v11a2 2 0 0 1 -2 2h-13a2 2 0 0 1 -2 -2v-11a2 2 0 0 1 2 -2z",
            "M9.5 4.5v15",
        ],
        "share" => &[
            "M15 5.5a2.5 2.5 0 1 0 5 0a2.5 2.5 0 1 0 -5 0",
            "M4 12a2.5 2.5 0 1 0 5 0a2.5 2.5 0 1 0 -5 0",
            "M15 18.5a2.5 2.5 0 1 0 5 0a2.5 2.5 0 1 0 -5 0",
            "m8.7 10.7 6.6-3.9M8.7 13.3l6.6 3.9",
        ],
        "key" => &[
            "M3.5 15a4.5 4.5 0 1 0 9 0a4.5 4.5 0 1 0 -9 0",
            "m11.2 11.8 8.3-8.3M16.5 6.5 19 9M14 9l2 2",
        ],
        "filter" => &["M4 5h16l-6 7.5V19l-4 1.5v-8z"],
        "refresh" => &[
            "M20 12a8 8 0 0 1-14.5 4.6M4 12a8 8 0 0 1 14.5-4.6",
            "M18.5 3.5v4h-4M5.5 20.5v-4h4",
        ],
        "trash" => &[
            "M4.5 7h15M9.5 7V4.5h5V7M6.5 7l1 12.5a1.5 1.5 0 0 0 1.5 1.5h6a1.5 1.5 0 0 0 1.5-1.5l1-12.5",
        ],
        "send" => &["M12 19V5M6 11l6-6 6 6"],
        "insert" => &["M4 12h11M11 8l4 4-4 4", "M19 5v14"],
        _ => &[],
    }
}

/// An icon: 16 px by default; `size` 14, 18 or 32 picks the other sizes.
#[component]
pub fn Icon(
    name: &'static str,
    #[prop(optional)] size: u8,
    #[prop(optional, into)] class: String,
) -> impl IntoView {
    let size_class = match size {
        14 => " icon--14",
        18 => " icon--18",
        32 => " icon--32",
        _ => "",
    };
    let class = if class.is_empty() {
        format!("icon{size_class}")
    } else {
        format!("icon{size_class} {class}")
    };
    view! {
        <svg class=class viewBox="0 0 24 24" aria-hidden="true">
            {paths(name).iter().map(|d| view! { <path d=*d></path> }).collect_view()}
        </svg>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_icon_has_paths() {
        for name in [
            "play",
            "stop",
            "plus",
            "x",
            "check",
            "chevron-right",
            "chevron-down",
            "search",
            "database",
            "table",
            "view",
            "layers",
            "lake",
            "index",
            "bolt",
            "function",
            "column",
            "file-code",
            "history",
            "clock",
            "users",
            "user",
            "shield",
            "hard-drive",
            "upload",
            "download",
            "copy",
            "sparkles",
            "more",
            "grid",
            "chart",
            "plan",
            "gauge",
            "sun",
            "moon",
            "log-out",
            "alert",
            "alert-circle",
            "info",
            "check-circle",
            "panel-left",
            "share",
            "key",
            "filter",
            "refresh",
            "trash",
            "send",
            "insert",
        ] {
            assert!(!paths(name).is_empty(), "{name}");
        }
        assert!(paths("nope").is_empty());
    }
}
