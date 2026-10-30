//! A chart of a result: bar, line or scatter of one x and one y column,
//! drawn as SVG elements (no canvas, no chart library).

use leptos::prelude::*;
use pinkdb_server_ui_api::StatementResult;

use crate::state::numeric;

const WIDTH: f64 = 720.0;
const HEIGHT: f64 = 320.0;
const LEFT: f64 = 64.0;
const RIGHT: f64 = 16.0;
const TOP: f64 = 16.0;
const BOTTOM: f64 = 48.0;
/// Bars beyond this are left out (the rest of the grid is still there).
const MAX_BARS: usize = 200;
/// Points beyond this are left out.
const MAX_POINTS: usize = 5000;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    Bar,
    Line,
    Scatter,
}

/// Round tick values covering `lo..=hi`.
pub fn ticks(lo: f64, hi: f64, count: usize) -> Vec<f64> {
    if !(lo.is_finite() && hi.is_finite()) || count == 0 {
        return Vec::new();
    }
    let span = (hi - lo).abs().max(f64::EPSILON);
    let raw = span / count as f64;
    let magnitude = 10f64.powf(raw.log10().floor());
    let step = [1.0, 2.0, 5.0, 10.0]
        .iter()
        .map(|m| m * magnitude)
        .find(|s| *s >= raw)
        .unwrap_or(raw);
    let mut out = Vec::new();
    let mut v = (lo / step).floor() * step;
    while v <= hi + step * 1e-9 && out.len() <= count * 3 {
        if v >= lo - step * 1e-9 {
            out.push(v);
        }
        v += step;
    }
    out
}

/// A tick label: short, no trailing zeros.
fn label(v: f64) -> String {
    let a = v.abs();
    if a >= 1e9 {
        format!("{:.1}G", v / 1e9)
    } else if a >= 1e6 {
        format!("{:.1}M", v / 1e6)
    } else if a >= 1e4 {
        format!("{:.0}k", v / 1e3)
    } else if v.fract() == 0.0 {
        format!("{v:.0}")
    } else {
        format!("{v:.2}")
    }
}

fn short(text: &str, n: usize) -> String {
    if text.chars().count() > n {
        format!("{}…", text.chars().take(n - 1).collect::<String>())
    } else {
        text.to_string()
    }
}

#[component]
pub fn Chart(result: StatementResult) -> impl IntoView {
    let numeric_columns: Vec<usize> = result
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| numeric(&c.dtype))
        .map(|(i, _)| i)
        .collect();
    if numeric_columns.is_empty() || result.rows.is_empty() {
        return view! {
            <div class="empty">"A chart needs a numeric column and at least one row."</div>
        }
        .into_any();
    }
    let kind = RwSignal::new(Kind::Bar);
    let x = RwSignal::new(if result.columns.len() > 1 && numeric_columns[0] == 0 {
        // A first numeric column is more likely a key than a measure when
        // another numeric column follows.
        0
    } else {
        (0..result.columns.len())
            .find(|i| !numeric_columns.contains(i))
            .unwrap_or(0)
    });
    let y = RwSignal::new(
        numeric_columns
            .iter()
            .copied()
            .find(|c| *c != x.get_untracked())
            .unwrap_or(numeric_columns[0]),
    );
    let names: Vec<String> = result.columns.iter().map(|c| c.name.clone()).collect();
    let rows = StoredValue::new(result.rows.clone());
    let x_options = names
        .iter()
        .enumerate()
        .map(|(i, n)| view! { <option value=i.to_string() selected=move || x.get() == i>{n.clone()}</option> })
        .collect_view();
    let y_options = numeric_columns
        .iter()
        .map(|&i| {
            let n = names[i].clone();
            view! { <option value=i.to_string() selected=move || y.get() == i>{n}</option> }
        })
        .collect_view();

    let plot = move || {
        let (xi, yi, k) = (x.get(), y.get(), kind.get());
        let points: Vec<(String, f64)> = rows.with_value(|rows| {
            rows.iter()
                .take(if k == Kind::Bar { MAX_BARS } else { MAX_POINTS })
                .filter_map(|row| {
                    let label = row.get(xi)?.clone().unwrap_or_else(|| "NULL".to_string());
                    let value = row.get(yi)?.as_deref()?.parse::<f64>().ok()?;
                    Some((label, value))
                })
                .collect()
        });
        if points.is_empty() {
            return view! { <div class="empty">"No numeric values to plot."</div> }.into_any();
        }
        let xs: Option<Vec<f64>> = (k != Kind::Bar)
            .then(|| points.iter().map(|(l, _)| l.parse::<f64>().ok()).collect())
            .flatten();
        let (mut y_lo, mut y_hi) = points
            .iter()
            .fold((f64::MAX, f64::MIN), |(lo, hi), (_, v)| {
                (lo.min(*v), hi.max(*v))
            });
        if k == Kind::Bar {
            y_lo = y_lo.min(0.0);
            y_hi = y_hi.max(0.0);
        }
        if y_hi == y_lo {
            y_hi = y_lo + 1.0;
        }
        let plot_w = WIDTH - LEFT - RIGHT;
        let plot_h = HEIGHT - TOP - BOTTOM;
        let sy = move |v: f64| TOP + plot_h - (v - y_lo) / (y_hi - y_lo) * plot_h;
        let n = points.len();
        // x positions: numeric x on a scale, else evenly by row.
        let (x_lo, x_hi) = xs.as_ref().map_or((0.0, 1.0), |xs| {
            let lo = xs.iter().copied().fold(f64::MAX, f64::min);
            let hi = xs.iter().copied().fold(f64::MIN, f64::max);
            (lo, if hi == lo { lo + 1.0 } else { hi })
        });
        let sx = |i: usize| -> f64 {
            match &xs {
                Some(xs) => LEFT + (xs[i] - x_lo) / (x_hi - x_lo) * plot_w,
                None => LEFT + (i as f64 + 0.5) / n as f64 * plot_w,
            }
        };
        let grid = ticks(y_lo, y_hi, 5)
            .into_iter()
            .map(|t| {
                let yy = sy(t);
                view! {
                    <line class="gridline" x1=LEFT x2=WIDTH - RIGHT y1=yy y2=yy></line>
                    <text class="tick" x=LEFT - 6.0 y=yy + 4.0 text-anchor="end">{label(t)}</text>
                }
            })
            .collect_view();
        let x_labels = match &xs {
            Some(_) => ticks(x_lo, x_hi, 6)
                .into_iter()
                .map(|t| {
                    let xx = LEFT + (t - x_lo) / (x_hi - x_lo) * plot_w;
                    view! { <text class="tick" x=xx y=HEIGHT - BOTTOM + 16.0 text-anchor="middle">{label(t)}</text> }
                })
                .collect_view()
                .into_any(),
            None => {
                let every = n.div_ceil(12).max(1);
                points
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| i % every == 0)
                    .map(|(i, (l, _))| {
                        let xx = sx(i);
                        view! { <text class="tick" x=xx y=HEIGHT - BOTTOM + 16.0 text-anchor="middle">{short(l, 12)}</text> }
                    })
                    .collect_view()
                    .into_any()
            }
        };
        let marks = match k {
            Kind::Bar => {
                let w = (plot_w / n as f64 * 0.8).max(1.0);
                let zero = sy(0.0);
                points
                    .iter()
                    .enumerate()
                    .map(|(i, (l, v))| {
                        let yy = sy(*v);
                        let title = format!("{l}: {v}");
                        view! {
                            <rect class="mark" x=sx(i) - w / 2.0 y=yy.min(zero) width=w height=(zero - yy).abs().max(0.5)>
                                <title>{title}</title>
                            </rect>
                        }
                    })
                    .collect_view()
                    .into_any()
            }
            Kind::Line => {
                let mut order: Vec<usize> = (0..n).collect();
                if xs.is_some() {
                    order.sort_by(|a, b| sx(*a).total_cmp(&sx(*b)));
                }
                let path = order
                    .iter()
                    .map(|&i| format!("{:.1},{:.1}", sx(i), sy(points[i].1)))
                    .collect::<Vec<_>>()
                    .join(" ");
                view! { <polyline class="line" points=path></polyline> }.into_any()
            }
            Kind::Scatter => points
                .iter()
                .enumerate()
                .map(|(i, (l, v))| {
                    let title = format!("{l}: {v}");
                    view! { <circle class="mark" cx=sx(i) cy=sy(*v) r=3.0><title>{title}</title></circle> }
                })
                .collect_view()
                .into_any(),
        };
        view! {
            <svg class="chart" viewBox=format!("0 0 {WIDTH} {HEIGHT}") role="img">
                {grid}
                <line class="axis" x1=LEFT x2=WIDTH - RIGHT y1=HEIGHT - BOTTOM y2=HEIGHT - BOTTOM></line>
                {x_labels}
                {marks}
            </svg>
        }
        .into_any()
    };
    let kind_button = move |name: &'static str, k: Kind| {
        view! {
            <button class="seg" class:active=move || kind.get() == k on:click=move |_| kind.set(k)>
                {name}
            </button>
        }
    };
    view! {
        <div class="chart-wrap">
            <div class="chart-controls">
                <span class="segs">
                    {kind_button("Bar", Kind::Bar)}
                    {kind_button("Line", Kind::Line)}
                    {kind_button("Scatter", Kind::Scatter)}
                </span>
                <label>
                    "x "
                    <select on:change=move |ev| {
                        if let Ok(i) = event_target_value(&ev).parse() {
                            x.set(i)
                        }
                    }>{x_options}</select>
                </label>
                <label>
                    "y "
                    <select on:change=move |ev| {
                        if let Ok(i) = event_target_value(&ev).parse() {
                            y.set(i)
                        }
                    }>{y_options}</select>
                </label>
            </div>
            {plot}
        </div>
    }
    .into_any()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticks_are_round() {
        assert_eq!(
            ticks(0.0, 100.0, 5),
            vec![0.0, 20.0, 40.0, 60.0, 80.0, 100.0]
        );
        assert_eq!(ticks(0.0, 1.0, 4), vec![0.0, 0.5, 1.0]);
        assert!(ticks(f64::NAN, 1.0, 4).is_empty());
        assert_eq!(label(25_000.0), "25k");
        assert_eq!(label(1.5), "1.50");
        assert_eq!(short("abcdefghijklmnop", 5), "abcd…");
    }
}
