#!/usr/bin/env python3
"""Draw the README's simulator charts as SVG, from the CSVs in results/.

Usage:
    python3 scripts/charts.py            # reads results/, writes results/*.svg

Standard library only (no matplotlib). Each chart shows the mean across the
20 seeds with a whisker of one standard deviation, and follows the viewer's
light or dark colour scheme. The numbers behind each chart are printed, and
the README has them as tables too.

These results describe the simulator's model, not the real internet.
"""
import csv
import statistics
from collections import defaultdict
from pathlib import Path

RESULTS = Path(__file__).resolve().parent.parent / "results"
FILE_SIZE = 64 * 1024 * 1024

W, H = 640, 330
LEFT, RIGHT, TOP, BOTTOM = 64, 20, 76, 60

STYLE = """
<style>
  svg { --surface:#fcfcfb; --ink:#0b0b0b; --ink2:#52514e; --muted:#898781;
        --grid:#e1e0d9; --axis:#c3c2b7; --s1:#2a78d6; --s2:#eb6834; }
  @media (prefers-color-scheme: dark) {
    svg { --surface:#1a1a19; --ink:#f0efec; --ink2:#c3c2b7; --muted:#898781;
          --grid:#2c2c2a; --axis:#383835; --s1:#3987e5; --s2:#d95926; }
  }
  text { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; }
  .title { font-size:15px; font-weight:600; fill:var(--ink); }
  .sub { font-size:12px; fill:var(--ink2); }
  .tick { font-size:11px; fill:var(--muted); }
  .cat { font-size:12px; fill:var(--ink2); }
  .val { font-size:11px; fill:var(--ink2); }
  .legend { font-size:12px; fill:var(--ink2); }
  .note { font-size:10px; fill:var(--muted); }
  .grid { stroke:var(--grid); stroke-width:1; }
  .axis { stroke:var(--axis); stroke-width:1; }
  .whisker { stroke:var(--ink2); stroke-width:1.5; }
</style>
"""

NOTE = "Simulated swarm, 20 seeds; whiskers are ±1 standard deviation. Describes the model, not the real internet."


def per_seed_times(path, group=None):
    """Mean download time and mean of the slowest 5%, one pair per seed."""
    times = defaultdict(list)
    for row in csv.DictReader(open(path, newline="")):
        if row["is_seed"] == "true" or (group and row["group"] != group):
            continue
        if row["download_time"]:
            times[row["seed"]].append(float(row["download_time"]))
    means, slow = [], []
    for ts in times.values():
        ts.sort()
        k = max(1, (len(ts) + 19) // 20)
        means.append(statistics.mean(ts))
        slow.append(statistics.mean(ts[-k:]))
    return means, slow


def per_seed_share(path):
    """Average share of the file each downloader ended with, per seed (%)."""
    got = defaultdict(list)
    for row in csv.DictReader(open(path, newline="")):
        if row["is_seed"] != "true":
            got[row["seed"]].append(int(row["bytes_downloaded"]) / FILE_SIZE * 100)
    return [statistics.mean(v) for v in got.values()]


def stats(values):
    return statistics.mean(values), statistics.pstdev(values)


def nice_max(v, step):
    return step * (int(v / step) + 1)


def frame(title, subtitle, desc):
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" aria-labelledby="t d">',
        f"<title id='t'>{title}</title><desc id='d'>{desc}</desc>",
        STYLE,
        f'<rect width="{W}" height="{H}" rx="8" fill="var(--surface)"/>',
        f'<text class="title" x="{LEFT}" y="26">{title}</text>',
        f'<text class="sub" x="{LEFT}" y="45">{subtitle}</text>',
        f'<text class="note" x="{LEFT}" y="{H - 12}">{NOTE}</text>',
    ]


def y_axis(out, ymax, step, label, fmt="{:.0f}"):
    plot_h = H - TOP - BOTTOM
    v = 0
    while v <= ymax + 1e-9:
        y = TOP + plot_h - v / ymax * plot_h
        cls = "axis" if v == 0 else "grid"
        out.append(f'<line class="{cls}" x1="{LEFT}" x2="{W - RIGHT}" y1="{y:.1f}" y2="{y:.1f}"/>')
        out.append(f'<text class="tick" x="{LEFT - 8}" y="{y + 4:.1f}" text-anchor="end">{fmt.format(v)}</text>')
        v += step
    out.append(
        f'<text class="tick" transform="translate(16 {TOP + plot_h / 2:.0f}) rotate(-90)" text-anchor="middle">{label}</text>'
    )


def bar(out, x, w, value, sd, ymax, color, value_label):
    """A vertical bar with a rounded top, an sd whisker and a value label."""
    plot_h = H - TOP - BOTTOM
    base = TOP + plot_h
    h = value / ymax * plot_h
    r = min(4, w / 2, h)
    top = base - h
    out.append(
        f'<path d="M{x:.1f},{base:.1f} V{top + r:.1f} Q{x:.1f},{top:.1f} {x + r:.1f},{top:.1f} '
        f'H{x + w - r:.1f} Q{x + w:.1f},{top:.1f} {x + w:.1f},{top + r:.1f} V{base:.1f} Z" fill="var({color})">'
        f"<title>{value_label}</title></path>"
    )
    cx = x + w / 2
    y1 = base - (value + sd) / ymax * plot_h
    y2 = base - max(0.0, value - sd) / ymax * plot_h
    out.append(f'<line class="whisker" x1="{cx:.1f}" x2="{cx:.1f}" y1="{y1:.1f}" y2="{y2:.1f}"/>')
    out.append(f'<line class="whisker" x1="{cx - 4:.1f}" x2="{cx + 4:.1f}" y1="{y1:.1f}" y2="{y1:.1f}"/>')
    out.append(f'<text class="val" x="{cx:.1f}" y="{y1 - 6:.1f}" text-anchor="middle">{value_label.split(": ")[-1]}</text>')


def legend(out, items):
    x = W - RIGHT
    for name, color in reversed(items):
        width = 8 * len(name) + 22
        x -= width
        out.append(f'<rect x="{x}" y="35" width="12" height="12" rx="2" fill="var({color})"/>')
        out.append(f'<text class="legend" x="{x + 18}" y="45">{name}</text>')


def grouped_chart(path, title, subtitle, groups, series, ymax, step, unit_label):
    """groups: [(name, [(mean, sd) per series])]; series: [(name, color)]."""
    out = frame(title, subtitle, f"{title}. {subtitle}")
    y_axis(out, ymax, step, unit_label)
    legend(out, series)
    plot_w = W - LEFT - RIGHT
    slot = plot_w / len(groups)
    bw = min(56, slot * 0.3)
    gap = 2
    for gi, (gname, vals) in enumerate(groups):
        cx = LEFT + slot * (gi + 0.5)
        total = len(vals) * bw + (len(vals) - 1) * gap
        for si, (m, sd) in enumerate(vals):
            x = cx - total / 2 + si * (bw + gap)
            sname, color = series[si]
            bar(out, x, bw, m, sd, ymax, color, f"{gname}, {sname}: {m:.0f} s")
        out.append(f'<text class="cat" x="{cx:.1f}" y="{H - BOTTOM + 20}" text-anchor="middle">{gname}</text>')
    out.append("</svg>")
    path.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"wrote {path}")


def share_chart(path, title, subtitle, groups):
    out = frame(title, subtitle, f"{title}. {subtitle}")
    y_axis(out, 100, 20, "share of the file (%)", "{:.0f}%")
    plot_w = W - LEFT - RIGHT
    slot = plot_w / len(groups)
    bw = min(72, slot * 0.4)
    for gi, (gname, (m, sd)) in enumerate(groups):
        cx = LEFT + slot * (gi + 0.5)
        bar(out, cx - bw / 2, bw, m, sd, 100, "--s1", f"{gname}: {m:.1f}%")
        out.append(f'<text class="cat" x="{cx:.1f}" y="{H - BOTTOM + 20}" text-anchor="middle">{gname}</text>')
    out.append("</svg>")
    path.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"wrote {path}")


def main():
    strategies = [("Rarest first", "rarest_first"), ("Random", "random"), ("Sequential", "sequential")]

    groups = []
    for name, key in strategies:
        m, s = per_seed_times(RESULTS / f"exp1_{key}.csv")
        groups.append((name, [stats(m), stats(s)]))
        print(f"exp1 {name:13s} mean {stats(m)[0]:7.1f} ± {stats(m)[1]:.1f}  slowest 5% {stats(s)[0]:7.1f} ± {stats(s)[1]:.1f}")
    top = max(m + sd for _, vals in groups for m, sd in vals)
    grouped_chart(
        RESULTS / "exp1_piece_strategies.svg",
        "1. Piece strategies",
        "Download time, 1 seeder and 200 downloaders",
        groups,
        [("Mean", "--s1"), ("Slowest 5%", "--s2")],
        nice_max(top * 1.08, 200),
        200,
        "download time (s)",
    )

    groups = []
    for name, key in strategies:
        v = stats(per_seed_share(RESULTS / f"exp2_{key}.csv"))
        groups.append((name, v))
        print(f"exp2 {name:13s} share {v[0]:5.1f}% ± {v[1]:.1f}")
    share_chart(
        RESULTS / "exp2_seeder_leaves.svg",
        "2. Seeder leaves after one copy",
        "Share of the file still in the swarm; no downloader finished",
        groups,
    )

    chokes = [("Tit-for-tat", "tit_for_tat"), ("Random", "random"), ("No reward", "no_reward")]
    groups = []
    for name, key in chokes:
        up, _ = per_seed_times(RESULTS / f"exp3_{key}.csv", "uploader")
        free, _ = per_seed_times(RESULTS / f"exp3_{key}.csv", "free_rider")
        groups.append((name, [stats(up), stats(free)]))
        print(f"exp3 {name:13s} uploaders {stats(up)[0]:7.1f} ± {stats(up)[1]:.1f}  free riders {stats(free)[0]:7.1f} ± {stats(free)[1]:.1f}")
    top = max(m + sd for _, vals in groups for m, sd in vals)
    grouped_chart(
        RESULTS / "exp3_free_riders.svg",
        "3. Free riders, by choke strategy",
        "Mean download time; 20% of peers upload nothing",
        groups,
        [("Uploaders", "--s1"), ("Free riders", "--s2")],
        nice_max(top * 1.08, 100),
        100,
        "download time (s)",
    )


if __name__ == "__main__":
    main()
