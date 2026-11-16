#!/usr/bin/env python3
"""Turn simulator CSV files into charts for the README.

Usage:
    python scripts/plot.py results/exp1_*.csv --out results/exp1.png \
        --title "Experiment 1: piece strategies"

Each CSV becomes one bar (labelled by file name) showing the mean download
time of downloaders across all seeds, with the spread across seeds as an
error bar, plus the mean of the slowest 5%. Needs matplotlib.

These results describe the simulator's model, not the real internet.
"""
import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path


def per_seed(path, group=None):
    times = defaultdict(list)
    for row in csv.DictReader(open(path, newline="")):
        if row["is_seed"] == "true":
            continue
        if group and row["group"] != group:
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", nargs="+")
    ap.add_argument("--out", required=True)
    ap.add_argument("--title", default="")
    ap.add_argument("--group", help="only peers of this group")
    args = ap.parse_args()

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    labels, mean, mean_sd, slow, slow_sd = [], [], [], [], []
    for p in args.csv:
        m, s = per_seed(p, args.group)
        labels.append(Path(p).stem)
        mean.append(statistics.mean(m))
        mean_sd.append(statistics.pstdev(m))
        slow.append(statistics.mean(s))
        slow_sd.append(statistics.pstdev(s))
        print(f"{labels[-1]:30s} mean {mean[-1]:8.1f}s (sd {mean_sd[-1]:.1f})  slowest 5% {slow[-1]:8.1f}s (sd {slow_sd[-1]:.1f})  runs {len(m)}")

    x = range(len(labels))
    fig, ax = plt.subplots(figsize=(max(5, 1.6 * len(labels)), 4))
    w = 0.38
    ax.bar([i - w / 2 for i in x], mean, w, yerr=mean_sd, capsize=4, label="mean")
    ax.bar([i + w / 2 for i in x], slow, w, yerr=slow_sd, capsize=4, label="slowest 5%")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=15)
    ax.set_ylabel("download time (s)")
    ax.set_title(args.title)
    ax.legend()
    fig.text(0.01, 0.01, "Simulated swarm: results describe the model, not the real internet.", fontsize=7)
    fig.tight_layout()
    fig.savefig(args.out, dpi=120)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
