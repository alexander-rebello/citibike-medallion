#!/usr/bin/env python3
"""Draw the report's figures (PDF) from docs/report/data/gold_summary.json and config/silver_rules.toml.

  python scripts/report_data.py      # once, needs Spark and published Gold
  python scripts/report_figures.py   # no Spark; writes docs/report/figures/*.pdf
"""
import json
import os
import tomllib

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = os.path.join(os.path.dirname(__file__), "..")
REPORT = os.path.join(ROOT, "docs", "report")
FIG = os.path.join(REPORT, "figures")

# categorical slots in fixed order (blue, orange, aqua, yellow); ink colours for text
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
SEQ = ["#0d366b", "#256abf", "#6da7ec", "#b7d3f6"]  # ordinal distance bands, near = dark
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"
FOCUS = [2015, 2019, 2024]

plt.rcParams.update({
    "font.family": "serif", "font.size": 8, "axes.titlesize": 8.5, "axes.labelsize": 8,
    "axes.edgecolor": INK2, "axes.labelcolor": INK, "xtick.color": INK2, "ytick.color": INK2,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.6, "axes.axisbelow": True, "legend.frameon": False, "lines.linewidth": 1.6,
    "pdf.fonttype": 42,
})


def results(d: dict) -> None:
    fig, ax = plt.subplots(2, 2, figsize=(6.7, 4.6))

    # (a) shares by year
    a = ax[0, 0]
    yrs = [r["year"] for r in d["yearly"]]
    casual = [100 * r["casual"] / r["trips"] for r in d["yearly"]]
    ebike = [100 * r["electric"] / r["trips"] if r["year"] >= 2020 else None for r in d["yearly"]]
    a.plot(yrs, casual, color=SERIES[0], marker="o", ms=3, label="casual riders")
    e_yrs = [y for y, v in zip(yrs, ebike) if v is not None]
    a.plot(e_yrs, [v for v in ebike if v is not None], color=SERIES[1], marker="o", ms=3, label="e-bike trips")
    a.axvline(2019.5, color=INK2, lw=0.8, ls=":")
    a.text(2019.6, 4, "legacy | modern\nschema", ha="left", va="bottom", fontsize=6, color=INK2)
    a.set_xticks(range(2013, 2027, 2))
    a.set_ylim(0, 80)
    a.set_ylabel("share of trips (%)")
    a.set_title("(a) Casual and e-bike share by year", loc="left")
    a.legend(loc="upper left", fontsize=7)

    # (b) hourly profile
    b = ax[0, 1]
    for i, y in enumerate(FOCUS):
        hrs = sorted((r for r in d["hourly"] if r["year"] == y), key=lambda r: r["hour"])
        tot = sum(r["trips"] for r in hrs)
        b.plot([r["hour"] for r in hrs], [100 * r["trips"] / tot for r in hrs], color=SERIES[i], label=str(y))
    b.set_xticks(range(0, 24, 3))
    b.set_xlabel("start hour")
    b.set_ylabel("share of the year's trips (%)")
    b.set_title("(b) Trips by hour of day", loc="left")
    b.legend(loc="upper left", fontsize=7)

    # (c) mean duration by rider and bike, 2020 vs 2024
    c = ax[1, 0]
    groups = [("casual", "classic"), ("casual", "electric"), ("member", "classic"), ("member", "electric")]
    w = 0.38
    for j, y in enumerate([2020, 2024]):
        vals = [next(r["avg_min"] for r in d["by_type"] if r["year"] == y and r["rider"] == g[0] and r["bike"] == g[1])
                for g in groups]
        xs = [k + (j - 0.5) * w for k in range(len(groups))]
        bars = c.bar(xs, vals, width=w - 0.04, color=SERIES[2 * j], label=str(y))
        for x, v in zip(xs, vals):
            c.text(x, v + 0.5, f"{v:.1f}", ha="center", va="bottom", fontsize=6, color=INK2)
        del bars
    c.set_xticks(range(len(groups)))
    c.set_xticklabels([f"{r}\n{b_}" for r, b_ in groups])
    c.set_ylabel("mean trip (min)")
    c.set_ylim(0, 33)
    c.grid(axis="x", visible=False)
    c.set_title("(c) Mean trip length, 2020 vs 2024", loc="left")
    c.legend(loc="upper right", fontsize=7)

    # (d) distance bands, stacked to 100 %
    dd = ax[1, 1]
    bands = ["0-2 km", "2-5 km", "5-10 km", "10+ km"]
    for k, y in enumerate(FOCUS):
        rows = {r["band"]: r["trips"] for r in d["bands"] if r["year"] == y}
        tot = sum(rows.values())
        left = 0.0
        for i, bnd in enumerate(bands):
            v = 100 * rows.get(bnd, 0) / tot
            dd.barh(k, v, left=left, color=SEQ[i], height=0.6, edgecolor="white", linewidth=1,
                    label=bnd if k == 0 else None)
            if v >= 6:
                dd.text(left + v / 2, k, f"{v:.0f}", ha="center", va="center", fontsize=6.5,
                        color="white" if i < 2 else INK)
            left += v
    dd.set_yticks(range(len(FOCUS)))
    dd.set_yticklabels([str(y) for y in FOCUS])
    dd.invert_yaxis()
    dd.set_xlim(0, 100)
    dd.set_xlabel("share of trips by start-station distance from Midtown (%)")
    dd.grid(axis="y", visible=False)
    dd.set_title("(d) Where trips start", loc="left")
    dd.legend(loc="upper center", bbox_to_anchor=(0.5, -0.32), ncol=4, fontsize=6.5)

    fig.tight_layout(h_pad=1.2, w_pad=1.5)
    fig.savefig(os.path.join(FIG, "results.pdf"))
    plt.close(fig)


def formats() -> None:
    rules = tomllib.load(open(os.path.join(ROOT, "config", "silver_rules.toml"), "rb"))
    runs = rules["legacy_v1"]["runs"]

    def idx(ym: str) -> float:
        y, m = map(int, ym.split("-"))
        return y + (m - 1) / 12

    patterns = []
    for r in runs:
        if r["pattern"] not in patterns:
            patterns.append(r["pattern"])
    modern = "yyyy-MM-dd HH:mm:ss.SSS"
    patterns.append(modern)
    fig, ax = plt.subplots(figsize=(6.7, 1.6))
    colours = {p: c for p, c in zip(patterns, SERIES + ["#e87ba4", "#6250d6"])}
    for r in runs:
        x0, x1 = idx(r["from"]), idx(r["to"]) + 1 / 12
        ax.barh(patterns.index(r["pattern"]), x1 - x0, left=x0, height=0.62, color=colours[r["pattern"]],
                edgecolor="white", linewidth=0.8)
    ax.barh(patterns.index(modern), idx("2026-10") + 1 / 12 - 2020, left=2020, height=0.62, color=colours[modern])
    ax.set_yticks(range(len(patterns)))
    ax.set_yticklabels(patterns, fontfamily="monospace", fontsize=6.5)
    ax.invert_yaxis()
    ax.set_xlim(2013.4, 2027)
    ax.set_xticks(range(2014, 2027, 2))
    ax.grid(axis="y", visible=False)
    ax.annotate("2015: with and without\nseconds alternate", xy=(2015.2, patterns.index("M/d/yyyy H:mm")),
                xytext=(2016.9, 2.1), fontsize=6.5, color=INK2, va="center",
                arrowprops=dict(arrowstyle="-", color=INK2, lw=0.6))
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, "formats.pdf"))
    plt.close(fig)


def main():
    os.makedirs(FIG, exist_ok=True)
    d = json.load(open(os.path.join(REPORT, "data", "gold_summary.json")))
    results(d)
    formats()
    print("figures written to", FIG)


if __name__ == "__main__":
    main()
