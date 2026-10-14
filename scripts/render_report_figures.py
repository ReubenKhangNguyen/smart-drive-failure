"""Render report-ready charts from the compact, real Gold analytics JSON exports."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "artifacts" / "reports" / "data_survey"
DESTINATION = ROOT / "artifacts" / "reports" / "figures"
COLORS = {"navy": "#163A5F", "blue": "#2F80ED", "orange": "#F2994A", "red": "#D9485F", "teal": "#149E9A", "gray": "#6B7280"}


def load(name: str):
    """Load one local JSON copy written by the Spark data-survey job."""
    return json.loads((SOURCE / f"{name}.json").read_text(encoding="utf-8"))


def save(figure, filename: str) -> None:
    """Save a PNG suitable for Word and PowerPoint plus a scalable SVG."""
    DESTINATION.mkdir(parents=True, exist_ok=True)
    figure.savefig(DESTINATION / f"{filename}.png", dpi=200, bbox_inches="tight", facecolor="white")
    figure.savefig(DESTINATION / f"{filename}.svg", bbox_inches="tight", facecolor="white")
    plt.close(figure)


def failure_by_date() -> None:
    rows = load("failure_by_date")
    dates = [row["date"][5:] for row in rows]
    failures = [row["failures"] for row in rows]
    rate_100k = [row["failure_rate"] * 100_000 for row in rows]
    figure, axis = plt.subplots(figsize=(12, 6.4))
    axis.bar(dates, failures, color=COLORS["blue"], label="Recorded failures")
    axis.set_ylabel("Failure rows", color=COLORS["navy"], fontweight="bold")
    axis.set_xlabel("Date in January 2026")
    axis.set_title("Recorded drive failures by day", loc="left", fontsize=17, fontweight="bold", color=COLORS["navy"])
    axis.grid(axis="y", alpha=.22)
    twin = axis.twinx()
    twin.plot(dates, rate_100k, color=COLORS["red"], marker="o", linewidth=2.4, label="Failure rate")
    twin.set_ylabel("Failures per 100,000 observations", color=COLORS["red"], fontweight="bold")
    handles, labels = axis.get_legend_handles_labels()
    handles2, labels2 = twin.get_legend_handles_labels()
    axis.legend(handles + handles2, labels + labels2, frameon=False, loc="upper left")
    figure.text(.125, .01, "Source: Backblaze Drive Stats; 14-day Silver subset processed with Spark SQL.", fontsize=9, color=COLORS["gray"])
    save(figure, "failure_by_date")


def failure_by_model() -> None:
    rows = sorted(load("failure_by_model"), key=lambda row: row["failures"], reverse=True)[:10]
    labels = [row["model"] for row in rows][::-1]
    failures = [row["failures"] for row in rows][::-1]
    figure, axis = plt.subplots(figsize=(12, 6.4))
    bars = axis.barh(labels, failures, color=COLORS["teal"])
    axis.bar_label(bars, padding=3, fontweight="bold")
    axis.set_xlabel("Recorded failure rows")
    axis.set_title("Top 10 drive models by recorded failures", loc="left", fontsize=17, fontweight="bold", color=COLORS["navy"])
    axis.grid(axis="x", alpha=.22)
    axis.set_axisbelow(True)
    figure.text(.125, .01, "Counts depend on the number of observations per model; they do not establish causation.", fontsize=9, color=COLORS["gray"])
    save(figure, "failure_by_model")


def smart_comparison() -> None:
    rows = {row["failure"]: row for row in load("smart_comparison")}
    smart = ["smart_5_raw", "smart_187_raw", "smart_197_raw", "smart_198_raw", "smart_199_raw"]
    non_failure = [rows[0][f"avg_{column}"] for column in smart]
    failure = [rows[1][f"avg_{column}"] for column in smart]
    index = list(range(len(smart)))
    figure, axis = plt.subplots(figsize=(12, 6.4))
    axis.bar([value - .19 for value in index], non_failure, .38, label="failure = 0", color=COLORS["blue"])
    axis.bar([value + .19 for value in index], failure, .38, label="failure = 1", color=COLORS["orange"])
    axis.set_yscale("log")
    axis.set_xticks(index, [column.replace("smart_", "SMART ").replace("_raw", "") for column in smart])
    axis.set_ylabel("Mean raw value (log scale)")
    axis.set_title("SMART raw averages: recorded-failure rows versus non-failure rows", loc="left", fontsize=15, fontweight="bold", color=COLORS["navy"])
    axis.legend(frameon=False)
    axis.grid(axis="y", which="both", alpha=.22)
    figure.text(.125, .01, "Descriptive screening only. Current-day failure is not a future prediction feature.", fontsize=9, color=COLORS["gray"])
    save(figure, "smart_comparison")


if __name__ == "__main__":
    plt.rcParams.update({"font.family": "DejaVu Sans", "axes.spines.top": False, "axes.spines.right": False})
    failure_by_date()
    failure_by_model()
    smart_comparison()
    print(f"Rendered figures in {DESTINATION}")
