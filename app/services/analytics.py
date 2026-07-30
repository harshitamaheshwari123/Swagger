"""NumPy summary stats and the Matplotlib chart over company risk scores."""
from __future__ import annotations

import os

import matplotlib

matplotlib.use("Agg")  # headless, no display available
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402


def compute_score_stats(company_rows: list[dict]) -> dict:
    if not company_rows:
        return {"mean": 0.0, "median": 0.0, "std_dev": 0.0, "p90": 0.0, "count": 0}

    scores = np.array([row["risk_score"] for row in company_rows], dtype=float)
    return {
        "mean": round(float(np.mean(scores)), 2),
        "median": round(float(np.median(scores)), 2),
        "std_dev": round(float(np.std(scores)), 2),
        "p90": round(float(np.percentile(scores, 90)), 2),
        "count": int(scores.size),
    }


def plot_top_companies(company_rows: list[dict], output_path: str, top_n: int = 10) -> str:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    ranked = sorted(company_rows, key=lambda r: r["risk_score"], reverse=True)[:top_n]
    names = [r["company_name"] for r in ranked]
    scores = [r["risk_score"] for r in ranked]

    fig, ax = plt.subplots(figsize=(10, 6))
    colors = [
        "#d62728" if s >= 70 else "#ff7f0e" if s >= 40 else "#2ca02c" for s in scores
    ]
    bars = ax.barh(names[::-1], scores[::-1], color=colors[::-1])

    ax.set_title("Top Companies by Risk Score", fontsize=14, fontweight="bold")
    ax.set_xlabel("Company Risk Score (0-100)")
    ax.set_ylabel("Company")
    ax.set_xlim(0, 100)

    for bar, score in zip(bars, scores[::-1]):
        ax.text(
            bar.get_width() + 1,
            bar.get_y() + bar.get_height() / 2,
            f"{score:.2f}",
            va="center",
            fontsize=9,
        )

    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    return output_path
