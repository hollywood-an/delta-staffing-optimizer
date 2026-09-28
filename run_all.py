"""Phase 8 — run the whole pipeline end to end, from an empty clone.

Every step is seeded, so a fresh run reproduces the committed results:

  Phase 1  synthetic contact data   -> data/raw/contacts.csv (~4.7M rows), volume_overview.png
  Phase 2  forecast                 -> forecast.csv, intraday_profile.csv, forecast_metrics.json, forecast.png
  Phase 3  Erlang C check           -> prints the spec's verified table (the engine is a library)
  Phase 4  simulation validation    -> validation.csv, validation.png
  Phase 5  optimization & savings   -> staffing_plan.csv, optimize_summary.json, staffing_curve.png, savings.png
  Phase 6  dashboard exports        -> outputs/dashboard/*.csv + dashboard.html (Plotly)
  Phase 7  executive deck           -> outputs/Delta_Reservations_Capstone.pptx

The Tableau Public dashboard is authored by hand from the Phase-6 CSVs
(outputs/dashboard/TABLEAU_GUIDE.md). Afterwards, `pytest` runs the three test suites.

Run:  python run_all.py
"""
from __future__ import annotations

import time

from src import build_deck, dashboard, erlang, forecast, generate_data, optimize, simulate

STEPS = [
    ("Phase 1 — synthetic contact data", generate_data.main),
    ("Phase 2 — forecast", forecast.main),
    ("Phase 3 — Erlang C verified table", erlang._demo),
    ("Phase 4 — simulation validates Erlang C", simulate.main),
    ("Phase 5 — optimize staffing & savings", optimize.main),
    ("Phase 6 — dashboard exports", dashboard.main),
    ("Phase 7 — executive deck", build_deck.main),
]


def main() -> None:
    start = time.perf_counter()
    for name, step in STEPS:
        print(f"\n{'#' * 72}\n# {name}\n{'#' * 72}")
        t = time.perf_counter()
        step()
        print(f"[{name}: {time.perf_counter() - t:.0f}s]")
    print(f"\nAll phases complete in {time.perf_counter() - start:.0f}s. "
          f"Run `pytest` to verify the three test suites.")


if __name__ == "__main__":
    main()
