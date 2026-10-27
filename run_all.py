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
(outputs/dashboard/TABLEAU_GUIDE.md). Afterwards, `pytest` runs the test suites.

`--engine spark` adds a Spark ETL step after Phase 1 (src/spark_etl.py: contacts.csv -> Parquet ->
aggregates) and the forecast, optimize and dashboard phases read its aggregates instead of
re-reading contacts.csv with pandas. Same outputs, byte for byte (see NOTES.md). Needs pyspark and
a Java runtime; the default pandas engine needs neither.

`--validate` runs the data-quality gates at the end (src/quality.py, Great Expectations, in its own
.venv-gx: make gx-install) and exits non-zero if a gating expectation fails. Without the flag
nothing changes.

Run:  python run_all.py                  # pandas engine (default)
      python run_all.py --engine spark   # Spark ETL for the raw-data stages
      python run_all.py --validate       # either engine, then the data-quality gates
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from functools import partial
from pathlib import Path

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


def spark_steps() -> list:
    """STEPS with the Spark ETL after Phase 1 and the raw-data phases reading its aggregates."""
    from src import spark_etl   # imported only here: the pandas engine never needs pyspark/Java
    return [
        ("Phase 1 — synthetic contact data", generate_data.main),
        ("Spark ETL — contacts.csv -> Parquet -> aggregates", spark_etl.run),
        ("Phase 2 — forecast (Spark aggregates)", partial(forecast.main, engine="spark")),
        ("Phase 3 — Erlang C verified table", erlang._demo),
        ("Phase 4 — simulation validates Erlang C", simulate.main),
        ("Phase 5 — optimize staffing & savings (Spark aggregates)",
         partial(optimize.main, engine="spark")),
        ("Phase 6 — dashboard exports (Spark aggregates)", partial(dashboard.main, engine="spark")),
        ("Phase 7 — executive deck", build_deck.main),
    ]


def validate(engine: str) -> None:
    """Run the data-quality gates in .venv-gx (Great Expectations only reads the files)."""
    root = Path(__file__).resolve().parent
    gx_python = Path(os.environ.get("STAFFING_GX_PYTHON", root / ".venv-gx" / "bin" / "python"))
    if not gx_python.exists():
        sys.exit(f"--validate needs the data-quality venv: run `make gx-install` ({gx_python} not found)")
    sys.stdout.flush()                   # keep the log in order when stdout is a file
    code = subprocess.run([str(gx_python), "-m", "src.quality", "--engine", engine], cwd=root).returncode
    if code:
        sys.exit(code)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the staffing pipeline end to end.")
    parser.add_argument("--engine", choices=["pandas", "spark"], default="pandas",
                        help="engine for the raw-data stages (default: pandas)")
    parser.add_argument("--validate", action="store_true",
                        help="run the data-quality gates at the end; exit non-zero on a failure")
    args = parser.parse_args(argv)
    steps = STEPS if args.engine == "pandas" else spark_steps()
    if args.validate:
        steps = steps + [("Data quality — Great Expectations gates", partial(validate, args.engine))]

    start = time.perf_counter()
    for name, step in steps:
        print(f"\n{'#' * 72}\n# {name}\n{'#' * 72}")
        t = time.perf_counter()
        step()
        print(f"[{name}: {time.perf_counter() - t:.0f}s]")
    print(f"\nAll phases complete in {time.perf_counter() - start:.0f}s. "
          f"Run `pytest` to verify the test suites.")


if __name__ == "__main__":
    main()
