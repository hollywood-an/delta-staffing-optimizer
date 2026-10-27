"""Data-quality trend — one row per validation run and suite in outputs/quality/history/quality_trend.csv.

src/quality.py appends to it after every run (full runs, run_all.py --validate, and the Airflow
gates); this module prints the last N runs. Standard library only, so the trend can be read from
any of the project's virtualenvs. The file is git-ignored: it carries timestamps and run ids.

Run:  python -m src.quality_trend          # last 10 runs
      python -m src.quality_trend 25       # last 25 runs
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TREND_PATH = Path("outputs") / "quality" / "history" / "quality_trend.csv"     # relative to a root
FIELDS = ["run_id", "finished_at", "engine", "scope", "suite", "expectations", "passed",
          "gate_failures", "warnings", "contacts_rows", "staffing_slots", "opt_agent_hours",
          "realistic_saved_dollars", "min_service_level", "mape_normal"]


def append(rows: list[dict], root: Path = PROJECT_ROOT) -> Path:
    path = Path(root) / TREND_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if new:
            writer.writeheader()
        writer.writerows({k: r.get(k, "") for k in FIELDS} for r in rows)
    return path


def last_runs(n: int = 10, root: Path = PROJECT_ROOT) -> list[list[dict]]:
    """The last n runs, oldest first, each as its list of per-suite rows."""
    path = Path(root) / TREND_PATH
    if not path.exists():
        return []
    runs: dict[str, list[dict]] = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            runs.setdefault(row["run_id"], []).append(row)
    return list(runs.values())[-n:]


def _num(text: str, fmt: str) -> str:
    try:
        return format(float(text), fmt)
    except (TypeError, ValueError):
        return "-"


def show(n: int = 10, root: Path = PROJECT_ROOT) -> None:
    runs = last_runs(n, root)
    if not runs:
        print(f"No validation runs recorded yet ({TREND_PATH}). Run `make validate` or "
              f"`python run_all.py --validate`.")
        return
    print(f"Last {len(runs)} validation run(s) — {TREND_PATH}")
    header = (f"{'finished (UTC)':<20} {'engine':<7} {'scope':<22} {'result':<6} {'checks':>6} "
              f"{'failed':>6} {'warn':>4} {'contacts':>10} {'slots':>6} {'agent-hours':>12} "
              f"{'saved vs 8h':>12} {'min SL':>7}")
    print(header)
    print("-" * len(header))
    for rows in runs:
        total = sum(int(r["expectations"]) for r in rows)
        failed = sum(int(r["gate_failures"]) for r in rows)
        warned = sum(int(r["warnings"]) for r in rows)
        r0 = rows[0]
        print(f"{r0['finished_at'][:19]:<20} {r0['engine']:<7} {r0['scope']:<22} "
              f"{'PASS' if not failed else 'FAIL':<6} {total:>6} {failed:>6} {warned:>4} "
              f"{_num(r0['contacts_rows'], ',.0f'):>10} {_num(r0['staffing_slots'], ',.0f'):>6} "
              f"{_num(r0['opt_agent_hours'], ',.1f'):>12} {_num(r0['realistic_saved_dollars'], ',.0f'):>12} "
              f"{_num(r0['min_service_level'], '.4f'):>7}")


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    show(int(args[0]) if args else 10)


if __name__ == "__main__":
    main()
