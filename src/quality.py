"""Data-quality gates — Great Expectations suites validated against the files a pipeline run wrote.

The pytest suites check the code at development time; these gates check the data at pipeline time
(run_all.py --validate, and the Airflow validate_* tasks). Four suites in three layers:

  raw        contacts.csv: the 11 invariants of tests/test_data_quality.py as expectations
             (labelled DQ-1 .. DQ-11) plus the catalog's column rules
  interim    Spark engine only: the cleaned Parquet (catalog rules, every raw row kept) and the
             aggregates (catalog rules, totals reconcile with the raw rows, fractions sum to 1),
             all fresher than their inputs
  processed  forecast.csv covers exactly the expected dates with no gaps, intraday fractions sum
             to 1 within 1e-9, the staffing plan's slots / agents / service level, plus the
             catalog's rules for every processed file
  outputs    cross-file consistency: the summary's agent-hours and savings equal what
             staffing_plan.csv implies; the dashboard CSVs and KPI tiles match the summary and plan

Every row count, column, allowed value and range comes from metadata/catalog.yaml. A failing
`error` expectation fails the gate (exit 1, each failure printed with its observed value); a
`warning` is reported only.

Writes:  outputs/quality/latest.md          deterministic summary of raw/processed/outputs (committed)
         outputs/quality/latest_interim.md  deterministic summary of the interim suite (committed)
         outputs/quality/history/*.json     one record per run, with timestamps (git-ignored)
         outputs/quality/data_docs/         Great Expectations Data Docs (git-ignored)

Runs in its own virtualenv (make gx-install); it only reads files.

Run:  python -m src.quality                         # pandas engine: raw, processed, outputs
      python -m src.quality --engine spark          # + interim
      python -m src.quality --layers raw,interim    # a gate: these suites only, history only
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path

# Great Expectations sends anonymous usage statistics unless told not to; this project never does.
os.environ.setdefault("GX_ANALYTICS_ENABLED", "False")

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import great_expectations as gx  # noqa: E402
import great_expectations.expectations as gxe  # noqa: E402
from great_expectations.data_context.types.base import ProgressBarsConfig  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402
from src import catalog as cat  # noqa: E402

SUITES = ("raw", "interim", "processed", "outputs")
ENGINE_SUITES = {"pandas": ("raw", "processed", "outputs"), "spark": SUITES}
REPORTS = {"latest.md": ("raw", "processed", "outputs"), "latest_interim.md": ("interim",)}
QUALITY_DIR = Path("outputs") / "quality"                       # relative to --root
INTERVAL_HOURS = config.INTERVAL_MINUTES / 60.0
DAY_NAMES = np.array(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])
RESULT_FORMAT = {"result_format": "SUMMARY", "partial_unexpected_count": 3}
EPS = 1e-9          # "equal" for values a file stores after rounding: same rounding, then within 1e-9


class QualityGateFailed(RuntimeError):
    """One or more gating expectations failed (the message lists them with observed values)."""


@dataclass
class Check:
    key: str                       # stable id, e.g. "staffing_plan.predicted_sl.range"
    view: str                      # the dataframe it runs on
    expectation: object            # a Great Expectations expectation
    what: str                      # one line for reports
    severity: str = "error"        # "error" gates the pipeline; "warning" is reported only
    mirrors: str = ""              # the tests/test_data_quality.py invariant it duplicates, if any
    id_column: str | None = None   # report failing rows by this column


@dataclass
class Outcome:
    suite: str
    key: str
    view: str
    what: str
    expectation: str
    severity: str
    mirrors: str
    success: bool
    observed: str


# --------------------------------------------------------------------------------------
# Inputs
# --------------------------------------------------------------------------------------
class MissingInput(FileNotFoundError):
    pass


class Inputs:
    """Reads catalog datasets relative to a root (the repo, or a test copy of it); cached."""

    def __init__(self, root: Path, catalog: dict):
        self.root, self.catalog, self._cache = Path(root), catalog, {}

    def ds(self, dataset_id: str) -> dict:
        return cat.dataset(self.catalog, dataset_id)

    def path(self, dataset_id: str) -> Path:
        p = self.root / self.ds(dataset_id)["path"]
        if not p.exists():
            raise MissingInput(f"missing input {self.ds(dataset_id)['path']} ({dataset_id})")
        return p

    def frame(self, dataset_id: str) -> pd.DataFrame:
        if dataset_id not in self._cache:
            d, p = self.ds(dataset_id), self.path(dataset_id)
            if d["format"] == "csv":
                self._cache[dataset_id] = pd.read_csv(p)
            elif d["format"] == "parquet":
                self._cache[dataset_id] = pd.read_parquet(p)
            elif d["format"] == "json":
                self._cache[dataset_id] = pd.DataFrame([_flatten(json.loads(p.read_text()))])
        return self._cache[dataset_id]

    def raw_rows(self) -> int:
        """Data rows in contacts.csv (newlines minus the header) without parsing it."""
        if "_raw_rows" not in self._cache:
            with open(self.path("contacts"), "rb") as f:
                n = sum(chunk.count(b"\n") for chunk in iter(lambda: f.read(1 << 24), b""))
            self._cache["_raw_rows"] = n - 1
        return self._cache["_raw_rows"]

    def mtime(self, dataset_id: str) -> float:
        p = self.path(dataset_id)
        return max(f.stat().st_mtime for f in p.iterdir()) if p.is_dir() else p.stat().st_mtime


def _flatten(obj: dict, prefix: str = "") -> dict:
    out = {}
    for k, v in obj.items():
        out.update(_flatten(v, f"{prefix}{k}.") if isinstance(v, dict) else {f"{prefix}{k}": v})
    return out


# --------------------------------------------------------------------------------------
# Checks generated from the catalog (any dataset)
# --------------------------------------------------------------------------------------
def catalog_checks(ds: dict, catalog: dict, view: str | None = None, mirrors: dict | None = None):
    """Columns, row count, primary key, nullability, allowed values, ranges and patterns — exactly
    what metadata/catalog.yaml declares for the dataset."""
    view, mirrors, did = view or ds["id"], mirrors or {}, ds["id"]
    names = [c["name"] for c in ds["columns"]]
    checks = []
    if ds["format"] == "json":
        checks.append(Check(f"{did}.fields", view, gxe.ExpectTableColumnsToMatchSet(
            column_set=names, exact_match=True), "has exactly the catalog's fields"))
    else:
        checks.append(Check(f"{did}.columns", view, gxe.ExpectTableColumnsToMatchOrderedList(
            column_list=names), "columns match the catalog, in order", mirrors=mirrors.get("columns", "")))
        lo, hi = cat.row_bounds(ds, catalog)
        exp = (gxe.ExpectTableRowCountToEqual(value=lo) if lo == hi else
               gxe.ExpectTableRowCountToBeBetween(min_value=lo, max_value=hi))
        checks.append(Check(f"{did}.rows", view, exp, f"rows {_bounds_text(lo, hi)}",
                            mirrors=mirrors.get("rows", "")))
        pk = ds["primary_key"]
        exp = (gxe.ExpectColumnValuesToBeUnique(column=pk[0]) if len(pk) == 1 else
               gxe.ExpectCompoundColumnsToBeUnique(column_list=pk))
        checks.append(Check(f"{did}.primary_key", view, exp, f"primary key ({', '.join(pk)}) is unique",
                            mirrors=mirrors.get("primary_key", "")))
    for col in ds["columns"]:
        n, m = col["name"], mirrors.get(col["name"], "")
        if not col["nullable"]:
            checks.append(Check(f"{did}.{n}.not_null", view, gxe.ExpectColumnValuesToNotBeNull(column=n),
                                f"{n} has no nulls", mirrors=mirrors.get("not_null", "")))
        if "allowed" in col:
            checks.append(Check(f"{did}.{n}.allowed", view, gxe.ExpectColumnValuesToBeInSet(
                column=n, value_set=col["allowed"]), f"{n} only takes allowed values", mirrors=m))
        lo, hi = cat.column_bounds(col, catalog)
        if lo is not None or hi is not None:
            checks.append(Check(f"{did}.{n}.range", view, gxe.ExpectColumnValuesToBeBetween(
                column=n, min_value=lo, max_value=hi), f"{n} {_bounds_text(lo, hi)}", mirrors=m))
        if "pattern" in col:
            checks.append(Check(f"{did}.{n}.pattern", view, gxe.ExpectColumnValuesToMatchRegex(
                column=n, regex=col["pattern"]), f"{n} matches {col['pattern']}"))
    return checks


def _bounds_text(lo, hi) -> str:
    if lo is not None and hi is not None:
        return f"= {_fmt(lo)}" if lo == hi else f"in [{_fmt(lo)}, {_fmt(hi)}]"
    return f">= {_fmt(lo)}" if lo is not None else f"<= {_fmt(hi)}"


def _equals(key, view, column, expected, tol, what, severity="error"):
    """Single-value reconciliation: the view's `column` must equal `expected` within `tol`
    (GX reports the value it saw as the observed value)."""
    exp = gxe.ExpectColumnMaxToBeBetween(column=column, min_value=expected - tol, max_value=expected + tol)
    return Check(key, view, exp, f"{what} (expected {_fmt(expected)}{f' ± {_fmt(tol)}' if tol else ''})",
                 severity=severity)


# --------------------------------------------------------------------------------------
# Suites
# --------------------------------------------------------------------------------------
def raw_suite(inp: Inputs):
    """contacts.csv — the 11 invariants of tests/test_data_quality.py (DQ-n) + catalog rules."""
    ds = inp.ds("contacts")
    df = inp.frame("contacts")
    mirrors = {"columns": "DQ-1", "not_null": "DQ-2", "handle_time_seconds": "DQ-3",
               "contact_reason": "DQ-5", "customer_segment": "DQ-5", "day_of_week": "DQ-5",
               "rows": "DQ-8", "primary_key": "DQ-10"}
    checks = catalog_checks(ds, inp.catalog, mirrors=mirrors)
    checks.append(Check("contacts.handle_time_seconds.integer", "contacts", gxe.ExpectColumnValuesToBeOfType(
        column="handle_time_seconds", type_="int64"), "handle_time_seconds is an integer type", mirrors="DQ-3"))
    for col, m in (("contact_reason", "DQ-4"), ("customer_segment", "DQ-5"), ("day_of_week", "DQ-5")):
        allowed = next(c["allowed"] for c in ds["columns"] if c["name"] == col)
        checks.append(Check(f"contacts.{col}.complete", "contacts", gxe.ExpectColumnDistinctValuesToEqualSet(
            column=col, value_set=allowed), f"every allowed {col} value occurs", mirrors=m))

    ts = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S")
    start = pd.to_datetime(df["interval_start"], format="%Y-%m-%d %H:%M:%S")
    derived = pd.DataFrame({
        "timestamp": ts, "interval_start": start,
        "secs_into_interval": (ts - start).dt.total_seconds(),
        "interval_start_is_floor": ts.dt.floor(f"{config.INTERVAL_SECONDS}s") == start,
        "hour_matches": ts.dt.hour.to_numpy() == df["hour"].to_numpy(),
        "day_matches": DAY_NAMES[ts.dt.dayofweek.to_numpy()] == df["day_of_week"].to_numpy(dtype=object),
        "date_matches": ts.dt.normalize() == pd.to_datetime(df["date"], format="%Y-%m-%d"),
        "abandoned_and_resolved": df["abandoned"] & df["resolved_first_contact"],
        "repeat_and_resolved": df["repeat_contact"] & df["resolved_first_contact"],
    })
    dv = "contacts_derived"
    checks += [
        Check("contacts.flags.abandoned_not_resolved", dv, gxe.ExpectColumnValuesToBeInSet(
            column="abandoned_and_resolved", value_set=[False]),
            "no abandoned contact is a first-contact resolution", mirrors="DQ-6"),
        Check("contacts.flags.repeat_not_resolved", dv, gxe.ExpectColumnValuesToBeInSet(
            column="repeat_and_resolved", value_set=[False]),
            "no repeat contact is a first-contact resolution", mirrors="DQ-6"),
        Check("contacts.bucket.start_le_timestamp", dv, gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="timestamp", column_B="interval_start", or_equal=True),
            "interval_start <= timestamp", mirrors="DQ-9"),
        Check("contacts.bucket.within_interval", dv, gxe.ExpectColumnValuesToBeBetween(
            column="secs_into_interval", min_value=0, max_value=config.INTERVAL_SECONDS - 1),
            "timestamp falls inside its 30-minute interval", mirrors="DQ-9"),
        Check("contacts.bucket.is_floor", dv, gxe.ExpectColumnValuesToBeInSet(
            column="interval_start_is_floor", value_set=[True]),
            "interval_start is the timestamp floored to the interval", mirrors="DQ-9"),
        Check("contacts.hour.matches_timestamp", dv, gxe.ExpectColumnValuesToBeInSet(
            column="hour_matches", value_set=[True]), "hour matches the timestamp", mirrors="DQ-11"),
        Check("contacts.day_of_week.matches_timestamp", dv, gxe.ExpectColumnValuesToBeInSet(
            column="day_matches", value_set=[True]), "day_of_week matches the timestamp", mirrors="DQ-11"),
        Check("contacts.date.matches_timestamp", dv, gxe.ExpectColumnValuesToBeInSet(
            column="date_matches", value_set=[True]), "date matches the timestamp", mirrors="DQ-11"),
    ]

    share = (df["contact_reason"].value_counts(normalize=True) * 100).rename("pct").reset_index()
    checks += [
        Check("contacts.reason_share.min", "reason_share", gxe.ExpectColumnMinToBeBetween(
            column="pct", min_value=1.0), "no reason below 1% of contacts", mirrors="DQ-4"),
        Check("contacts.reason_share.max", "reason_share", gxe.ExpectColumnMaxToBeBetween(
            column="pct", max_value=60.0), "no reason above 60% of contacts", mirrors="DQ-4"),
        Check("contacts.reason_share.total", "reason_share", gxe.ExpectColumnSumToBeBetween(
            column="pct", min_value=99.5, max_value=100.5), "reason shares sum to 100% (±0.5)", mirrors="DQ-4"),
    ]

    irop_days = pd.to_datetime(pd.Series(df.loc[df["irop_flag"], "date"].unique())).sort_values()
    run_id = (irop_days.diff().dt.days != 1).cumsum()
    events = irop_days.groupby(run_id.to_numpy()).agg(["min", "max"]).reset_index(drop=True)
    events["days_from_first"] = (events["max"] - events["min"].min()).dt.days if len(events) else []
    checks += [
        Check("contacts.irop.events", "irop_events", gxe.ExpectTableRowCountToBeBetween(min_value=10),
              "at least 10 distinct IROP events (runs of consecutive IROP days)", mirrors="DQ-7"),
        Check("contacts.irop.spread", "irop_events", gxe.ExpectColumnMaxToBeBetween(
            column="days_from_first", min_value=31), "IROP events span more than 30 days", mirrors="DQ-7"),
    ]
    views = {"contacts": df, dv: derived, "reason_share": share, "irop_events": events}
    return views, checks


def interim_suite(inp: Inputs):
    """Spark artifacts: catalog rules, every raw row kept, totals reconcile, all fresh."""
    views, checks, raw_rows = {}, [], inp.raw_rows()
    for did in ("contacts_parquet", "spark_daily_series", "spark_intraday_profile",
                "spark_interval_aht", "spark_reason_mix", "spark_scalars"):
        views[did] = inp.frame(did)
        checks += catalog_checks(inp.ds(did), inp.catalog)
    checks += [
        Check("contacts_parquet.rows_equal_raw", "contacts_parquet", gxe.ExpectTableRowCountToEqual(
            value=raw_rows), f"keeps every contacts.csv row (expected {raw_rows:,})"),
        Check("spark_daily_series.y.total_equals_raw", "spark_daily_series", gxe.ExpectColumnSumToBeBetween(
            column="y", min_value=raw_rows, max_value=raw_rows), f"daily totals sum to the raw rows ({raw_rows:,})"),
        Check("spark_reason_mix.contacts.total_equals_raw", "spark_reason_mix", gxe.ExpectColumnSumToBeBetween(
            column="contacts", min_value=raw_rows, max_value=raw_rows),
            f"reason counts sum to the raw rows ({raw_rows:,})"),
        _equals("spark_scalars.n_contacts.equals_raw", "spark_scalars", "n_contacts", raw_rows, 0,
                "n_contacts equals the raw rows"),
        Check("spark_intraday_profile.fraction.sum_is_one", "spark_intraday_profile", gxe.ExpectColumnSumToBeBetween(
            column="fraction", min_value=1 - 1e-9, max_value=1 + 1e-9), "fractions sum to 1 within 1e-9"),
    ]
    pairs = [("contacts_parquet", "contacts")] + [
        (a, "contacts_parquet") for a in ("spark_daily_series", "spark_intraday_profile",
                                          "spark_interval_aht", "spark_reason_mix", "spark_scalars")]
    views["freshness"] = pd.DataFrame({
        "artifact": [a for a, _ in pairs], "upstream": [u for _, u in pairs],
        "fresh": [inp.mtime(a) >= inp.mtime(u) for a, u in pairs]})
    checks.append(Check("interim.freshness", "freshness", gxe.ExpectColumnValuesToBeInSet(
        column="fresh", value_set=[True]), "every artifact is at least as new as its input",
        id_column="artifact"))
    return views, checks


def processed_suite(inp: Inputs):
    """Forecast dates, intraday fractions, staffing plan service and agents + catalog rules."""
    c = inp.catalog["constants"]
    views, checks = {}, []
    for did in ("forecast", "intraday_profile", "forecast_metrics", "staffing_plan",
                "optimize_summary", "validation"):
        views[did] = inp.frame(did)
        checks += catalog_checks(inp.ds(did), inp.catalog)

    fc = views["forecast"]
    offset = (pd.to_datetime(fc["date"], format="%Y-%m-%d") - pd.Timestamp(config.SIM_START)).dt.days
    views["forecast_days"] = pd.DataFrame({"date": fc["date"], "day_offset": offset,
                                           "gap_days": offset.diff()}).iloc[1:]
    first, last = c["SIM_DAYS"] - c["TEST_DAYS"], c["SIM_DAYS"] + c["FUTURE_DAYS"] - 1
    test, future = fc[fc["segment"] == "test"], fc[fc["segment"] == "future"]
    views["forecast_test"], views["forecast_future"] = test, future
    future_dates = sorted(future["date"])
    checks += [
        _equals("forecast.date.first_day", "forecast_range", "first_offset", first, 0,
                "starts on the first held-out day (days since SIM_START)"),
        _equals("forecast.date.last_day", "forecast_range", "last_offset", last, 0,
                "ends on the last day of the horizon (days since SIM_START)"),
        Check("forecast.date.no_gaps", "forecast_days", gxe.ExpectColumnValuesToBeInSet(
            column="gap_days", value_set=[1]), "consecutive dates, no gaps or repeats", id_column="date"),
        Check("forecast.test.days", "forecast_test", gxe.ExpectTableRowCountToEqual(value=c["TEST_DAYS"]),
              f"{c['TEST_DAYS']} held-out test days"),
        Check("forecast.test.actual_present", "forecast_test", gxe.ExpectColumnValuesToNotBeNull(
            column="actual"), "every test day has an actual"),
        Check("forecast.future.days", "forecast_future", gxe.ExpectTableRowCountToEqual(value=c["FUTURE_DAYS"]),
              f"{c['FUTURE_DAYS']} future days"),
        Check("forecast.future.actual_empty", "forecast_future", gxe.ExpectColumnValuesToBeNull(
            column="actual"), "no future day has an actual"),
        Check("forecast.upper_ge_forecast", "forecast", gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="forecast_upper", column_B="forecast", or_equal=True), "forecast_upper >= forecast"),
        Check("forecast.forecast_ge_lower", "forecast", gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="forecast", column_B="forecast_lower", or_equal=True), "forecast >= forecast_lower"),
        Check("intraday_profile.fraction.sum_is_one", "intraday_profile", gxe.ExpectColumnSumToBeBetween(
            column="fraction", min_value=1 - 1e-9, max_value=1 + 1e-9), "fractions sum to 1 within 1e-9"),
        Check("staffing_plan.scheduled_ge_required", "staffing_plan", gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="scheduled_agents", column_B="required_agents", or_equal=True),
            "scheduled agents >= required agents in every slot"),
        Check("staffing_plan.dates_match_forecast", "staffing_plan", gxe.ExpectColumnDistinctValuesToEqualSet(
            column="date", value_set=future_dates), "plans exactly the forecast's future dates"),
        _equals("forecast_metrics.n_test", "forecast_metrics", "n_test", c["TEST_DAYS"], 0,
                "scored on the whole held-out window"),
        Check("forecast_metrics.mape_normal_target", "forecast_metrics", gxe.ExpectColumnMaxToBeBetween(
            column="mape_normal", max_value=10.0), "normal-day MAPE under the spec's 10% target",
            severity="warning"),
        _equals("optimize_summary.n_days", "optimize_summary", "n_days", c["FUTURE_DAYS"], 0,
                "covers the whole horizon"),
    ]
    views["forecast_range"] = pd.DataFrame({"first_offset": [offset.min()], "last_offset": [offset.max()]})
    return views, checks


def _plan_kpis(plan: pd.DataFrame) -> dict:
    """The dashboard's KPI definitions (volume-weighted SL/ASA; load-weighted occupancy)."""
    w = plan["volume"]
    return {"service_level": float(np.average(plan["predicted_sl"], weights=w)) * 100,
            "asa": float(np.average(plan["predicted_asa_seconds"], weights=w)),
            "occupancy": float(plan["offered_load_erlangs"].sum() / plan["required_agents"].sum()) * 100}


def _dashboard_tiles(html_path: Path) -> dict:
    """KPI tile values from the figure data embedded in dashboard.html (the last Plotly.newPlot
    call; earlier occurrences are inside the bundled plotly.js)."""
    html = html_path.read_text()
    start = html.index("[", html.rfind("Plotly.newPlot("))
    data, _ = json.JSONDecoder().raw_decode(html, start)
    titles = {"Service level": "service_level", "Avg speed of answer": "asa", "Occupancy": "occupancy",
              "Saved vs": "saved"}
    tiles = {}
    for trace in data:
        if trace.get("type") == "indicator":
            title = re.sub("<[^>]+>", " ", trace["title"]["text"])
            key = next((v for k, v in titles.items() if title.startswith(k)), None)
            if key:
                tiles[key] = float(trace["value"])
    return tiles


def outputs_suite(inp: Inputs):
    """Cross-file consistency between the summary, the plan, the dashboard CSVs and KPI tiles."""
    c = inp.catalog["constants"]
    views, checks = {}, []
    for did in ("daily_volume", "interval_staffing", "reason_mix", "savings_summary"):
        views[did] = inp.frame(did)
        checks += catalog_checks(inp.ds(did), inp.catalog)
    plan, fc = inp.frame("staffing_plan"), inp.frame("forecast")
    s = inp.frame("optimize_summary").iloc[0]
    rate, sched = config.AGENT_HOURLY_COST, plan["scheduled_agents"]

    # what staffing_plan.csv implies
    opt = float(sched.sum() * INTERVAL_HOURS)
    peak = int(sched.max())
    flat = peak * len(plan) * INTERVAL_HOURS
    block = plan["interval_index"] // (config.REALISTIC_BASELINE_BLOCK_HOURS * int(1 / INTERVAL_HOURS))
    realistic = float(plan.groupby([plan["date"], block])["scheduled_agents"].transform("max").sum()
                      * INTERVAL_HOURS)
    kpis = _plan_kpis(plan)
    tiles = _dashboard_tiles(inp.path("dashboard_html"))
    sav = views["savings_summary"].set_index("scenario")
    opt_row = "Optimized (Erlang C)"
    recon = {   # observed (what the files say) -> expected (what the plan / summary imply), tolerance
        "summary_opt_hours": (s["cost.opt_hours"], opt, 0, "summary agent-hours = plan's scheduled agents x 0.5 h"),
        "summary_peak_scheduled": (s["cost.peak_scheduled"], peak, 0, "summary peak = plan's busiest slot"),
        "summary_flat_hours": (s["cost.flat_hours"], flat, 0, "naive baseline = peak x every slot x 0.5 h"),
        "summary_realistic_hours": (s["cost.realistic_hours"], realistic, 0,
                                    f"realistic baseline = each {config.REALISTIC_BASELINE_BLOCK_HOURS}h shift at its busiest slot"),
        "summary_opt_cost": (s["cost.opt_cost"], opt * rate, 1e-6, f"optimized cost = agent-hours x ${rate:g}"),
        "summary_saved_dollars": (s["cost.saved_dollars"], (flat - opt) * rate, 1e-6, "savings vs naive"),
        "summary_realistic_saved_dollars": (s["cost.realistic_saved_dollars"], (realistic - opt) * rate, 1e-6,
                                            "savings vs realistic (the headline)"),
        "summary_realistic_pct_reduction": (s["cost.realistic_pct_reduction"], (realistic - opt) / realistic,
                                            1e-12, "% reduction vs realistic"),
        "savings_naive_hours": (sav.loc["Naive flat (peak 24/7)", "agent_hours"], round(s["cost.flat_hours"], 1), EPS,
                                "savings CSV naive agent-hours = summary"),
        "savings_realistic_hours": (sav.loc["Realistic (8h shifts)", "agent_hours"],
                                    round(s["cost.realistic_hours"], 1), EPS,
                                    "savings CSV realistic agent-hours = summary"),
        "savings_optimized_hours": (sav.loc[opt_row, "agent_hours"], round(s["cost.opt_hours"], 1), EPS,
                                    "savings CSV optimized agent-hours = summary"),
        "savings_deflection_hours": (sav.loc["Optimized + deflection", "agent_hours"], round(s["def_hours"], 1), EPS,
                                     "savings CSV deflection agent-hours = summary"),
        "savings_optimized_sl": (sav.loc[opt_row, "SL"], round(kpis["service_level"] / 100, 4), EPS,
                                 "savings CSV optimized SL = plan's volume-weighted SL"),
        "kpi_service_level": (tiles.get("service_level", np.nan), kpis["service_level"], 1e-9,
                              "dashboard SL tile = plan's volume-weighted SL (%)"),
        "kpi_asa": (tiles.get("asa", np.nan), kpis["asa"], 1e-9, "dashboard ASA tile = plan's volume-weighted ASA"),
        "kpi_occupancy": (tiles.get("occupancy", np.nan), kpis["occupancy"], 1e-9,
                          "dashboard occupancy tile = plan's load / required agents (%)"),
        "kpi_saved": (tiles.get("saved", np.nan), s["cost.realistic_saved_dollars"], 1e-6,
                      "dashboard savings tile = summary's savings vs realistic"),
        "reason_total_equals_actuals": (views["reason_mix"]["contacts"].sum(),
                                        views["daily_volume"]["actual"].sum(), 0,
                                        "reason-mix contacts = sum of daily actuals"),
    }
    views["reconciliation"] = pd.DataFrame({k: [float(v[0])] for k, v in recon.items()})
    checks += [_equals(f"outputs.{k}", "reconciliation", k, float(exp), tol, what)
               for k, (_, exp, tol, what) in recon.items()]

    # savings_summary: every row's cost = agent-hours x rate (rounded to whole dollars)
    views["savings_cost"] = views["savings_summary"].assign(
        abs_diff=(views["savings_summary"]["cost"] - views["savings_summary"]["agent_hours"] * rate).abs())
    checks.append(Check("outputs.savings_cost_matches_hours", "savings_cost", gxe.ExpectColumnValuesToBeBetween(
        column="abs_diff", max_value=0.5), f"every scenario's cost = agent-hours x ${rate:g} (to the dollar)",
        id_column="scenario"))

    # interval_staffing = the plan averaged by interval of day
    means = plan.groupby("interval_index").agg(volume=("volume", "mean"), required=("required_agents", "mean"),
                                               scheduled=("scheduled_agents", "mean"), SL=("predicted_sl", "mean"),
                                               occupancy=("predicted_occupancy", "mean")).reset_index(drop=True)
    istaff = views["interval_staffing"]
    diffs = pd.DataFrame({"interval": istaff["interval"]})
    for col, digits in (("volume", 1), ("required", 1), ("scheduled", 1), ("SL", 4), ("occupancy", 4)):
        expected = means[col].round(digits)                      # the dashboard's own rounding
        diffs[f"{col}_diff"] = (istaff[col] - expected).abs() if len(istaff) == len(means) else np.inf
        checks.append(Check(f"outputs.interval_staffing.{col}", "interval_vs_plan", gxe.ExpectColumnValuesToBeBetween(
            column=f"{col}_diff", max_value=EPS), f"interval {col} = plan mean by interval, rounded to {digits} dp",
            id_column="interval"))
    views["interval_vs_plan"] = diffs

    # daily_volume: consecutive dates over history + horizon, forecast columns = forecast.csv
    dv = views["daily_volume"]
    dv_offset = (pd.to_datetime(dv["date"], format="%Y-%m-%d") - pd.Timestamp(config.SIM_START)).dt.days
    views["daily_volume_days"] = pd.DataFrame({"date": dv["date"], "gap_days": dv_offset.diff()}).iloc[1:]
    checks += [
        _equals("outputs.daily_volume.first_day", "reconciliation_dates", "first_offset", 0, 0,
                "daily_volume starts on SIM_START"),
        _equals("outputs.daily_volume.last_day", "reconciliation_dates", "last_offset",
                c["SIM_DAYS"] + c["FUTURE_DAYS"] - 1, 0, "daily_volume ends on the horizon's last day"),
        Check("outputs.daily_volume.no_gaps", "daily_volume_days", gxe.ExpectColumnValuesToBeInSet(
            column="gap_days", value_set=[1]), "daily_volume dates are consecutive", id_column="date"),
    ]
    views["reconciliation_dates"] = pd.DataFrame({"first_offset": [dv_offset.min()], "last_offset": [dv_offset.max()]})
    joined = fc.merge(dv, on="date", how="left", suffixes=("", "_dv"))
    match = pd.DataFrame({"date": joined["date"]})
    for fcol, dcol in (("forecast", "forecast_dv"), ("forecast_lower", "lower"), ("forecast_upper", "upper"),
                       ("actual", "actual_dv")):
        a, b = joined[fcol], joined[dcol]
        match[f"{fcol}_diff"] = np.where(a.isna() & b.isna(), 0.0, (a - b).abs().fillna(np.inf))
        checks.append(Check(f"outputs.daily_volume.{fcol}", "daily_volume_vs_forecast", gxe.ExpectColumnValuesToBeBetween(
            column=f"{fcol}_diff", max_value=0), f"daily_volume {dcol.removesuffix('_dv')} = forecast.csv {fcol}",
            id_column="date"))
    views["daily_volume_vs_forecast"] = match
    return views, checks


SUITE_BUILDERS = {"raw": raw_suite, "interim": interim_suite, "processed": processed_suite,
                  "outputs": outputs_suite}


# --------------------------------------------------------------------------------------
# Running with Great Expectations
# --------------------------------------------------------------------------------------
def _context(gx_dir: Path, docs_dir: Path):
    context = gx.get_context(mode="file", project_root_dir=str(gx_dir))
    context.variables.progress_bars = ProgressBarsConfig(globally=False, metric_calculations=False)
    context.variables.save()
    sites = context.list_data_docs_sites()
    for name in list(sites):
        if name != "quality_docs":
            context.delete_data_docs_site(name)
    if "quality_docs" not in sites:
        context.add_data_docs_site(site_name="quality_docs", site_config={
            "class_name": "SiteBuilder", "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
            "store_backend": {"class_name": "TupleFilesystemStoreBackend", "base_directory": str(docs_dir)}})
    return context


def _validate(context, suite_name: str, views: dict, checks: list) -> list[Outcome]:
    source = context.data_sources.add_or_update_pandas("pipeline")
    outcomes = []
    for view in dict.fromkeys(ch.view for ch in checks):            # first-seen order
        mine = [ch for ch in checks if ch.view == view]
        name = f"{suite_name}.{view}"
        try:
            asset = source.get_asset(name)
        except LookupError:
            asset = source.add_dataframe_asset(name)
        try:
            batch = asset.get_batch_definition("all")
        except (KeyError, LookupError):
            batch = asset.add_batch_definition_whole_dataframe("all")
        suite = gx.ExpectationSuite(name=name)
        for ch in mine:
            ch.expectation.meta = {"key": ch.key, "what": ch.what, "severity": ch.severity,
                                   "mirrors": ch.mirrors}
            suite.add_expectation(ch.expectation)
        suite = context.suites.add_or_update(suite)
        vd = context.validation_definitions.add_or_update(gx.ValidationDefinition(name=name, data=batch, suite=suite))
        fmt = dict(RESULT_FORMAT)
        ids = {ch.id_column for ch in mine if ch.id_column}
        if ids:
            fmt["unexpected_index_column_names"] = sorted(ids)
        result = vd.run(batch_parameters={"dataframe": views[view]}, result_format=fmt)
        by_key = {r.expectation_config.meta["key"]: r for r in result.results}
        for ch in mine:                                              # report in check order
            r = by_key[ch.key]
            outcomes.append(Outcome(suite_name, ch.key, view, ch.what, r.expectation_config.type, ch.severity,
                                    ch.mirrors, bool(r.success), _observed(r.result, ids)))
    return outcomes


def _fmt(x) -> str:
    if isinstance(x, (np.generic,)):
        x = x.item()
    if isinstance(x, bool) or x is None:
        return str(x)
    if isinstance(x, float):
        if not np.isfinite(x):
            return str(x)
        if x.is_integer() and abs(x) < 1e15:
            return f"{int(x):,}"
        return f"{x:.10g}"
    if isinstance(x, int):
        return f"{x:,}"
    return str(x)


def _observed(result: dict, id_columns: set) -> str:
    """A short, deterministic description of what Great Expectations measured."""
    if result.get("observed_value") is not None:
        v = result["observed_value"]
        if isinstance(v, (list, tuple)):
            return f"{len(v)} values" if len(v) > 6 else ", ".join(_fmt(x) for x in v)
        return _fmt(v)
    if "unexpected_count" in result:
        n, total = result["unexpected_count"] or 0, result.get("element_count") or 0
        if not n:
            return f"0 of {total:,} rows unexpected"
        examples = result.get("partial_unexpected_index_list") or result.get("partial_unexpected_list") or []
        shown = []
        for e in examples[:3]:
            if isinstance(e, dict):
                ident = " / ".join(_fmt(e[k]) for k in sorted(id_columns) if k in e)
                vals = ", ".join(f"{k}={_fmt(v)}" for k, v in e.items() if k not in id_columns)
                shown.append(f"{ident}: {vals}" if ident else vals)
            else:
                shown.append(_fmt(e))
        return f"{n:,} of {total:,} rows unexpected (e.g. {'; '.join(shown)})"
    return "n/a"


def run(suites, root: Path = PROJECT_ROOT, docs: bool = True) -> list[Outcome]:
    """Build and validate the given suites against the files under `root`."""
    catalog = cat.load_valid()
    qdir = Path(root) / QUALITY_DIR
    context = _context(qdir / "gx", qdir / "data_docs")
    outcomes: list[Outcome] = []
    for name in suites:
        inp = Inputs(root, catalog)
        try:
            views, checks = SUITE_BUILDERS[name](inp)
        except MissingInput as exc:
            outcomes.append(Outcome(name, f"{name}.inputs", "-", "every input file exists", "file_exists",
                                    "error", "", False, str(exc)))
            continue
        outcomes += _validate(context, name, views, checks)
        del views, inp
    if docs:
        context.build_data_docs(site_names=["quality_docs"])
    return outcomes


# --------------------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------------------
def gating_failures(outcomes) -> list[Outcome]:
    return [o for o in outcomes if not o.success and o.severity == "error"]


def _counts(outcomes):
    return (len(outcomes), sum(o.success for o in outcomes), len(gating_failures(outcomes)),
            sum(not o.success and o.severity == "warning" for o in outcomes))


def render_report(outcomes, suites) -> str:
    """Deterministic Markdown summary: no timestamps, run ids or absolute paths."""
    mine = [o for o in outcomes if o.suite in suites]
    total, passed, failed, warned = _counts(mine)
    verdict = "PASS" if not failed else "FAIL"
    out = [
        "# Data quality — latest validation" + (" (interim layer, Spark engine)" if suites == ("interim",) else ""),
        "",
        f"Written by `python -m src.quality` (Great Expectations {gx.__version__}) from the files of the "
        "last pipeline run. Deterministic: no timestamps or run ids; those go to "
        "`outputs/quality/history/` (git-ignored). Rules come from "
        "[`metadata/catalog.yaml`](../../metadata/catalog.yaml); `DQ-n` marks an expectation that "
        "duplicates invariant n of `tests/test_data_quality.py`. A failing **gate** stops the "
        "pipeline; a **warning** is reported only.",
        "",
        f"**Result: {verdict}** — {total} expectations: {passed} passed, {failed} failed"
        f"{f', {warned} warning(s)' if warned else ''}.",
        "",
        "| Suite | Expectations | Passed | Gate failures | Warnings |",
        "|---|---:|---:|---:|---:|",
    ]
    for s in suites:
        t, p, f, w = _counts([o for o in mine if o.suite == s])
        out.append(f"| {s} | {t} | {p} | {f} | {w} |")
    for s in suites:
        out += ["", f"## {s}", "", "| Check | What | Expectation | Kind | Result | Observed |",
                "|---|---|---|---|---|---|"]
        for o in (x for x in mine if x.suite == s):
            what = f"{o.what} ({o.mirrors})" if o.mirrors else o.what
            result = "pass" if o.success else ("**FAIL**" if o.severity == "error" else "warn")
            kind = "gate" if o.severity == "error" else "warning"
            out.append(f"| `{o.key}` | {_md(what)} | `{o.expectation}` | {kind} | {result} | {_md(o.observed)} |")
    return "\n".join(out) + "\n"


def _md(text: str) -> str:
    return str(text).replace("|", "\\|")


def print_summary(outcomes, suites) -> None:
    print("=" * 72)
    print(f"DATA QUALITY — Great Expectations {gx.__version__}: {', '.join(suites)}")
    print("=" * 72)
    for s in suites:
        t, p, f, w = _counts([o for o in outcomes if o.suite == s])
        print(f"  {s:<10} {t:>4} expectations  {p:>4} passed  {f:>3} gate failures  {w:>2} warnings")
    for o in outcomes:
        if not o.success:
            tag = "FAIL" if o.severity == "error" else "WARN"
            print(f"  {tag} [{o.suite}] {o.key} — {o.what}\n         {o.expectation}: observed {o.observed}")
    failed = gating_failures(outcomes)
    print("-" * 72)
    print(f"{'PASS' if not failed else 'FAIL'}: {len(outcomes)} expectations, {len(failed)} gate failure(s).")
    print("=" * 72)


def write_history(outcomes, suites, engine, root: Path, started: float, finished: float) -> Path:
    stamp = dt.datetime.now(dt.timezone.utc)
    run_id = f"{stamp:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    hdir = Path(root) / QUALITY_DIR / "history"
    hdir.mkdir(parents=True, exist_ok=True)
    path = hdir / f"{run_id}.json"
    path.write_text(json.dumps({
        "run_id": run_id, "finished_at": stamp.isoformat(timespec="seconds"),
        "duration_seconds": round(finished - started, 2), "engine": engine, "suites": list(suites),
        "great_expectations": gx.__version__, "success": not gating_failures(outcomes),
        "outcomes": [asdict(o) for o in outcomes]}, indent=2))
    return path


def gate(suites, root: Path = PROJECT_ROOT, engine: str = "spark") -> None:
    """Pipeline gate (Airflow): validate, record history, raise if a gating expectation failed."""
    started = time.perf_counter()
    outcomes = run(suites, root=root, docs=True)
    write_history(outcomes, suites, engine, root, started, time.perf_counter())
    print_summary(outcomes, suites)
    failed = gating_failures(outcomes)
    if failed:
        raise QualityGateFailed(f"{len(failed)} gating expectation(s) failed: " + "; ".join(
            f"[{o.suite}] {o.key} observed {o.observed}" for o in failed))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate pipeline outputs with Great Expectations.")
    parser.add_argument("--engine", choices=list(ENGINE_SUITES), default="pandas",
                        help="which suites a full run covers (spark adds interim)")
    parser.add_argument("--layers", help=f"comma-separated subset of {', '.join(SUITES)} "
                                         "(gate mode: history only, latest*.md not rewritten)")
    parser.add_argument("--root", type=Path, default=PROJECT_ROOT, help="validate the files under this root")
    parser.add_argument("--no-docs", action="store_true", help="skip building Data Docs")
    parser.add_argument("--results-json", type=Path, help="also write the outcomes here (for tests)")
    args = parser.parse_args(argv)

    suites = tuple(args.layers.split(",")) if args.layers else ENGINE_SUITES[args.engine]
    unknown = set(suites) - set(SUITES)
    if unknown:
        parser.error(f"unknown layer(s): {', '.join(sorted(unknown))}")
    started = time.perf_counter()
    outcomes = run(suites, root=args.root, docs=not args.no_docs)
    finished = time.perf_counter()
    history = write_history(outcomes, suites, args.engine, args.root, started, finished)
    if not args.layers:                                     # a full run refreshes the committed summaries
        for name, members in REPORTS.items():
            members = tuple(s for s in members if s in suites)
            if members:
                (Path(args.root) / QUALITY_DIR / name).write_text(render_report(outcomes, members))
    if args.results_json:
        args.results_json.write_text(json.dumps([asdict(o) for o in outcomes], indent=2))
    print_summary(outcomes, suites)
    print(f"History: {history.relative_to(Path(args.root)) if history.is_relative_to(Path(args.root)) else history}"
          f"  ({finished - started:.1f}s)")
    if not args.no_docs:
        print(f"Data Docs: open {(QUALITY_DIR / 'data_docs' / 'index.html')}")
    return 1 if gating_failures(outcomes) else 0


if __name__ == "__main__":
    sys.exit(main())
