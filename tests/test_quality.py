"""Data-quality gates — the Great Expectations suites pass on a clean run and catch injected defects.

Runs `python -m src.quality` in its own venv (.venv-gx) as a subprocess against a copy of a
pipeline run's files in a temp dir — exactly how run_all.py --validate and the Airflow gates call
it. Skipped, with the reason, if .venv-gx isn't installed (make gx-install).

Mutation tests: each layer gets one copy with several independent defects, each aimed at a
different expectation, and the run must fail on exactly those expectations — every defect caught by
its intended check and nothing else firing. (Each defect was also run on its own once; NOTES.md.)
"""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.generate_data import COLUMNS, RAW_CSV_PATH, generate_contacts, write_contacts  # noqa: E402

GX_PYTHON = PROJECT_ROOT / ".venv-gx" / "bin" / "python"
PROCESSED = ["forecast.csv", "intraday_profile.csv", "forecast_metrics.json", "staffing_plan.csv",
             "optimize_summary.json", "validation.csv"]
DASHBOARD = ["daily_volume.csv", "interval_staffing.csv", "reason_mix.csv", "savings_summary.csv",
             "dashboard.html"]

pytestmark = pytest.mark.skipif(not GX_PYTHON.exists(), reason=".venv-gx not installed (make gx-install)")


# --- helpers ----------------------------------------------------------------------------------
def run_quality(root: Path, *args):
    """(exit code, outcomes, stdout) of `python -m src.quality --root <root> --no-docs ...`."""
    results = root / "results.json"
    proc = subprocess.run([str(GX_PYTHON), "-m", "src.quality", "--root", str(root), "--no-docs",
                           "--results-json", str(results), *args],
                          cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=900)
    outcomes = json.loads(results.read_text()) if results.exists() else []
    return proc.returncode, outcomes, proc.stdout + proc.stderr


def failed(outcomes, severity="error"):
    return {o["key"] for o in outcomes if not o["success"] and o["severity"] == severity}


def make_root(tmp: Path, interim_src: Path) -> Path:
    """A pipeline-run layout under tmp: raw contacts (symlink), interim (copy of the shared Spark
    ETL run), processed + dashboard files (copies of the committed ones)."""
    if not RAW_CSV_PATH.exists():
        write_contacts(generate_contacts(), RAW_CSV_PATH)
    (tmp / "data" / "raw").mkdir(parents=True)
    (tmp / "data" / "raw" / "contacts.csv").symlink_to(RAW_CSV_PATH)
    shutil.copytree(interim_src, tmp / "data" / "interim")
    (tmp / "data" / "processed").mkdir()
    for name in PROCESSED:
        shutil.copy2(PROJECT_ROOT / "data" / "processed" / name, tmp / "data" / "processed" / name)
    (tmp / "outputs" / "dashboard").mkdir(parents=True)
    for name in DASHBOARD:
        shutil.copy2(PROJECT_ROOT / "outputs" / "dashboard" / name, tmp / "outputs" / "dashboard" / name)
    return tmp


def edit_csv(path: Path, row: int, column: str, value: str):
    """Set one cell of a small CSV (row = 0-based data row)."""
    lines = path.read_text().splitlines()
    header = lines[0].split(",")
    cells = lines[row + 1].split(",")
    cells[header.index(column)] = value
    lines[row + 1] = ",".join(cells)
    path.write_text("\n".join(lines) + "\n")


def edit_json(path: Path, dotted: str, fn):
    data = json.loads(path.read_text())
    node, keys = data, dotted.split(".")
    for k in keys[:-1]:
        node = node[k]
    node[keys[-1]] = fn(node[keys[-1]])
    path.write_text(json.dumps(data, indent=2))


@pytest.fixture(scope="module")
def interim_src(spark_etl_full):
    return spark_etl_full["root"]                 # <root>/contacts.parquet, <root>/spark/


@pytest.fixture(scope="module")
def clean_run(interim_src, tmp_path_factory):
    root = make_root(tmp_path_factory.mktemp("quality_clean"), interim_src)
    code, outcomes, out = run_quality(root, "--engine", "spark")
    return root, code, outcomes, out


# --- a clean pipeline run passes, and the committed reports are current --------------------------------
def test_clean_run_passes_every_suite(clean_run):
    _, code, outcomes, out = clean_run
    assert code == 0, out[-3000:]
    assert {o["suite"] for o in outcomes} == {"raw", "interim", "processed", "outputs"}
    assert not failed(outcomes) and not failed(outcomes, "warning")
    assert len(outcomes) > 300


def test_committed_reports_are_up_to_date(clean_run):
    root = clean_run[0]
    for name in ("latest.md", "latest_interim.md"):
        committed = (PROJECT_ROOT / "outputs" / "quality" / name).read_text()
        assert (root / "outputs" / "quality" / name).read_text() == committed, \
            f"outputs/quality/{name} is stale — run `make validate` after the pipeline"


def test_every_data_quality_invariant_is_mirrored(clean_run):
    mirrored = {o["mirrors"] for o in clean_run[2] if o["mirrors"]}
    assert mirrored == {f"DQ-{i}" for i in range(1, 12)}


# --- injected defects: each caught by its intended expectation, nothing else fires -------------------
def test_raw_defects_are_caught(interim_src, tmp_path):
    root = make_root(tmp_path, interim_src)
    target = root / "data" / "raw" / "contacts.csv"
    target.unlink()                                          # replace the symlink with an edited copy
    col = {c: i for i, c in enumerate(COLUMNS)}
    with open(RAW_CSV_PATH) as src, open(target, "w") as dst:
        dst.write(src.readline())
        rows = [src.readline().rstrip("\n").split(",") for _ in range(4)]
        rows[0][col["hour"]] = str((int(rows[0][col["hour"]]) + 1) % 24)     # hour != timestamp hour
        rows[1][col["customer_segment"]] = ""                                # a null
        rows[2][col["handle_time_seconds"]] = "3601"                         # above the clip range
        rows[3][col["contact_id"]] = rows[0][col["contact_id"]]              # a duplicate id
        dst.writelines(",".join(r) + "\n" for r in rows)
        shutil.copyfileobj(src, dst, 1 << 24)
    code, outcomes, out = run_quality(root, "--layers", "raw")
    assert code == 1, out[-2000:]
    assert failed(outcomes) == {"contacts.hour.matches_timestamp", "contacts.customer_segment.not_null",
                                "contacts.handle_time_seconds.range", "contacts.primary_key"}


def test_interim_defects_are_caught(interim_src, tmp_path):
    root = make_root(tmp_path, interim_src)
    spark = root / "data" / "interim" / "spark"
    edit_csv(spark / "daily_series.csv", 100, "y", str(int(
        (spark / "daily_series.csv").read_text().splitlines()[101].split(",")[1]) + 1))
    lines = (spark / "intraday_profile.csv").read_text().splitlines()
    edit_csv(spark / "intraday_profile.csv", 20, "fraction", repr(float(lines[21].split(",")[2]) + 1e-6))
    edit_json(spark / "scalars.json", "n_contacts", lambda n: n - 1)
    stale = RAW_CSV_PATH.stat().st_mtime - 3600                # Parquet older than its input
    for f in (root / "data" / "interim" / "contacts.parquet").iterdir():
        os.utime(f, (stale, stale))
    code, outcomes, out = run_quality(root, "--layers", "interim")
    assert code == 1, out[-2000:]
    assert failed(outcomes) == {"spark_daily_series.y.total_equals_raw",
                                "spark_intraday_profile.fraction.sum_is_one",
                                "spark_scalars.n_contacts.equals_raw", "interim.freshness"}
    stale_row = next(o for o in outcomes if o["key"] == "interim.freshness")
    assert "contacts_parquet" in stale_row["observed"]       # the failing artifact is named


def test_processed_defects_are_caught(interim_src, tmp_path):
    root = make_root(tmp_path, interim_src)
    proc = root / "data" / "processed"
    edit_csv(proc / "staffing_plan.csv", 700, "predicted_sl", "0.79")        # below the 80/20 target
    edit_csv(proc / "staffing_plan.csv", 701, "required_agents", "0")        # no agent in a slot
    lines = (proc / "intraday_profile.csv").read_text().splitlines()
    edit_csv(proc / "intraday_profile.csv", 20, "fraction", repr(float(lines[21].split(",")[2]) + 0.001))
    edit_csv(proc / "validation.csv", 3, "sl_diff", "0.05")                  # sim disagrees with Erlang C
    fc = (proc / "forecast.csv").read_text().splitlines()
    (proc / "forecast.csv").write_text("\n".join(l for l in fc if not l.startswith("2026-01-15")) + "\n")
    code, outcomes, out = run_quality(root, "--layers", "processed")
    assert code == 1, out[-2000:]
    assert failed(outcomes) == {
        "staffing_plan.predicted_sl.range", "staffing_plan.required_agents.range",
        "intraday_profile.fraction.sum_is_one", "validation.sl_diff.range",
        # one missing forecast day: wrong row count, a gap, one future day short, and the plan
        # now covers a date the forecast doesn't
        "forecast.rows", "forecast.date.no_gaps", "forecast.future.days", "staffing_plan.dates_match_forecast"}


def test_outputs_defects_are_caught(interim_src, tmp_path):
    root = make_root(tmp_path, interim_src)
    dash = root / "outputs" / "dashboard"
    edit_csv(dash / "savings_summary.csv", 0, "cost", "3528100.0")           # cost != hours x rate
    edit_json(root / "data" / "processed" / "optimize_summary.json", "cost.peak_scheduled", lambda n: n + 1)
    lines = (dash / "interval_staffing.csv").read_text().splitlines()
    edit_csv(dash / "interval_staffing.csv", 33, "scheduled", repr(float(lines[34].split(",")[3]) + 5))
    html = (dash / "dashboard.html").read_text()
    sl = repr(json.loads(json.dumps(87.84118057965726)))
    assert html.count(sl) == 1
    (dash / "dashboard.html").write_text(html.replace(sl, "88.84118057965726"))  # KPI tile drifted
    dv = (dash / "daily_volume.csv").read_text().splitlines()
    i = next(n for n, l in enumerate(dv) if l.startswith("2026-01-10")) - 1
    edit_csv(dash / "daily_volume.csv", i, "forecast", str(int(dv[i + 1].split(",")[2]) + 1))
    code, outcomes, out = run_quality(root, "--layers", "outputs")
    assert code == 1, out[-2000:]
    assert failed(outcomes) == {"outputs.savings_cost_matches_hours", "outputs.summary_peak_scheduled",
                                "outputs.interval_staffing.scheduled", "outputs.kpi_service_level",
                                "outputs.daily_volume.forecast"}


def test_a_warning_is_reported_but_does_not_fail_the_gate(interim_src, tmp_path):
    root = make_root(tmp_path, interim_src)
    edit_json(root / "data" / "processed" / "forecast_metrics.json", "mape_normal", lambda _: 12.0)
    code, outcomes, out = run_quality(root, "--layers", "processed")
    assert code == 0, out[-2000:]
    assert failed(outcomes, "warning") == {"forecast_metrics.mape_normal_target"} and not failed(outcomes)
    assert "WARN [processed] forecast_metrics.mape_normal_target" in out


def test_a_missing_input_fails_the_gate(interim_src, tmp_path):
    root = make_root(tmp_path, interim_src)
    (root / "data" / "processed" / "staffing_plan.csv").unlink()
    code, outcomes, out = run_quality(root, "--layers", "processed")
    assert code == 1 and failed(outcomes) == {"processed.inputs"}, out[-2000:]
    assert "staffing_plan.csv" in next(o["observed"] for o in outcomes if o["key"] == "processed.inputs")


def test_python_dash_c_import_does_not_exit_silently():
    # Why .venv-gx exists: with pyspark installed, importing Great Expectations from `python -c`
    # hit PySpark 4.2.0's doctest heuristic and exited 0 before running anything (NOTES.md).
    proc = subprocess.run([str(GX_PYTHON), "-c", "import src.quality; print('imported-ok')"],
                          cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=300)
    assert proc.returncode == 0 and "imported-ok" in proc.stdout, proc.stderr[-2000:]
