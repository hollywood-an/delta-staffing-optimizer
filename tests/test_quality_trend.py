"""Data-quality trend — rows are appended per run and suite, and the last N runs print in order.

Standard library only (like src/quality_trend.py), so this runs without the .venv-gx validator.
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src import quality_trend  # noqa: E402


def _rows(run_id, failures=0):
    return [{"run_id": run_id, "finished_at": f"2026-01-01T00:00:{run_id[-2:]}+00:00", "engine": "spark",
             "scope": "full", "suite": s, "expectations": 10, "passed": 10 - failures,
             "gate_failures": failures, "warnings": 0, "contacts_rows": 4740671, "staffing_slots": 1440,
             "opt_agent_hours": 44390.5, "realistic_saved_dollars": 845237.5, "min_service_level": 0.8}
            for s in ("raw", "outputs")]


def test_append_groups_rows_by_run_and_keeps_the_last_n(tmp_path):
    for i in range(1, 6):
        quality_trend.append(_rows(f"run-{i:02d}", failures=int(i == 4)), root=tmp_path)
    path = tmp_path / quality_trend.TREND_PATH
    assert path.read_text().splitlines()[0].split(",") == quality_trend.FIELDS     # header written once
    runs = quality_trend.last_runs(3, root=tmp_path)
    assert [r[0]["run_id"] for r in runs] == ["run-03", "run-04", "run-05"]
    assert all(len(r) == 2 for r in runs)


def test_show_prints_pass_and_fail(tmp_path, capsys):
    quality_trend.append(_rows("run-01"), root=tmp_path)
    quality_trend.append(_rows("run-02", failures=1), root=tmp_path)
    quality_trend.show(10, root=tmp_path)
    out = capsys.readouterr().out.splitlines()
    assert "Last 2 validation run(s)" in out[0]
    assert "PASS" in out[3] and "4,740,671" in out[3] and "845,238" in out[3]
    assert "FAIL" in out[4]


def test_show_without_history_says_how_to_create_it(tmp_path, capsys):
    quality_trend.show(10, root=tmp_path)
    assert "No validation runs recorded yet" in capsys.readouterr().out
