"""Spark ETL parity — the PySpark aggregates match the pandas pipeline's.

Runs the existing (unchanged) pandas functions and src/spark_etl.py on the same small seeded
dataset and asserts every aggregate the downstream stages consume agrees within 1e-6; labels,
keys and ordering must match exactly. The dataset comes from the Phase-1 generator at reduced
size (90 days at ~1,500 contacts/day, ~140k rows) rather than a row sample of the 570 MB
contacts.csv, so the test is fast, deterministic and doesn't need the full file. The full
4.7M-row check is run by hand: `python -m src.spark_etl --parity-full`.

Skipped, with the reason, when pyspark isn't installed or no Java runtime can start.
"""
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

pytest.importorskip("pyspark")

from src import spark_etl  # noqa: E402
from src.generate_data import generate_contacts, write_contacts  # noqa: E402

SAMPLE_DAYS = 90
SAMPLE_BASE_DAILY = 1500
TOLERANCE = spark_etl.TOLERANCE      # 1e-6


@pytest.fixture(scope="module")
def spark():
    from pyspark.errors import PySparkRuntimeError
    try:
        session = spark_etl.get_spark("staffing-etl-tests")
    except PySparkRuntimeError as exc:          # e.g. JAVA_GATEWAY_EXITED: no usable JDK
        pytest.skip(f"no working Java runtime for Spark: {exc}")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def parity(spark, tmp_path_factory):
    """(pandas reference, Spark outputs read back from disk, {check: max |diff|})."""
    tmp = tmp_path_factory.mktemp("spark_parity")
    csv = tmp / "contacts.csv"
    write_contacts(generate_contacts(sim_days=SAMPLE_DAYS, base_daily=SAMPLE_BASE_DAILY), csv)
    spark_etl.run_etl(csv, tmp / "contacts.parquet", tmp / "spark", spark=spark)
    ref = spark_etl.pandas_reference(csv)
    out = spark_etl.load_outputs(tmp / "spark")
    return ref, out, dict(spark_etl.compare(ref, out))


def _assert_checks(checks, names):
    bad = {n: checks[n] for n in names if not checks[n] <= TOLERANCE}
    assert not bad, f"pandas vs Spark disagree (max |diff|, inf = labels/order differ): {bad}"


def test_sample_exercises_irop_and_every_bucket(parity):
    ref, out, _ = parity
    assert ref["irop"].any() and not ref["irop"].all(), "sample needs both IROP and normal days"
    assert len(out["daily"]) == SAMPLE_DAYS
    assert (out["profile"]["fraction"] > 0).all(), "every half-hour bucket should have contacts"


def test_daily_series_matches_pandas(parity):
    _, out, checks = parity
    assert list(out["daily"].columns) == ["ds", "y", "irop_flag"]
    _assert_checks(checks, ["daily: dates", "daily: contacts per day", "daily: IROP flag per day",
                            "dashboard daily actuals: dates", "dashboard daily actuals: counts"])


def test_intraday_profile_matches_pandas(parity):
    _, out, checks = parity
    assert abs(out["profile"]["fraction"].sum() - 1.0) < 1e-9
    _assert_checks(checks, ["intraday: columns + interval labels", "intraday: fraction"])


def test_handle_time_and_shares_match_pandas(parity):
    _, _, checks = parity
    _assert_checks(checks, ["AHT per interval (optimize)", "AHT per interval (dashboard)",
                            "IROP AHT", "deflectable share (optimize)",
                            "deflectable share (dashboard)"])


def test_reason_mix_matches_pandas(parity):
    _, _, checks = parity
    _assert_checks(checks, ["reason mix: columns + reason order", "reason mix: contacts",
                            "reason mix: avg_AHT", "reason mix: repeat_rate"])


def test_all_checks_pass(parity):
    # Catches a check added to spark_etl.compare() later that no test above names.
    _, _, checks = parity
    assert all(diff <= TOLERANCE for diff in checks.values()), checks
