"""Spark ETL — schema, row counts, read rules, and the data-quality invariants on the Parquet output.

The shared `spark_etl_full` fixture (conftest.py) runs src/spark_etl.py on the full
data/raw/contacts.csv (generated once with the seeded defaults if missing, like
tests/test_data_quality.py) into a temp directory, then this file checks
the cleaned Parquet it wrote. The invariants mirror tests/test_data_quality.py rule for rule, so
the curated layer is held to the same standard as the raw CSV. They are evaluated in Spark:
reading 4.7M rows of Parquet into pandas would need pyarrow, which the project doesn't install.

Skipped, with the reason, when pyspark isn't installed or no Java runtime can start.
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

pytest.importorskip("pyspark")
from pyspark.sql import functions as F  # noqa: E402

from src import spark_etl  # noqa: E402
from src.generate_data import (  # noqa: E402
    COLUMNS, HANDLE_TIME_MAX, HANDLE_TIME_MIN, INTERVAL_SECONDS, RAW_CSV_PATH, REASONS, SEGMENTS,
    count_irop_events, generate_contacts, write_contacts,
)

ROW_COUNT_MIN, ROW_COUNT_MAX = 3_500_000, 5_500_000      # same band as test_data_quality.py
DAYS_OF_WEEK = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"}
EXPECTED_PARQUET_TYPES = {
    "contact_id": "string", "timestamp": "timestamp_ntz", "date": "date",
    "interval_start": "timestamp_ntz", "hour": "smallint", "day_of_week": "string",
    "contact_reason": "string", "customer_segment": "string", "handle_time_seconds": "int",
    "irop_flag": "boolean", "resolved_first_contact": "boolean", "repeat_contact": "boolean",
    "abandoned": "boolean", "interval_index": "int",
}


@pytest.fixture(scope="module")
def etl(spark_etl_full):                    # one full-data ETL run per session (conftest.py)
    return spark_etl_full


@pytest.fixture(scope="module")
def pq(spark, etl):
    return spark.read.parquet(str(etl["parquet"]))


def _count(df, condition) -> int:
    return df.filter(condition).count()


# --- schema ---------------------------------------------------------------------------
def test_contact_schema_matches_generator_columns():
    assert spark_etl.CONTACT_SCHEMA.fieldNames() == COLUMNS


def test_parquet_columns_and_types(pq):
    assert pq.columns == COLUMNS + ["interval_index"]
    assert dict(pq.dtypes) == EXPECTED_PARQUET_TYPES


# --- row counts -----------------------------------------------------------------------
def test_parquet_keeps_every_csv_row(pq, etl):
    with open(RAW_CSV_PATH, "rb") as f:                   # data lines = newlines - header
        csv_rows = sum(chunk.count(b"\n") for chunk in iter(lambda: f.read(1 << 24), b"")) - 1
    assert pq.count() == csv_rows
    assert etl["result"]["aggs"]["scalars"]["n_contacts"] == csv_rows


def test_row_count_within_expected_band(pq):
    assert ROW_COUNT_MIN <= pq.count() <= ROW_COUNT_MAX


# --- data-quality invariants (mirroring tests/test_data_quality.py) ------------------------
def test_no_nulls_anywhere(pq):
    nulls = pq.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in pq.columns]).first()
    assert not any(nulls), {c: n for c, n in nulls.asDict().items() if n}


def test_handle_time_positive_and_in_clip_range(pq):
    lo, hi = pq.select(F.min("handle_time_seconds"), F.max("handle_time_seconds")).first()
    assert 0 < HANDLE_TIME_MIN <= lo and hi <= HANDLE_TIME_MAX


def test_reason_distribution_complete_and_sane(pq):
    counts = {r["contact_reason"]: r["count"] for r in pq.groupBy("contact_reason").count().collect()}
    total = sum(counts.values())
    pct = {k: 100 * v / total for k, v in counts.items()}
    assert set(pct) == set(REASONS)
    assert abs(sum(pct.values()) - 100.0) <= 0.5
    assert min(pct.values()) >= 1.0 and max(pct.values()) <= 60.0


def test_categorical_values_allowed_and_complete(pq):
    def distinct(col):
        return {r[0] for r in pq.select(col).distinct().collect()}
    assert distinct("contact_reason") == set(REASONS)
    assert distinct("customer_segment") == set(SEGMENTS)
    assert distinct("day_of_week") == DAYS_OF_WEEK


def test_behavioural_flags_are_logically_consistent(pq):
    assert _count(pq, F.col("abandoned") & F.col("resolved_first_contact")) == 0
    assert _count(pq, F.col("repeat_contact") & F.col("resolved_first_contact")) == 0


def test_at_least_10_distinct_irop_events(pq):
    dates = [r["date"] for r in pq.filter("irop_flag").select("date").distinct().collect()]
    assert dates, "no IROP-flagged contacts present"
    events = count_irop_events(pd.DataFrame({"date": [d.isoformat() for d in dates],
                                             "irop_flag": True}))
    assert events >= 10, f"expected >=10 distinct IROP events, found {events}"
    assert (max(dates) - min(dates)).days > 30, "IROP events should be spread across the year"


def test_interval_start_le_timestamp_and_same_bucket(pq):
    # timezone-naive -> epoch seconds via the session's UTC zone: exact, no DST shifts
    ts = F.col("timestamp").cast("timestamp").cast("long")
    start = F.col("interval_start").cast("timestamp").cast("long")
    lo, hi = pq.select(F.min(ts - start), F.max(ts - start)).first()
    assert lo >= 0, "interval_start must be <= timestamp"
    assert hi < INTERVAL_SECONDS, "timestamp must be inside its 30-min bucket"
    assert _count(pq, (ts - ts % INTERVAL_SECONDS) != start) == 0


def test_contact_id_unique(pq):
    assert pq.select(F.countDistinct("contact_id")).first()[0] == pq.count()


def test_derived_columns_consistent(pq):
    assert _count(pq, F.col("hour") != F.hour("timestamp")) == 0
    assert _count(pq, F.col("day_of_week") != F.date_format("timestamp", "EEEE")) == 0
    assert _count(pq, F.col("date") != F.to_date("timestamp")) == 0
    expected_bucket = F.hour("interval_start") * 2 + (F.minute("interval_start") == 30).cast("int")
    assert _count(pq, F.col("interval_index") != expected_bucket) == 0
    assert pq.select(F.min("interval_index"), F.max("interval_index")).first() == (0, 47)


# --- read rules: a bad header or a malformed value fails the ETL, never becomes a null ----------
@pytest.fixture(scope="module")
def tiny_csv_lines(tmp_path_factory):
    path = tmp_path_factory.mktemp("tiny") / "contacts.csv"
    write_contacts(generate_contacts(sim_days=2, base_daily=100), path)
    return path.read_text().splitlines()


def _write(tmp_path, lines):
    path = tmp_path / "contacts.csv"
    path.write_text("\n".join(lines) + "\n")
    return path


def test_valid_tiny_csv_round_trips(spark, tiny_csv_lines, tmp_path):
    csv = _write(tmp_path, tiny_csv_lines)
    spark_etl.write_parquet(spark_etl.read_contacts(spark, csv), tmp_path / "out.parquet")
    assert spark.read.parquet(str(tmp_path / "out.parquet")).count() == len(tiny_csv_lines) - 1


def test_header_that_does_not_match_schema_fails(spark, tiny_csv_lines, tmp_path):
    lines = [tiny_csv_lines[0].replace("handle_time_seconds", "handle_secs")] + tiny_csv_lines[1:]
    with pytest.raises(Exception, match="CSV header"):
        spark_etl.write_parquet(spark_etl.read_contacts(spark, _write(tmp_path, lines)),
                                tmp_path / "out.parquet")


@pytest.mark.parametrize("column,bad_value", [("irop_flag", "maybe"),
                                              ("handle_time_seconds", "abc"),
                                              ("timestamp", "not-a-time")])
def test_malformed_value_fails_fast(spark, tiny_csv_lines, tmp_path, column, bad_value):
    i = COLUMNS.index(column)
    row = tiny_csv_lines[1].split(",")
    row[i] = bad_value
    lines = [tiny_csv_lines[0], ",".join(row)] + tiny_csv_lines[2:]
    with pytest.raises(Exception, match="MALFORMED"):
        spark_etl.write_parquet(spark_etl.read_contacts(spark, _write(tmp_path, lines)),
                                tmp_path / "out.parquet")
