"""Phase 1 — data-quality invariants for the synthetic contact dataset.

These are *invariant* tests, not exact-value tests: they must hold for ANY random
seed, so they assert ranges and rules rather than specific numbers (per FLAGSHIP_SPEC
Section 3.5). This is the kind of suite a real analytics team runs on a pipeline.

The fixture validates the on-disk artifact `data/raw/contacts.csv`. If it is missing
(e.g. a fresh clone), it is generated once with the seeded defaults so the suite is
self-contained.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402
from src.generate_data import (  # noqa: E402
    RAW_CSV_PATH, COLUMNS, REASONS, SEGMENTS, HANDLE_TIME_MIN, HANDLE_TIME_MAX,
    INTERVAL_SECONDS, generate_contacts, write_contacts, count_irop_events,
)

ROW_COUNT_MIN = 3_500_000
ROW_COUNT_MAX = 5_500_000
DAYS_OF_WEEK = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
                "Saturday", "Sunday"}
BOOL_COLS = ["irop_flag", "resolved_first_contact", "repeat_contact", "abandoned"]


@pytest.fixture(scope="module")
def df() -> pd.DataFrame:
    path = Path(RAW_CSV_PATH)
    if not path.exists():
        write_contacts(generate_contacts(), path)
    data = pd.read_csv(path, parse_dates=["timestamp", "interval_start", "date"])
    # Make boolean columns robust regardless of how the CSV reader inferred them.
    for col in BOOL_COLS:
        if data[col].dtype != bool:
            coerced = data[col].astype(str).str.strip().str.lower().map(
                {"true": True, "false": False}
            )
            assert not coerced.isna().any(), f"unparseable boolean values in {col}"
            data[col] = coerced
    return data


def test_columns_present_and_ordered(df):
    assert list(df.columns) == COLUMNS


def test_no_nulls_anywhere(df):
    null_counts = df.isnull().sum()
    assert null_counts.sum() == 0, f"nulls found:\n{null_counts[null_counts > 0]}"


def test_handle_time_positive_and_in_clip_range(df):
    ht = df["handle_time_seconds"]
    assert ht.dtype.kind in "iu", "handle_time_seconds must be an integer type"
    assert (ht > 0).all()
    assert ht.min() >= HANDLE_TIME_MIN
    assert ht.max() <= HANDLE_TIME_MAX


def test_reason_distribution_complete_and_sane(df):
    # NOTE: this deliberately avoids the vacuous `value_counts(normalize=True).sum()==1`
    # tautology — it checks the mix is actually populated, not just normalized.
    pct = df["contact_reason"].value_counts(normalize=True, dropna=False) * 100
    assert abs(pct.sum() - 100.0) <= 0.5
    # every expected reason must be present (catches a collapse to fewer categories)
    assert set(df["contact_reason"].dropna().unique()) == set(REASONS)
    # no single reason dominates or vanishes -> a non-vacuous distributional check
    assert pct.min() >= 1.0
    assert pct.max() <= 60.0


def test_categorical_values_allowed_and_complete(df):
    # `==` enforces both "only allowed values" and "all expected values present".
    assert set(df["contact_reason"].unique()) == set(REASONS)
    assert set(df["customer_segment"].unique()) == set(SEGMENTS)
    assert set(df["day_of_week"].unique()) == DAYS_OF_WEEK


def test_behavioural_flags_are_logically_consistent(df):
    # an abandoned contact (left before answer) cannot be a first-contact resolution
    assert not (df["abandoned"] & df["resolved_first_contact"]).any()
    # a follow-up to a prior unresolved contact is not itself a first-contact resolution
    assert not (df["repeat_contact"] & df["resolved_first_contact"]).any()


def test_at_least_10_distinct_irop_events(df):
    assert df["irop_flag"].any(), "no IROP-flagged contacts present"
    n_events = count_irop_events(df)
    assert n_events >= 10, f"expected >=10 distinct IROP events, found {n_events}"
    # events must span multiple, separated date ranges (not one contiguous block)
    irop_dates = pd.to_datetime(df.loc[df["irop_flag"], "date"].unique())
    span_days = (irop_dates.max() - irop_dates.min()).days
    assert span_days > 30, "IROP events should be spread across the year"


def test_row_count_within_expected_band(df):
    assert ROW_COUNT_MIN <= len(df) <= ROW_COUNT_MAX, f"row count {len(df):,} out of band"


def test_interval_start_le_timestamp_and_same_bucket(df):
    delta = (df["timestamp"] - df["interval_start"]).dt.total_seconds()
    assert (delta >= 0).all(), "interval_start must be <= timestamp"
    assert (delta < INTERVAL_SECONDS).all(), "timestamp must be inside its 30-min bucket"
    # the floor of timestamp to the interval must equal interval_start exactly
    floored = df["timestamp"].dt.floor(f"{INTERVAL_SECONDS}s")
    assert (floored.to_numpy() == df["interval_start"].to_numpy()).all()


def test_contact_id_unique(df):
    assert df["contact_id"].is_unique


def test_derived_columns_consistent(df):
    assert df["hour"].between(0, 23).all()
    # derived columns must match the timestamp they were derived from
    assert (df["hour"].to_numpy() == df["timestamp"].dt.hour.to_numpy()).all()
    assert (df["day_of_week"].to_numpy() == df["timestamp"].dt.day_name().to_numpy()).all()
    assert (df["date"].dt.normalize().to_numpy()
            == df["timestamp"].dt.normalize().to_numpy()).all()
