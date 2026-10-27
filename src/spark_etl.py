"""Spark ETL — the raw-data stages on PySpark, running locally (local[*]).

The pandas pipeline reads data/raw/contacts.csv (~4.7M rows) in three places: forecast (daily
series + intraday profile), optimize (normal-day AHT per interval, IROP AHT, deflectable share)
and dashboard (daily actuals, reason mix). This module does that raw-data work once, in Spark:

  contacts.csv --explicit schema, FAILFAST--> cleaned Parquet (data/interim/contacts.parquet)
  Parquet --one scan--> cube: date x interval x IROP x reason cells (counts + integer sums)
  cube --small groupBys--> the aggregates the downstream stages consume (data/interim/spark/)

Cleaning = exactly what the pandas readers do, nothing more: parse the True/False booleans and
derive the half-hour bucket (interval_index) from interval_start. No rows are dropped — a filter
the pandas path doesn't apply would change the outputs. The header must match the schema and a
malformed value fails the read (FAILFAST) instead of silently becoming null. Timestamps are read
timezone-naive (TimestampNTZ), like pandas, so daylight-saving days don't shift buckets.

Exactness: Spark computes only counts and integer sums (exact in LongType); the final divisions
happen in pandas/numpy exactly as the pandas path does them, so means and fractions are
bit-identical to pandas, not merely close. The small results are collected to the driver and
written with pandas, so number formatting matches the pandas-written files (Spark's CSV writer
would emit 3.2E-4-style numbers into a directory of part files).

Run:  python -m src.spark_etl                # CSV -> Parquet -> aggregates
      python -m src.spark_etl --parity-full  # full-data pandas-vs-Spark parity + wall-clock times
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402
from src.generate_data import RAW_CSV_PATH  # noqa: E402

INTERIM_DIR = PROJECT_ROOT / "data" / "interim"          # git-ignored, regenerable
PARQUET_PATH = INTERIM_DIR / "contacts.parquet"
SPARK_OUT_DIR = INTERIM_DIR / "spark"
# Output file names inside the output directory (one aggregate per file).
DAILY_SERIES_NAME = "daily_series.csv"        # ds, y, irop_flag          -> forecast, dashboard
INTRADAY_NAME = "intraday_profile.csv"        # same columns as forecast's -> forecast, optimize
INTERVAL_AHT_NAME = "interval_aht.csv"        # interval_index, aht_seconds -> optimize, dashboard
REASON_MIX_NAME = "reason_mix.csv"            # same columns as dashboard's -> dashboard
SCALARS_NAME = "scalars.json"                 # n_contacts, aht_irop, deflectable_share

INTERVALS_PER_HOUR = 60 // config.INTERVAL_MINUTES        # 2
INTERVALS_PER_DAY = 24 * INTERVALS_PER_HOUR              # 48
TOLERANCE = 1e-6                                         # parity tolerance (spec'd by the user)

# One row = one contact; field order and names match generate_data.COLUMNS (and the CSV header,
# which Spark checks because enforceSchema is off).
CONTACT_SCHEMA = T.StructType([
    T.StructField("contact_id", T.StringType()),
    T.StructField("timestamp", T.TimestampNTZType()),
    T.StructField("date", T.DateType()),
    T.StructField("interval_start", T.TimestampNTZType()),
    T.StructField("hour", T.ShortType()),
    T.StructField("day_of_week", T.StringType()),
    T.StructField("contact_reason", T.StringType()),
    T.StructField("customer_segment", T.StringType()),
    T.StructField("handle_time_seconds", T.IntegerType()),
    T.StructField("irop_flag", T.BooleanType()),
    T.StructField("resolved_first_contact", T.BooleanType()),
    T.StructField("repeat_contact", T.BooleanType()),
    T.StructField("abandoned", T.BooleanType()),
])


# --------------------------------------------------------------------------------------
# Session + read/clean/write
# --------------------------------------------------------------------------------------
def get_spark(app_name: str = "staffing-etl") -> SparkSession:
    """Local Spark on every core. UI and console progress bars off; few shuffle partitions
    because the aggregates are tiny (partitioning never changes the results — every sum is exact).

    Session start-up imports pyspark.testing, whose import-time check warns that PySpark 4.2
    "does not yet fully support pandas >= 3.0". That concerns PySpark's pandas interop (toPandas,
    pandas UDFs, createDataFrame from pandas), none of which this module uses — results come back
    as Rows and are turned into pandas here — so that one warning is silenced during start-up."""
    builder = (SparkSession.builder.master("local[*]").appName(app_name)
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.enabled", "false")
               .config("spark.ui.showConsoleProgress", "false")
               .config("spark.sql.shuffle.partitions", "8")
               .config("spark.driver.memory", "2g"))
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="PySpark does not yet fully support pandas",
                                category=FutureWarning)
        spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _interval_index(ts):
    """Half-hour bucket of the day — the pandas readers' rule (hour * 2 + [minute == 30]),
    applied to a typed timestamp instead of string slicing."""
    return (F.hour(ts) * INTERVALS_PER_HOUR + (F.minute(ts) == 30).cast("int")).cast("int")


def read_contacts(spark: SparkSession, path: Path = RAW_CSV_PATH) -> DataFrame:
    """contacts.csv -> typed, cleaned DataFrame (+ the derived interval_index)."""
    df = (spark.read.schema(CONTACT_SCHEMA)
          .option("header", True)
          .option("enforceSchema", False)      # header names must match the schema
          .option("mode", "FAILFAST")          # malformed value -> error, not a silent null
          .option("timestampNTZFormat", "yyyy-MM-dd HH:mm:ss")
          .option("dateFormat", "yyyy-MM-dd")
          .csv(str(path)))
    return df.withColumn("interval_index", _interval_index(F.col("interval_start")))


def write_parquet(df: DataFrame, path: Path = PARQUET_PATH) -> None:
    df.write.mode("overwrite").parquet(str(path))


# --------------------------------------------------------------------------------------
# Aggregates (Spark: exact integer counts/sums; pandas: the final divisions)
# --------------------------------------------------------------------------------------
def contact_cube(contacts: DataFrame) -> DataFrame:
    """One scan -> one row per (date, interval, IROP, reason) cell with its contact count, total
    handle time and repeat count (~120k rows). Every downstream aggregate is a sum over cells."""
    return (contacts.groupBy("date", "interval_index", "irop_flag", "contact_reason")
            .agg(F.count(F.lit(1)).alias("n"),
                 F.sum("handle_time_seconds").alias("ht_sum"),          # LongType: exact
                 F.sum(F.col("repeat_contact").cast("long")).alias("repeat_n")))


def _ratio(num, den) -> float:
    """float64(sum) / float64(count) — how pandas computes a mean; NaN when empty, like pandas."""
    return float(num) / float(den) if den else float("nan")


def aggregates(contacts: DataFrame) -> dict:
    """Every aggregate the downstream stages read from the raw data, as pandas objects."""
    cube = contact_cube(contacts).cache()
    try:
        daily_rows = (cube.groupBy("date")
                      .agg(F.sum("n").alias("y"), F.max("irop_flag").alias("irop_flag"))
                      .orderBy("date").collect())
        bucket = pd.DataFrame([r.asDict() for r in cube.groupBy("interval_index", "irop_flag")
                               .agg(F.sum("n").alias("n"), F.sum("ht_sum").alias("ht_sum"))
                               .collect()])
        cells = pd.DataFrame([r.asDict() for r in cube.groupBy("irop_flag", "contact_reason")
                              .agg(F.sum("n").alias("n"), F.sum("ht_sum").alias("ht_sum"),
                                   F.sum("repeat_n").alias("repeat_n"))
                              .collect()])
    finally:
        cube.unpersist()

    # Daily series (forecast's daily[ds, y] + its per-date IROP flag; dashboard's daily actuals).
    daily = pd.DataFrame({
        "ds": [r["date"].isoformat() for r in daily_rows],
        "y": np.array([r["y"] for r in daily_rows], dtype=np.int64),
        "irop_flag": np.array([bool(r["irop_flag"]) for r in daily_rows]),
    })

    # Intraday profile — same construction as forecast.intraday_profile.
    counts = (bucket.groupby("interval_index")["n"].sum()
              .reindex(range(INTERVALS_PER_DAY), fill_value=0).sort_index())
    fraction = counts / counts.sum()
    idx = np.arange(INTERVALS_PER_DAY)
    profile = pd.DataFrame({
        "interval_index": idx,
        "interval_start": [f"{b // 2:02d}:{30 * (b % 2):02d}" for b in idx],
        "fraction": fraction.to_numpy(),
    })

    # Normal-day AHT per interval (optimize / dashboard), empty buckets filled with the overall
    # normal-day mean — same rule as optimize._historical_aht_and_mix.
    is_irop = cells["irop_flag"].astype(bool)
    normal_cells = cells[~is_irop]
    normal_mean = _ratio(normal_cells["ht_sum"].sum(), normal_cells["n"].sum())
    normal_bucket = bucket[~bucket["irop_flag"].astype(bool)].set_index("interval_index")
    aht = (normal_bucket["ht_sum"].astype(np.float64) / normal_bucket["n"].astype(np.float64))
    aht = aht.reindex(range(INTERVALS_PER_DAY)).fillna(normal_mean).to_numpy()

    irop_cells = cells[is_irop]
    deflectable = normal_cells["contact_reason"].isin(config.DEFLECTABLE_REASONS)
    scalars = {
        "n_contacts": int(cells["n"].sum()),
        "aht_irop": _ratio(irop_cells["ht_sum"].sum(), irop_cells["n"].sum()),
        "deflectable_share": _ratio(normal_cells.loc[deflectable, "n"].sum(), normal_cells["n"].sum()),
    }

    # Reason mix — same columns, rounding and ordering as dashboard._reason_mix.
    by_reason = cells.groupby("contact_reason")[["n", "ht_sum", "repeat_n"]].sum()
    reason_mix = pd.DataFrame({
        "contacts": by_reason["n"],
        "avg_AHT": (by_reason["ht_sum"] / by_reason["n"]).round(1),
        "repeat_rate": (by_reason["repeat_n"] / by_reason["n"]).round(4),
    }).reset_index().rename(columns={"contact_reason": "reason"})
    reason_mix = reason_mix.sort_values("contacts", ascending=False)

    return {"daily": daily, "profile": profile, "aht": aht, "reason_mix": reason_mix,
            "scalars": scalars}


# --------------------------------------------------------------------------------------
# Outputs
# --------------------------------------------------------------------------------------
def write_outputs(aggs: dict, out_dir: Path = SPARK_OUT_DIR) -> list[Path]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = [out_dir / n for n in (DAILY_SERIES_NAME, INTRADAY_NAME, INTERVAL_AHT_NAME,
                                   REASON_MIX_NAME, SCALARS_NAME)]
    aggs["daily"].to_csv(paths[0], index=False)
    aggs["profile"].to_csv(paths[1], index=False)
    pd.DataFrame({"interval_index": np.arange(INTERVALS_PER_DAY),
                  "aht_seconds": aggs["aht"]}).to_csv(paths[2], index=False)
    aggs["reason_mix"].to_csv(paths[3], index=False)
    paths[4].write_text(json.dumps(aggs["scalars"], indent=2))
    return paths


def load_outputs(out_dir: Path = SPARK_OUT_DIR) -> dict:
    """Read the ETL outputs back. round_trip float parsing gives back exactly the doubles that
    were written (pandas' default C parser can be off by one ulp)."""
    out_dir = Path(out_dir)
    rt = {"float_precision": "round_trip"}
    return {
        "daily": pd.read_csv(out_dir / DAILY_SERIES_NAME, **rt),
        "profile": pd.read_csv(out_dir / INTRADAY_NAME, **rt),
        "aht": pd.read_csv(out_dir / INTERVAL_AHT_NAME, **rt),
        "reason_mix": pd.read_csv(out_dir / REASON_MIX_NAME, **rt),
        "scalars": json.loads((out_dir / SCALARS_NAME).read_text()),
    }


def run_etl(csv_path: Path = RAW_CSV_PATH, parquet_path: Path = PARQUET_PATH,
            out_dir: Path = SPARK_OUT_DIR, spark: SparkSession | None = None) -> dict:
    """CSV -> cleaned Parquet -> aggregates -> small CSVs. Starts (and stops) its own session
    unless one is passed in. Returns the aggregates, written paths and wall-clock timings."""
    timings = {}
    t = time.perf_counter()
    own_session = spark is None
    if own_session:
        spark = get_spark()
    timings["session_start"] = time.perf_counter() - t
    try:
        t = time.perf_counter()
        write_parquet(read_contacts(spark, csv_path), parquet_path)
        timings["csv_to_parquet"] = time.perf_counter() - t
        t = time.perf_counter()
        aggs = aggregates(spark.read.parquet(str(parquet_path)))
        paths = write_outputs(aggs, out_dir)
        timings["parquet_to_aggregates"] = time.perf_counter() - t
        version = spark.version
    finally:
        if own_session:
            spark.stop()
    return {"aggs": aggs, "paths": paths, "timings": timings, "spark_version": version}


# --------------------------------------------------------------------------------------
# Parity with the pandas path
# --------------------------------------------------------------------------------------
def pandas_reference(csv_path: Path = RAW_CSV_PATH) -> dict:
    """The same aggregates from the UNCHANGED pandas pipeline functions, with per-stage
    wall-clock times. optimize and dashboard read RAW_CSV_PATH directly, so it is patched for
    the duration (only matters when csv_path isn't the default)."""
    from unittest import mock
    from src import dashboard, forecast, optimize

    csv_path = Path(csv_path)
    timings = {}
    with mock.patch.object(optimize, "RAW_CSV_PATH", csv_path), \
         mock.patch.object(dashboard, "RAW_CSV_PATH", csv_path):
        t = time.perf_counter()
        contacts = forecast._read_contacts(csv_path)
        daily, irop = forecast.daily_series(contacts)
        profile = forecast.intraday_profile(contacts)
        del contacts
        timings["forecast"] = time.perf_counter() - t

        t = time.perf_counter()
        aht, aht_irop, share = optimize._historical_aht_and_mix()
        timings["optimize"] = time.perf_counter() - t

        t = time.perf_counter()
        contacts = dashboard._read_contacts()
        daily_actual = dashboard._daily_actuals(contacts)
        reason_mix = dashboard._reason_mix(contacts)
        dash_aht, dash_share = dashboard._aht_and_deflectable(contacts)
        del contacts
        timings["dashboard"] = time.perf_counter() - t
    return {"daily": daily, "irop": irop, "profile": profile, "aht": aht, "aht_irop": aht_irop,
            "deflectable_share": share, "daily_actual": daily_actual, "reason_mix": reason_mix,
            "dash_aht": dash_aht, "dash_share": dash_share, "timings": timings}


def _max_diff(a, b) -> float:
    """max |a - b| (0.0 = bit-identical); NaN in both counts as equal; shape mismatch = inf."""
    a, b = np.asarray(a, dtype=float), np.asarray(b, dtype=float)
    if a.shape != b.shape:
        return float("inf")
    if not a.size:
        return 0.0
    d = np.where(np.isnan(a) & np.isnan(b), 0.0, np.abs(a - b))
    return float(np.nan_to_num(d, nan=np.inf).max())


def _same(a, b) -> float:
    """0.0 if two label sequences are identical, else inf (so it fails any tolerance)."""
    return 0.0 if list(a) == list(b) else float("inf")


def compare(ref: dict, out: dict) -> list[tuple[str, float]]:
    """(check, max |pandas - spark|) for every aggregate. Labels/keys/ordering must match exactly
    (0.0 or inf); numeric values are compared as max absolute difference."""
    d, p, a, rm = out["daily"], out["profile"], out["aht"], out["reason_mix"]
    s = out["scalars"]
    ref_rm = ref["reason_mix"]
    return [
        ("daily: dates", _same(ref["daily"]["ds"].dt.strftime("%Y-%m-%d"), d["ds"])),
        ("daily: contacts per day", _max_diff(ref["daily"]["y"], d["y"])),
        ("daily: IROP flag per day", _same(ref["irop"].to_numpy(), d["irop_flag"])),
        ("dashboard daily actuals: dates", _same(ref["daily_actual"].index.astype(str), d["ds"])),
        ("dashboard daily actuals: counts", _max_diff(ref["daily_actual"], d["y"])),
        ("intraday: columns + interval labels",
         _same(list(ref["profile"].columns) + list(ref["profile"]["interval_start"]),
               list(p.columns) + list(p["interval_start"]))),
        ("intraday: fraction", _max_diff(ref["profile"]["fraction"], p["fraction"])),
        ("AHT per interval (optimize)", _max_diff(ref["aht"], a["aht_seconds"])),
        ("AHT per interval (dashboard)", _max_diff(ref["dash_aht"], a["aht_seconds"])),
        ("IROP AHT", _max_diff(ref["aht_irop"], s["aht_irop"])),
        ("deflectable share (optimize)", _max_diff(ref["deflectable_share"], s["deflectable_share"])),
        ("deflectable share (dashboard)", _max_diff(ref["dash_share"], s["deflectable_share"])),
        ("reason mix: columns + reason order",
         _same(list(ref_rm.columns) + list(ref_rm["reason"]), list(rm.columns) + list(rm["reason"]))),
        ("reason mix: contacts", _max_diff(ref_rm["contacts"], rm["contacts"])),
        ("reason mix: avg_AHT", _max_diff(ref_rm["avg_AHT"], rm["avg_AHT"])),
        ("reason mix: repeat_rate", _max_diff(ref_rm["repeat_rate"], rm["repeat_rate"])),
    ]


def parity_full(csv_path: Path = RAW_CSV_PATH) -> int:
    """Full-data parity + wall-clock comparison (manual check). Returns a process exit code."""
    print(f"Full-data parity on {Path(csv_path).relative_to(PROJECT_ROOT)} — pandas path first, "
          f"then Spark...")
    ref = pandas_reference(csv_path)
    etl = run_etl(csv_path)
    checks = compare(ref, load_outputs())
    worst = max(diff for _, diff in checks)

    tp, ts = ref["timings"], etl["timings"]
    pandas_total = sum(tp.values())
    spark_work = ts["csv_to_parquet"] + ts["parquet_to_aggregates"]
    print("=" * 72)
    print(f"SPARK vs PANDAS — full-data parity ({etl['aggs']['scalars']['n_contacts']:,} contacts, "
          f"Spark {etl['spark_version']} local[*])")
    print("=" * 72)
    print(f"{'check':<40} {'max |diff|':>12}   result")
    print("-" * 72)
    for name, diff in checks:
        verdict = "bit-identical" if diff == 0 else ("within 1e-6" if diff <= TOLERANCE else "FAIL")
        print(f"{name:<40} {diff:>12.3g}   {verdict}")
    print("-" * 72)
    print("Wall-clock (seconds):")
    print(f"  pandas  forecast   read + daily + intraday         {tp['forecast']:>7.1f}")
    print(f"          optimize   read + AHT / IROP / share       {tp['optimize']:>7.1f}")
    print(f"          dashboard  read + actuals / mix / AHT      {tp['dashboard']:>7.1f}")
    print(f"          total (reads contacts.csv 3x)              {pandas_total:>7.1f}")
    print(f"  spark   session start (JVM)                        {ts['session_start']:>7.1f}")
    print(f"          CSV -> Parquet (read, clean, write)        {ts['csv_to_parquet']:>7.1f}")
    print(f"          Parquet -> aggregates + small CSVs         {ts['parquet_to_aggregates']:>7.1f}")
    print(f"          total excl. / incl. session start          "
          f"{spark_work:>7.1f} / {spark_work + ts['session_start']:.1f}")
    print("-" * 72)
    ok = worst <= TOLERANCE
    print(f"Worst difference {worst:.3g} -> {'PASS' if ok else 'FAIL'} at tolerance {TOLERANCE:g}")
    print("=" * 72)
    return 0 if ok else 1


# --------------------------------------------------------------------------------------
# Entry points
# --------------------------------------------------------------------------------------
def run() -> dict:
    """Run the ETL with the default paths and print a summary (the pipeline step)."""
    print(f"Spark ETL: {RAW_CSV_PATH.relative_to(PROJECT_ROOT)} -> Parquet -> aggregates...")
    etl = run_etl()
    parquet_files = list(PARQUET_PATH.glob("*.parquet"))
    parquet_mb = sum(f.stat().st_size for f in parquet_files) / 1e6
    s, t = etl["aggs"]["scalars"], etl["timings"]
    print("=" * 64)
    print(f"SPARK ETL — Spark {etl['spark_version']}, local[*]")
    print("=" * 64)
    print(f"Contacts                  : {s['n_contacts']:,}")
    print(f"Days covered              : {len(etl['aggs']['daily'])}")
    print(f"Cleaned Parquet           : {PARQUET_PATH.relative_to(PROJECT_ROOT)} "
          f"({len(parquet_files)} files, {parquet_mb:.0f} MB)")
    print(f"IROP AHT / deflectable    : {s['aht_irop']:.1f}s / {s['deflectable_share']*100:.1f}%")
    print(f"Wall-clock                : session {t['session_start']:.1f}s | CSV -> Parquet "
          f"{t['csv_to_parquet']:.1f}s | aggregates {t['parquet_to_aggregates']:.1f}s")
    print(f"Wrote                     : {SPARK_OUT_DIR.relative_to(PROJECT_ROOT)}/ "
          f"({', '.join(p.name for p in etl['paths'])})")
    print("=" * 64)
    return etl


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Spark ETL for the contact-center pipeline.")
    parser.add_argument("--parity-full", action="store_true",
                        help="compare against the pandas path on the full dataset and time both")
    args = parser.parse_args(argv)
    if args.parity_full:
        sys.exit(parity_full())
    run()


if __name__ == "__main__":
    main()
