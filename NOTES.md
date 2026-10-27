# Engineering notes — Spark + Airflow path

Working notes for the distributed-computing path added on top of the original pandas
pipeline. The pandas path (`python run_all.py`) is unchanged and remains the reference.

**Environment for every number below:** Apple M3 (8 cores, 16 GB), macOS (Darwin 25.6),
Python 3.11.14 (pyenv), pandas 3.0.3, PySpark 4.2.0 on Oracle JDK 25.0.1 (the machine's
default `java`; `JAVA_HOME` unset). Data: the seeded synthetic `contacts.csv`, 4,740,671 rows,
570 MB.

## Baseline (before any change)

`run_all.py` run in a scratch copy of the repo, every committed output compared with `cmp`:

- **15 of 17 outputs reproduce byte for byte**: every CSV and JSON in `data/processed/` and
  `outputs/dashboard/`, and all five PNGs. `contacts.csv` also regenerates identically.
- **2 outputs differ on every run, by construction, with identical content:**
  - `outputs/dashboard/dashboard.html` — Plotly gives the chart a random `uuid4` div id each run
    (identical once the id is masked).
  - `outputs/Delta_Reservations_Capstone.pptx` — python-pptx stamps each zip entry with the
    wall-clock time (every zip member's bytes are identical).

  Left as-is. Byte-for-byte comparisons in these notes cover the 15 reproducible outputs.
- Test suite: 26 passed in 17.5 s.

## Phase 1 — Spark ETL (`src/spark_etl.py`)

**What it does.** One pass in local Spark replaces the three separate pandas reads of
`contacts.csv` (forecast, optimize, dashboard):

1. Read `contacts.csv` with an explicit 13-column schema. The header must match the schema
   (`enforceSchema=false`), a malformed value fails the read (`FAILFAST`), and timestamps are
   timezone-naive (`TimestampNTZ`).
2. Write the cleaned rows plus the derived `interval_index` to `data/interim/contacts.parquet`
   (8 files, 62 MB vs 570 MB of CSV).
3. Scan the Parquet once into a cube of date × interval × IROP × reason cells, holding counts and
   integer sums. Every aggregate downstream is a sum over those cells.
4. Collect the small results and write them with pandas to `data/interim/spark/`:
   - `daily_series.csv` (`ds, y, irop_flag`)
   - `intraday_profile.csv` (same columns as the pandas one)
   - `interval_aht.csv` (normal-day AHT per half-hour)
   - `reason_mix.csv` (same columns as the dashboard's)
   - `scalars.json` (IROP AHT, deflectable share, row count)

**Why it's bit-identical rather than within 1e-6.** Spark only computes counts and integer sums,
which are exact in `LongType` whatever the partitioning or summation order. The final divisions
(`sum / count`) happen in pandas/numpy the same way the pandas path does them. Small outputs are
written with pandas (not Spark's CSV writer, which would print `3.2E-4`-style numbers into a
directory of part files) and read back with `float_precision="round_trip"`.

**Parity, full data** (`python -m src.spark_etl --parity-full`): all 16 checks have a max
|difference| of **0** — every count, fraction, mean, label and ordering for the daily series,
dashboard daily actuals, intraday profile, per-interval AHT, IROP AHT, deflectable share and
reason mix. Spark's `intraday_profile.csv` and `reason_mix.csv` are byte-identical to the
committed pandas-written `data/processed/intraday_profile.csv` and
`outputs/dashboard/reason_mix.csv`.

**Parity, automated** (`tests/test_spark_parity.py`, 6 tests, ~7 s): same 16 checks at
tolerance 1e-6 on a 90-day seeded dataset. I checked the comparison isn't vacuous by injecting
errors into the Spark outputs. A +2e-6 fraction, a one-call daily change, a swapped reason
order, a +1e-3 IROP AHT and a flipped IROP flag were each caught by exactly the expected checks.

**Wall-clock, full 4,740,671 rows** (three consecutive runs, warm file cache, seconds):

| Step | Run 1 | Run 2 | Run 3 |
|---|---:|---:|---:|
| pandas — forecast (read + daily + intraday) | 2.9 | 3.0 | 2.9 |
| pandas — optimize (read + AHT / IROP / share) | 2.9 | 2.9 | 3.0 |
| pandas — dashboard (read + actuals / mix / AHT) | 3.3 | 3.4 | 3.4 |
| **pandas total** (reads the CSV 3×) | **9.1** | **9.3** | **9.3** |
| Spark — session start (JVM) | 3.0 | 2.6 | 2.2 |
| Spark — CSV → Parquet (read, clean, write) | 6.1 | 5.5 | 5.4 |
| Spark — Parquet → aggregates + small CSVs | 1.7 | 1.7 | 1.8 |
| **Spark total** excl. / incl. session start | **7.8 / 10.8** | **7.2 / 9.8** | **7.1 / 9.3** |

Read honestly: at 570 MB on one laptop the two paths are about break-even end to end. Spark is
not faster here once JVM start-up counts. What Spark changes is the shape of the work. The raw
CSV is parsed once instead of three times, the curated layer is 62 MB of columnar Parquet, and
aggregating from it takes 1.7–1.8 s against ~3 s per pandas stage that re-parses the CSV.

**Decisions and things I was unsure about**

- *"Same cleaning rules as pandas."* The pandas path has no cleaning step. Its readers only parse
  `True`/`False` and derive the half-hour bucket from `interval_start`, and they drop no rows. The
  Spark read mirrors exactly that. The schema, header check and FAILFAST make malformed input fail
  loudly but change nothing on valid data. I didn't add filters (e.g. dropping out-of-range handle
  times), because a rule pandas doesn't apply would change the outputs.
- *The header/FAILFAST checks only fire when columns are parsed.* The ETL's Parquet write parses
  every column, so they always apply there. But a bare `read_contacts(...).count()` skips parsing
  and would not catch a bad header or value.
- *"Sampled subset" for the parity test* is a 90-day dataset from the same seeded generator, not
  a row sample of `contacts.csv`. That keeps the test fast, deterministic and independent of the
  570 MB file. The full file is covered by the manual `--parity-full` check.
- *PySpark 4.2.0 warns it "does not yet fully support pandas >= 3.0".* The warning comes from an
  import-time version check (`pyspark/testing/utils.py`) during session start-up. It concerns
  PySpark's pandas interop (`toPandas`, pandas UDFs, `createDataFrame` from pandas), which this
  module never uses: results come back as `Row`s and are converted to pandas in our code. The
  zero-difference parity above confirms it. `get_spark()` silences that one message, scoped to
  start-up. If a later change starts using `toPandas()`, revisit this.
- *Two harmless JVM start-up lines remain on stderr* ("Using incubator modules:
  jdk.incubator.vector", "package sun.security.action not in java.base"). They come from running
  on JDK 25.
- *Cost of the dependency:* `pyspark` is a 484 MB install. It added exactly two packages
  (`pyspark==4.2.0`, `py4j==0.10.9.9`), and no existing pin moved (`pip freeze` diff,
  `pip check` clean).
- *Test suite:* 32 passed in 25.0 s (was 26 in 17.5 s). The Spark tests skip with a reason if
  pyspark is missing or no Java runtime starts.
- *Pandas path unaffected:* after Phase 1, `run_all.py` from a copy of the working tree still
  reproduces all 15 reproducible outputs byte for byte.

## Phase 2 — Wire-in (`run_all.py --engine spark`)

**What changed.**
- **`run_all.py`** takes `--engine pandas|spark` (default `pandas`). The default runs the
  original `STEPS` list, unchanged. `spark` inserts the Spark ETL after Phase 1 and passes
  `engine="spark"` to forecast, optimize and dashboard. `pyspark` is imported only on the Spark
  branch, so the pandas engine still needs neither pyspark nor Java.
- **`forecast.py`, `optimize.py`, `dashboard.py`** each have one small input function
  (`_history`, `_handle_time_inputs`, `_history_aggregates`). Its pandas branch is the original
  code moved verbatim, including the original print messages. Its Spark branch reads the ETL
  outputs. An unknown engine name raises `ValueError` rather than silently falling back to pandas.
- **`spark_etl.py`** adds `forecast_inputs`, `optimize_inputs` and `dashboard_inputs`. They return
  the Spark outputs shaped exactly like the pandas readers' return values: same columns, dtypes
  and index names. Everything after a stage's input step is untouched.

**Verification — full pipeline, both engines, each in a fresh scratch copy of the working tree**
(regenerating `contacts.csv` from scratch):

- **`forecast.csv` and `staffing_plan.csv` are byte-identical between engines**, and so is every
  other CSV, JSON and PNG: 15 of 15 reproducible outputs. All 15 are also byte-identical to the
  committed files.
- `dashboard.html` and the deck are identical in content (div uuid masked / every zip member
  equal). Their bytes differ per run on either engine, as in the baseline.
- **No difference between Spark and pandas anywhere**, so there is nothing to explain.
- The pandas engine's console output is identical to the pre-change baseline log (timings
  masked).
- End-to-end wall-clock: **33 s on both engines**.

  | Engine | Raw-data work | Forecast / optimize / dashboard |
  |---|---|---|
  | pandas | spread across forecast 3 s, optimize 3 s, dashboard 4 s (each re-reads the CSV) | 3 s / 3 s / 4 s |
  | spark | one Spark ETL step, 9 s | ~0 s each (they read 5 small files) |

- New tests (3, in `tests/test_spark_parity.py`): what each stage receives on the Spark engine is
  exactly equal (`check_exact`, dtypes, index names) to its pandas reader's return value.
  Full suite: 35 passed in 24.6 s.

**Things I was unsure about**

- `run_all.py` still ends with "Run `pytest` to verify the three test suites" even though there
  are now four test files. I left the pandas engine's output byte-for-byte as it was; the README
  pass in Phase 4 can fix the wording if you want.
- Individual stage modules don't take an `--engine` flag on the command line
  (`python -m src.forecast` stays pandas). The Spark engine is reached through
  `run_all.py --engine spark` or by calling `main(engine="spark")`, which is what the Airflow
  tasks in Phase 3 will do.
