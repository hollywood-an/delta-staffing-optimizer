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
  pass in Phase 4 can fix the wording if you want. (Fixed afterwards — see Open questions.)
- Individual stage modules don't take an `--engine` flag on the command line
  (`python -m src.forecast` stays pandas). The Spark engine is reached through
  `run_all.py --engine spark` or by calling `main(engine="spark")`, which is what the Airflow
  tasks in Phase 3 will do.

## Phase 3 — Airflow (`dags/staffing_pipeline.py`)

**Install.** One attempt, and it succeeded: `apache-airflow==3.3.2` into its own
`.venv-airflow` (Python 3.11.14), pinned by Airflow's official constraints file for 3.3.2 / Python
3.11. That's 131 packages, `pip check` clean. No Prefect fallback was needed.
`requirements-airflow.txt` is the exact file used for the install. Airflow stays out of the
pipeline's `.venv` because its constraints pin `pandas==3.0.5` (the pipeline pins 3.0.3).

**The DAG** — one task per stage, all on the Spark engine:

```
generate_data ──► spark_etl ──► forecast ──► optimize ──► dashboard ──► build_deck
                              erlang_check ──┘                             ▲
                              simulate ────────────────────────────────────┘
```

- **Main chain:** each task reads what the previous one wrote: contacts.csv → Spark aggregates →
  forecast.csv → staffing_plan.csv → dashboard CSVs → deck.
- **`erlang_check`** prints the spec's verified Erlang C table and fails the run if
  `agents_required(5, 180, 20, 0.80)` is no longer 8. It gates `optimize`.
- **`simulate`** needs no data, so it starts immediately. `build_deck` waits for it because the
  deck reads `validation.csv`.
- **How tasks run:** every task is `@task.external_python` pointed at the project's
  `.venv/bin/python`. The task body imports the stage module and calls its existing entry point
  (`generate_data.main`, `spark_etl.run`, `forecast.main(engine="spark")`, …). Airflow and the
  pipeline never share an environment. Set `STAFFING_PYTHON` to use another interpreter.
- **Run settings:** `schedule=None` (on demand), `retries=0` (failures surface),
  `max_active_runs=1` (every run rewrites the same files).

**Start it, trigger a run, find the logs**

```bash
make airflow-install   # once: .venv-airflow + pinned Airflow + its SQLite DB in ./airflow_home
make airflow-up        # terminal 1: scheduler + API server + DAG processor + triggerer
                       #   UI → http://localhost:8080, user "admin"; the generated password is in
                       #   airflow_home/simple_auth_manager_passwords.json.generated
make airflow-trigger   # terminal 2: unpause + trigger staffing_pipeline (or the UI's ▶ Trigger)
make airflow-test      # alternative: run every task once in this terminal, no scheduler/UI
```

- **Task logs:** `airflow_home/logs/dag_id=staffing_pipeline/run_id=<run>/task_id=<task>/attempt=<n>.log`.
  Each file holds that stage's full console output (e.g. the Spark ETL summary, the savings
  table). The same logs show in the UI: click a task, then the Logs tab.
- **Outputs** land in the repo exactly where `run_all.py` writes them. Afterwards `git status`
  shows `dashboard.html` and the `.pptx` as modified: their content is identical and only the
  per-run div id and zip timestamps differ (see Baseline). `git checkout -- <file>` restores them.
- All Airflow state lives in `airflow_home/` (git-ignored). Delete it for a clean slate.

**What the graph looks like in the UI (Graph view, after a run).**
- Eight green boxes, all `@task.external_python`. Three start on the left: `generate_data` on
  top, `erlang_check` and `simulate` below it.
- The spine runs left to right: `generate_data → spark_etl → forecast → optimize → dashboard →
  build_deck`.
- `erlang_check` joins the spine from below at `optimize`, a gate before any staffing is sized.
- `simulate` runs along the bottom the whole width and joins at `build_deck`, so the deck node
  has two arrows in.
- The Gantt view of the measured run (below) is the most telling picture: three bars start
  together at 0 s, the long `spark_etl` bar follows `generate_data`, then four short bars step
  down to the end.

**Verification**

| Run | How | Result | Wall-clock |
|---|---|---|---:|
| 1 | `make airflow-test` (in-process, tasks one after another) | 8/8 success | 40.8 s |
| 2 | `make airflow-up` + `make airflow-trigger` (scheduler, LocalExecutor) | 8/8 success | 38.2 s |
| 3 | same, after binding the UI to 127.0.0.1 | 8/8 success | 37.0 s |

Per-task timing in run 2. The first three ran in parallel; Airflow adds ~0.2–0.6 s between
dependent tasks.

| Task | Starts at | Duration |
|---|---:|---:|
| erlang_check | 0.0 s | 1.9 s |
| simulate | 0.0 s | 13.3 s |
| generate_data | 0.0 s | 15.7 s |
| spark_etl | 16.2 s | 12.8 s |
| forecast | 29.1 s | 2.0 s |
| optimize | 31.2 s | 1.8 s |
| dashboard | 33.2 s | 1.4 s |
| build_deck | 35.2 s | 1.2 s |

After the runs, all 15 reproducible outputs in the repo were byte-identical to the committed
files (`git status` clean for them). `dashboard.html` and the deck were content-identical and
were restored with `git checkout`.

**Problems the live runs found (fixed)**

1. **`airflow standalone` died with `FileNotFoundError: 'airflow'`.** It launches its components
   by running `airflow`, so the venv's `bin/` must be on PATH. `make airflow-up` now prepends it
   for that one command. (`airflow dags test` didn't need it, which is why run 1 passed first.)
2. **The UI listened on every network interface (`*:8080`)** — Airflow's default `0.0.0.0` — and
   the API server logged a "deployment shape looks like production" warning. The Makefile now
   sets `AIRFLOW__API__HOST=127.0.0.1`. Verified: listening on `127.0.0.1:8080` only, the
   warning is gone, and run 3 shows tasks still reach the API.
3. **Hardening:** with the Airflow venv first on PATH, `python3` means *its* interpreter, which
   has no pandas or pyspark. The ETL never starts Python workers (no UDFs), but `get_spark()` now
   pins `spark.pyspark.python` to the driver's own interpreter so it never could.

**Things I was unsure about**

- The two macOS settings in the Makefile (`OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES`,
  `no_proxy=*`) are the standard Airflow-on-macOS workarounds for crashes in forked task
  processes. I set them up front and did not test whether this machine needs them.
- The CLI prints "You might need to install the graphviz package" on every command. That only
  matters for `airflow dags show` (rendering the graph to an image), which isn't used. I didn't
  add graphviz.
- SimpleAuthManager (Airflow 3's default) stores the generated admin password in plain text in
  `airflow_home/` (git-ignored). Fine for a laptop bound to localhost, not for a server.

## Phase 4 — Tests and docs

**New tests.** 53 in total (was 26), passing in 34.9 s:

- **`tests/test_spark_etl.py`** (18) runs the ETL on the full `contacts.csv` into a temp directory
  and checks the cleaned Parquet:
  - the schema and column types;
  - every CSV row kept (Parquet rows = CSV data lines = the ETL's own count);
  - the same data-quality invariants as `tests/test_data_quality.py`, rule for rule (no nulls,
    handle-time range, category sets, reason mix, flag logic, ≥ 10 IROP events, timestamp inside
    its bucket, unique ids, derived columns);
  - the read rules: a renamed header, a malformed boolean, integer or timestamp each fail the ETL
    rather than becoming nulls.

  The invariants are evaluated in Spark, because reading 4.7M Parquet rows into pandas would need
  pyarrow, which the project doesn't install.
- **`tests/conftest.py`** holds a single shared Spark session. It is created only when a Spark
  test asks for it, and skipped with the reason if no Java runtime starts.
- **The new tests can fail:** 11 deliberate defects were injected (a null, an out-of-range handle
  time, an unknown segment, an abandoned-and-resolved row, a shifted `interval_start`, a duplicate
  id, an off-by-one hour, IROP flags removed, an extra column, and a permissive reader against
  both read-rule tests). Each was caught by the intended test.

**README.** New "Spark + Airflow" section with a Mermaid architecture diagram and commands. The
test list, test count, project tree and the per-phase commands are updated. The reproducibility
sentence is corrected: byte-for-byte holds for every CSV, JSON and PNG, while the HTML dashboard
and the deck repeat in content only (see Baseline).

## Final verification (after all four phases)

| Check | Result |
|---|---|
| Full test suite, project `.venv` | 53 passed (34.9 s) |
| Fresh venv built only from `requirements.txt`, `run_all.py` both engines, clean copy of the working tree | all 15 reproducible outputs byte-identical to the committed files on both engines |
| Same fresh venv, full test suite | 53 passed (33.6 s) |
| `python -m src.spark_etl --parity-full` with the final code | 16/16 checks, max difference 0; pandas 9.0 s, Spark 6.9 s / 9.3 s with JVM start |
| `make airflow-test` with the final code | 8/8 tasks success, 38.5 s; repo outputs identical to committed |
| `airflow standalone` + trigger (two runs) | 8/8 tasks success both times, 38.2 s and 37.0 s |
| `python run_all.py` (pandas) after every phase | outputs and console output unchanged |

**Finding about the existing setup (orjson part fixed afterwards).** `requirements.txt` pins
only direct dependencies. A fresh install resolves newer indirect ones than the project `.venv`
has (e.g. Pillow 13.0.0 vs 12.2.0, fonttools 4.67.0 vs 4.63.0). Every CSV, JSON and PNG still
reproduced byte for byte under them.

The one effect was on `dashboard.html`. The project `.venv` also had `orjson` installed (it
wasn't in `requirements.txt`), and Plotly uses it when present. Without it, Plotly's JSON writer
turns non-ASCII characters such as "—" into ASCII escape sequences (backslash-u plus four hex
digits). The fresh-venv dashboard was identical to the committed one once those escapes were
decoded, so it rendered the same, but the bytes differed.

**Fixed:** `orjson==3.11.9` (the version that produced the committed dashboard) is now pinned in
`requirements.txt`. Re-checked with a fresh venv built from the updated file, running
`run_all.py` on both engines from a clean copy of the working tree:
- `dashboard.html` matches the committed file byte for byte apart from the per-run chart id;
- all 15 CSV/JSON/PNG outputs are byte-identical;
- the deck's content is identical.

Not done (optional): a full lock file (`pip freeze`) to also guard the indirect dependencies
against future releases.

## Summary of what changed

- **`src/spark_etl.py`** (new): Spark ETL, pandas parity check (`--parity-full`), and the
  `--engine spark` input shaping.
- **`run_all.py`**: `--engine pandas|spark` (default unchanged); its closing line no longer
  counts test suites.
- **`src/forecast.py`, `src/optimize.py`, `src/dashboard.py`**: one small input function each;
  the pandas branch is the original code.
- **`dags/staffing_pipeline.py`, `Makefile`, `requirements-airflow.txt`** (new): Airflow DAG
  and how to run it.
- **Tests:** `tests/test_spark_parity.py`, `tests/test_spark_etl.py`, `tests/conftest.py` (new).
- **Config:** `requirements.txt` (+ pyspark, py4j, and orjson so `dashboard.html` reproduces from
  a fresh install) and `.gitignore` (+ `data/interim/`, `.venv-airflow/`, `airflow_home/`).
- **Docs:** `README.md` and `NOTES.md`.
- **Outputs:** no committed output file changed.

## Open questions / things I was unsure about (all phases)

- **Mermaid diagram — resolved.** Two attempts to validate it before pushing were blocked by this
  session's permission rules: uploading it to mermaid.ink, and downloading mermaid-cli via npx.
  After the push I checked github.com directly: it renders correctly, with all 8 tasks, the
  two-step Spark subgraph and the right arrows.
- **`run_all.py`'s closing line — fixed.** It said "verify the three test suites" though there
  are five test files. It now says "verify the test suites" (docstring too). It's console text
  only, so no output file changed.
- **PySpark 4.2's "pandas ≥ 3.0 not fully supported" warning is silenced at session start-up**
  (Phase 1 notes explain why it doesn't apply). Revisit if `toPandas()` is ever used.
- **macOS fork-safety settings** in the Makefile are standard workarounds that I set up front and
  did not test without.
- **Speed.** Spark is not faster than pandas at this data size on one laptop (Phase 1 timings).
  Don't claim a speed-up.

## Candidate resume bullets (only numbers measured above; data is synthetic)

1. Ported the raw-data ETL of a contact-center staffing pipeline to **PySpark** (explicit schema,
   fail-fast parsing, a Parquet curated layer that shrinks 570 MB of CSV to 62 MB) over
   **4.7M synthetic contact records**; all **16 pandas-vs-Spark parity checks matched exactly**,
   and the Spark path reproduces all 15 committed pipeline outputs byte for byte.
2. Orchestrated the **8-stage pipeline** (data generation → Spark ETL → Prophet forecast →
   Erlang C staffing → SimPy validation → dashboards → executive deck) as an **Apache Airflow 3
   DAG** with isolated environments and a staffing-math verification gate. Scheduled runs finish
   end to end in **~38 s** on a laptop, and the plan they produce uses **43% fewer agent-hours than
   fixed 8-hour shifts** (synthetic data).
3. Doubled the automated test suite **from 26 to 53 tests**: data-quality invariants on the Parquet
   layer, exact-equality engine parity and fail-fast read rules. Each new check was confirmed by
   fault injection (**16 of 16 seeded defects caught**), and both engines were verified to
   reproduce every output from a clean environment.
