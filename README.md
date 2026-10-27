# Delta Reservations — Contact Center Staffing & Service-Level Optimizer

Forecast contact volume for Delta's Atlanta (ATL) Reservations contact center, size staffing for
every 30-minute interval with verified **Erlang C** queueing math, confirm that math with a
discrete-event **simulation**, and turn the result into a costed recommendation — delivered as a
Tableau dashboard and an executive deck.

> **Headline:** staffing each half-hour to the forecast meets the **80/20** service target
> (80% of calls answered within 20 s) in every interval while using **43% fewer agent-hours than
> fixed 8-hour shifts — $845K over a 30-day horizon** at an assumed $25/hour.

> [!IMPORTANT]
> **All data is synthetic.** Real Delta contact-center logs are confidential and don't exist
> publicly, so the ~4.7M contacts are generated from documented, seeded assumptions. Dollar figures
> are modeled results that exercise a real method — not Delta's actual costs. A student project,
> not affiliated with or endorsed by Delta.

**See it without running anything**
- **Interactive dashboard (Tableau Public):** <https://public.tableau.com/app/profile/frank.an5089/viz/DeltaATLReservationsStaffingOptimizer/Dashboard>
- **Executive deck:** [`outputs/Delta_Reservations_Capstone.pptx`](outputs/Delta_Reservations_Capstone.pptx) — 8 slides with speaker notes, recommendation first
- **Offline dashboard (Plotly):** [`outputs/dashboard/dashboard.html`](outputs/dashboard/dashboard.html)

## Results

| Question | Answer |
|---|---|
| How accurate is the forecast? | **9.7% MAPE** on held-out normal days (December); 16.2% including the Christmas spike |
| Is the staffing math right? | Simulation matches Erlang C within **0.38 pts** of service level across 4 load levels (tolerance 3) |
| What does it save? | **43%** fewer agent-hours than fixed 8h shifts (**$845,238** per 30 days); 69% vs naive peak-24/7 |
| What service does it deliver? | **87.8%** of calls answered ≤ 20 s (≥ 80% in every half-hour); 7.9 s average wait; 87% occupancy |
| What about disruption (IROP) days? | Peak scheduled agents go **150 → 461** (~3×); the day needs 3.0× the agent-hours |
| And self-service? | Moving 20% of app-friendly contacts to the Fly Delta app: −11% volume, a further **$116K** |

| Scenario (30-day horizon) | Agent-hours | Cost @ $25/h | Service level |
|---|---:|---:|---:|
| Naive flat — peak headcount (196), 24/7 | 141,120 | $3,528,000 | 99.9% |
| Realistic — 8h shifts, each staffed to its busiest half-hour | 78,200 | $1,955,000 | 98.2% |
| **Optimized — Erlang C, interval by interval** | **44,390.5** | **$1,109,762** | **87.8%** |
| Optimized + app deflection | 39,732.5 | $993,312 | 86.5% |

The baselines' higher service level is the waste: they buy far more service than the 80/20 target
asks for, most of it in quiet hours.

![Cost vs baselines](outputs/figures/savings.png)
![Staffing curve: optimized vs flat vs an IROP day](outputs/figures/staffing_curve.png)

## How it works

```mermaid
flowchart LR
    A["1 · Synthetic data<br/>4.7M contacts"] --> B["2 · Forecast<br/>Prophet + intraday profile"]
    B --> C["3 · Erlang C<br/>agents per 30 min"]
    D["4 · Simulation<br/>SimPy M/M/c"] -. validates .-> C
    C --> E["5 · Optimize & price<br/>baselines · IROP · deflection"]
    E --> F["6 · Dashboards<br/>Tableau + Plotly"]
    E --> G["7 · Executive deck<br/>python-pptx"]
```

| Phase | What it does | Code | Key outputs |
|---|---|---|---|
| 1 | Generates one year of contacts: Poisson arrivals, lognormal handle times, weekly/seasonal/holiday demand, 12–18 IROP disruptions | [`src/generate_data.py`](src/generate_data.py) | `data/raw/contacts.csv` (git-ignored) |
| 2 | Forecasts daily volume with Prophet (SARIMA fallback) and splits days into 48 intervals | [`src/forecast.py`](src/forecast.py) | `forecast.csv`, `intraday_profile.csv`, `forecast.png` |
| 3 | Erlang C engine: service level, wait time, occupancy, minimum agents, shrinkage | [`src/erlang.py`](src/erlang.py) | library (verified table in `python -m src.erlang`) |
| 4 | Simulates the queue call by call and compares measured vs predicted service level | [`src/simulate.py`](src/simulate.py) | `validation.csv`, `validation.png` |
| 5 | Builds the interval staffing plan and prices it against two baselines, plus IROP and deflection scenarios | [`src/optimize.py`](src/optimize.py) | `staffing_plan.csv`, `savings.png`, `staffing_curve.png` |
| 6 | Exports BI-ready CSVs and a self-contained Plotly dashboard | [`src/dashboard.py`](src/dashboard.py) | `outputs/dashboard/` ([Tableau guide](outputs/dashboard/TABLEAU_GUIDE.md)) |
| 7 | Generates the executive deck; every number is read from the pipeline's outputs | [`src/build_deck.py`](src/build_deck.py) | `outputs/Delta_Reservations_Capstone.pptx` |

The project was built phase by phase from [`FLAGSHIP_SPEC.md`](FLAGSHIP_SPEC.md).

## Spark + Airflow

The same pipeline also runs with its raw-data stages on **PySpark** and every stage **orchestrated
by Airflow**. This is additive: `python run_all.py` is unchanged and remains the reference.

```mermaid
flowchart LR
    subgraph AF["Airflow DAG · staffing_pipeline — one task per stage, each run in the project venv"]
        direction LR
        GEN["generate_data<br/>4.7M contacts → CSV"] --> ETL
        subgraph ETL["spark_etl · PySpark local[*]"]
            direction TB
            P1["CSV → schema-checked<br/>Parquet (62 MB)"] --> P2["one scan → cube →<br/>5 small aggregate files"]
        end
        ETL --> VI{{"validate_interim<br/>GX gate"}} --> FC["forecast<br/>Prophet"] --> OPT["optimize<br/>Erlang C"] --> DASH["dashboard<br/>CSVs + Plotly"] --> VO{{"validate_outputs<br/>GX gate"}} --> DECK["build_deck<br/>python-pptx"]
        ERL["erlang_check<br/>verified table"] --> OPT
        SIM["simulate<br/>SimPy"] --> VO
    end
```

- **Spark ETL** ([`src/spark_etl.py`](src/spark_etl.py)) reads `contacts.csv` once with an explicit
  schema (a bad header or malformed value fails the read) and writes 62 MB of Parquet. From that
  it computes every aggregate the forecast, optimize and dashboard stages otherwise re-read the
  570 MB CSV for. Spark does only exact integer counts and sums, so its results match pandas bit
  for bit: `python run_all.py --engine spark` reproduces every committed CSV, JSON and PNG byte
  for byte.
- **Airflow DAG** ([`dags/staffing_pipeline.py`](dags/staffing_pipeline.py)) gives each stage its
  own task with explicit dependencies. Two Great Expectations gates stop the run on bad data
  (below), `erlang_check` gates `optimize`, and `simulate` runs in parallel from the start. Airflow
  lives in its own virtualenv, the gates in another, and every pipeline task runs in the
  project's, so the pinned stack above is never touched.

```bash
python run_all.py --engine spark        # needs a Java runtime (tested on JDK 25)
python -m src.spark_etl --parity-full   # full-data pandas-vs-Spark check + wall-clock times
make airflow-install                    # once: .venv-airflow with pinned Airflow 3.3.2
make airflow-up                         # UI at http://localhost:8080 (login in airflow_home/)
make airflow-trigger                    # in a second terminal: run the DAG
```

At this data size Spark isn't faster: one laptop, 570 MB, and it's roughly break-even with pandas
once JVM start-up counts. The point is the shape: one parse of the raw data, a columnar curated
layer, and stages that scale out. Timings, parity results, the DAG's measured runs and design
decisions are in [`NOTES.md`](NOTES.md).

## Data quality and lineage

Every dataset the pipeline reads or writes is described once, in
[`metadata/catalog.yaml`](metadata/catalog.yaml): path, layer, producing and consuming task, grain,
primary key, a row-count rule, and every column's type, unit, allowed values or range. Three things
are built from it:

- **[`DATA_DICTIONARY.md`](DATA_DICTIONARY.md)**: one table per dataset (24) plus the full
  dataset-level lineage graph, generated by `python -m src.catalog`. A test fails if it's stale, or
  if a real file's columns drift from the catalog.
- **Validation gates** ([`src/quality.py`](src/quality.py), Great Expectations): 348 expectations,
  with every row count, column and range taken from the catalog. The pytest suites check the code
  at development time; these check the data at pipeline time and stop the run when it's wrong.

  | Layer | What it checks | Gate in the DAG |
  |---|---|---|
  | raw / interim | `contacts.csv`: the 11 data-quality invariants as expectations. Spark engine: the Parquet keeps every row, the aggregates reconcile with it, and every artifact is fresher than its input | `validate_interim`, before `forecast` |
  | processed | the forecast covers exactly the expected dates with no gaps; intraday fractions sum to 1 within 1e-9; 1,440 staffing slots with agents ≥ 1 and service level ≥ 0.80 in every slot; simulation within 3 pts of Erlang C | `validate_outputs`, before `build_deck` |
  | outputs | the summary's agent-hours and savings equal what `staffing_plan.csv` implies; the savings CSV and the dashboard's KPI tiles match the summary and the plan | `validate_outputs` |

- **Airflow asset lineage**: each task's inlets and outlets are Airflow Assets built from the
  catalog, so the Airflow UI draws the same dataset → task → dataset graph and records an update
  event for every dataset a run produces.

The lineage at a glance (generated from the catalog; red boxes are checks that gate the next task):

<!-- lineage:start (generated by `python -m src.catalog`) -->
```mermaid
flowchart LR
    classDef raw fill:#f6e3c5,stroke:#b07d2b,color:#1b2a4a
    classDef interim fill:#e4dcf2,stroke:#6c4fa3,color:#1b2a4a
    classDef processed fill:#d6e6f5,stroke:#2f6aa3,color:#1b2a4a
    classDef output fill:#d9efe0,stroke:#2e7d4f,color:#1b2a4a
    classDef task fill:#1b2a4a,stroke:#1b2a4a,color:#ffffff
    classDef gate fill:#c8102e,stroke:#c8102e,color:#ffffff
    t_generate_data["generate_data"]:::task
    t_spark_etl["spark_etl"]:::task
    t_validate_interim["validate_interim"]:::gate
    t_forecast["forecast"]:::task
    t_erlang_check["erlang_check"]:::gate
    t_simulate["simulate"]:::task
    t_optimize["optimize"]:::task
    t_dashboard["dashboard"]:::task
    t_validate_outputs["validate_outputs"]:::gate
    t_build_deck["build_deck"]:::task
    g0[("contacts.csv")]:::raw
    t_generate_data --> g0
    g0 --> t_spark_etl
    g0 --> t_validate_interim
    g1[("contacts.parquet<br/>daily_series.csv<br/>+ 4 more")]:::interim
    t_spark_etl --> g1
    g1 --> t_validate_interim
    g1 --> t_forecast
    g1 --> t_dashboard
    g1 --> t_optimize
    g2[("forecast.csv<br/>intraday_profile.csv<br/>forecast_metrics.json")]:::processed
    t_forecast --> g2
    g2 --> t_optimize
    g2 --> t_dashboard
    g2 --> t_build_deck
    g2 --> t_validate_outputs
    g3[("staffing_plan.csv<br/>optimize_summary.json")]:::processed
    t_optimize --> g3
    g3 --> t_dashboard
    g3 --> t_build_deck
    g3 --> t_validate_outputs
    g4[("validation.csv")]:::processed
    t_simulate --> g4
    g4 --> t_build_deck
    g4 --> t_validate_outputs
    g5[("daily_volume.csv<br/>interval_staffing.csv<br/>+ 3 more")]:::output
    t_dashboard --> g5
    g5 --> t_build_deck
    g5 --> t_validate_outputs
    g6[("volume_overview.png")]:::output
    t_generate_data --> g6
    g6 --> t_build_deck
    g7[("forecast.png")]:::output
    t_forecast --> g7
    g7 --> t_build_deck
    g8[("validation.png")]:::output
    t_simulate --> g8
    g8 --> t_build_deck
    g9[("staffing_curve.png<br/>savings.png")]:::output
    t_optimize --> g9
    g9 --> t_build_deck
    g10[("Delta_Reservations_Capstone.pptx")]:::output
    t_build_deck --> g10
    t_validate_interim -. gates .-> t_forecast
    t_erlang_check -. gates .-> t_optimize
    t_validate_outputs -. gates .-> t_build_deck
```
<!-- lineage:end -->

```bash
make gx-install                         # once: .venv-gx with the pinned validator stack
python run_all.py --validate            # run the pipeline, then the gates (exit 1 + the failing check)
make validate                           # re-validate the last run's files (all four suites)
python -m src.quality_trend             # the last 10 validation runs
open outputs/quality/data_docs/index.html   # Great Expectations Data Docs (generated, git-ignored)
```

The latest results are summarised deterministically in
[`outputs/quality/latest.md`](outputs/quality/latest.md) and
[`outputs/quality/latest_interim.md`](outputs/quality/latest_interim.md). Per-run history, the
trend and Data Docs carry timestamps, so they're generated on demand and git-ignored.

## Reproduce

Requires Python 3.11 (Prophet and SimPy wheels).

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python run_all.py     # regenerates data, forecast, validation, staffing plan, figures, dashboards, deck
pytest                # 103 tests (Spark / Great Expectations / Airflow tests skip, with the reason,
                      #   if Java, .venv-gx or .venv-airflow is missing)
```

`run_all.py` takes under a minute. Every step is seeded and `requirements.txt` pins the exact
library versions, so a fresh clone reproduces every committed CSV, JSON and PNG byte for byte.
The HTML dashboard and the deck embed a per-run chart id and timestamp, so for those two only the
content repeats.
Each phase also runs on its own: `python -m src.generate_data`, `src.forecast`, `src.erlang`,
`src.simulate`, `src.optimize`, `src.dashboard`, `src.build_deck` (and `src.spark_etl` for the
Spark path). The ~570 MB raw dataset is
git-ignored and regenerated by Phase 1; the small processed files, figures, and deck are committed
so the repo reads without running anything. The Tableau dashboard is authored by hand from the
Phase-6 CSVs.

**Testing — tested where the answer is deterministic, validated where it's random:**
- `tests/test_erlang.py` (10) — exact-value checks against the spec's verified Erlang C table.
- `tests/test_data_quality.py` (11) — invariants that hold for any seed: no nulls, valid categories,
  handle times in range, ≥ 10 distinct IROP events, timestamps inside their interval.
- `tests/test_validation.py` (5) — the simulation agrees with Erlang C within 3 points at 4 loads.
- `tests/test_spark_parity.py` (9) — Spark's aggregates match the pandas path's (tolerance 1e-6;
  identical in practice), and each stage's `--engine spark` inputs equal its pandas inputs exactly.
- `tests/test_spark_etl.py` (18) — the cleaned Parquet: schema, every CSV row kept, the same
  data-quality invariants as the raw CSV, and the read rules (bad header or malformed value fails).
- `tests/test_catalog.py` (36) — the catalog is valid and matches the code; every column of every
  real file is in it with a matching type (schema drift); lineage is acyclic; the catalog's tasks are
  the Airflow DAG's tasks and the DAG runs every producer before its consumers; the data dictionary
  and the README lineage graph are current.
- `tests/test_quality.py` (11) — the Great Expectations gates pass on a clean run and the committed
  reports are current; 18 injected defects across the layers are each caught by exactly the intended
  expectation; warnings don't gate; a missing input fails; every run lands in the trend.
- `tests/test_quality_trend.py` (3) — the validation trend is appended and printed correctly.

## Assumptions

Every business assumption lives in [`config.py`](config.py) — change one number and re-run.

| Assumption | Value | `config.py` |
|---|---|---|
| Service target | 80% of calls answered within 20 s | `TARGET_SL`, `TARGET_ANSWER_SECONDS` |
| Max occupancy | 90% of agent time busy | `MAX_OCCUPANCY` |
| Shrinkage | 30% of paid time unavailable (breaks, training, meetings) | `SHRINKAGE` |
| Loaded agent cost | $25 per hour | `AGENT_HOURLY_COST` |
| Base demand | 12,000 contacts on a normal day | `BASE_DAILY_CONTACTS` |
| Planning interval / history | 30 minutes / 365 days from 2025-01-01 | `INTERVAL_MINUTES`, `SIM_START`, `SIM_DAYS` |
| Realistic baseline | 8-hour shift blocks, each staffed to its busiest interval | `REALISTIC_BASELINE_BLOCK_HOURS` |
| IROP stress day | 2.5× volume with IROP handle times | `IROP_VOLUME_MULTIPLIER` |
| Self-service deflection | 20% of Booking_Change, Seat_Upgrade, SkyMiles contacts | `DEFLECTION_RATE`, `DEFLECTABLE_REASONS` |
| Random seed | 42 | `SEED` |

### How the synthetic demand is built
Daily volume is `BASE_DAILY_CONTACTS` × layered multipliers (see [`src/generate_data.py`](src/generate_data.py)),
spread over a twin-peak intraday curve, with Poisson counts per interval:

- **Day-of-week** — Monday-high, weekend-low.
- **Seasonal curve (smooth)** — broad summer plateau + year-end shape, deepest trough late
  Jan/early Feb; mean-normalized so it never moves the annual total. Calibrated to the *shape* of
  public TSA checkpoint data.
- **Holiday-travel spikes (discrete)** — calendar-fixed multipliers on the heavy travel days around
  Thanksgiving and Christmas, with the holidays themselves quiet. Volume only.
- **Pre-holiday booking surge** — TSA data counts *passengers*, who peak on the travel day; this
  project models *contacts*, which peak 2–3 weeks earlier, when trips are booked and changed.
- **IROP events** — random weather disruptions (1.8–3.0× volume): the tallest spikes, and the only
  layer that shifts the reason mix toward longer rebooking calls.

Contact reasons and SkyMiles tiers follow fixed mixes; handle times are lognormal per reason with a
small premium for higher tiers.

## Call-center concepts, in plain English

- **Offered load (Erlangs)** — work arriving per interval: 50 calls × 180 s in a 30-minute window
  = 5 Erlangs, i.e. five agents' worth of continuous work.
- **Service level (80/20)** — share of calls answered within a target time; 80% within 20 s is the
  industry default.
- **Average speed of answer (ASA)** — average wait before a call is answered.
- **Occupancy** — share of agent time spent on calls. Above ~90% burns agents out; low means idle pay.
- **Erlang C** — the queueing formula that turns load + agents into service level and wait time;
  run in reverse, it finds the fewest agents that hit the target. For 5 Erlangs at 180 s AHT: 7
  agents give 74% within 20 s, 8 give 88% — so 8.
- **Shrinkage** — time agents are paid but not taking calls. Needing 8 on the phones at 30%
  shrinkage means scheduling 12.
- **IROP (irregular operations)** — cancellations and major delays, usually weather; the reason
  contact volume spikes and the hardest days to staff.
- **Agent-hours** — the common currency: scheduled agents × hours, and × hourly cost for dollars.

## What I learned

- **The baseline decides the headline.** The same plan saves 69% against naive peak-24/7 staffing
  but 43% against fixed 8-hour shifts. I lead with 43%, the comparison that survives questions.
- **Test where it's deterministic, validate where it's random.** Erlang C gets exact-value tests; the
  generated data gets invariant tests; the simulation gets an agreement test. A single 60k-call run
  missed the 3-point tolerance on ~18% of seeds at high occupancy (queue waits are autocorrelated),
  so the test averages 8 replications instead of loosening the tolerance.
- **A forecast should only claim what it can know.** Blanking IROP days out of training made the
  95% band ~4× narrower (it had dipped below zero) for a small cost in accuracy (8.7% → 9.7% MAPE);
  disruptions are handled as a separate stress scenario instead.
- **Passengers aren't contacts.** Travel volume peaks on the holiday; calls peak weeks before it.
- **One year of history can't teach yearly seasonality.** Prophet disables its yearly term with
  under two years of data, so the January forecast carries December's level forward (see below).

## Limitations and future work

- **Synthetic data** proves the pipeline is internally consistent and the method works end to end;
  it is not a claim about Delta's real volumes or costs.
- **The January forecast runs ~18% above last January** (11,785 vs 9,974 contacts/day), so absolute
  dollars are likely overstated; percentage savings are more robust because every scenario uses the
  same forecast.
- **Erlang C assumes no abandonment and exponential handle times.** The simulation validates the
  formula on those terms; its optional `patience_seconds` argument turns it into an Erlang A model.
- **`apply_shrinkage` float rounding** — the spec's verified code, kept verbatim, occasionally rounds
  up one extra agent (e.g. 322 required → 461 scheduled, not 460): 24 of 1,440 intervals,
  +12 agent-hours (~$300, 0.03%).
- **Out of scope:** turning interval requirements into shift rosters, multi-skill routing,
  chat/app channels, and intraday re-forecasting. Deliberately deferred: a fully passenger-shaped
  seasonal curve and more holiday notches (July 4, Memorial Day, Labor Day, New Year) — the
  forecaster learns whatever seasonality is present, so more shape-matching has diminishing returns.

## Project structure

```
delta-staffing-optimizer/
├── run_all.py               # whole pipeline, one command (--engine pandas|spark)
├── config.py                # every tunable assumption
├── metadata/catalog.yaml    # every dataset: path, layer, lineage, grain, key, row rule, columns
├── DATA_DICTIONARY.md       # generated from the catalog (tables + lineage graph)
├── src/
│   ├── catalog.py           #    validates the catalog, renders the dictionary + lineage
│   ├── quality.py           #    Great Expectations gates (raw, interim, processed, outputs)
│   ├── quality_trend.py     #    per-run validation trend
│   ├── generate_data.py     # 1  synthetic contacts
│   ├── spark_etl.py         #    Spark path: CSV → Parquet → aggregates (+ pandas parity check)
│   ├── forecast.py          # 2  Prophet (+ SARIMA fallback), intraday profile
│   ├── erlang.py            # 3  verified Erlang C engine
│   ├── simulate.py          # 4  SimPy validation
│   ├── optimize.py          # 5  staffing plan, savings, IROP + deflection scenarios
│   ├── dashboard.py         # 6  BI-ready CSVs + Plotly dashboard
│   └── build_deck.py        # 7  executive deck
├── dags/
│   └── staffing_pipeline.py # Airflow DAG: one task per stage, two quality gates, asset lineage
├── Makefile                 # airflow-* targets, gx-install, validate
├── requirements-airflow.txt # Airflow pins (separate .venv-airflow)
├── requirements-gx.txt      # Great Expectations pins (separate .venv-gx)
├── tests/                   # math, data quality, simulation, Spark ETL + parity, catalog, gates
├── data/
│   ├── raw/                 # contacts.csv (git-ignored, regenerated)
│   ├── interim/             # Spark Parquet + aggregates (git-ignored, regenerated)
│   └── processed/           # forecast, staffing plan, metric summaries
├── outputs/
│   ├── figures/             # PNG charts
│   ├── dashboard/           # CSVs, dashboard.html, TABLEAU_GUIDE.md
│   ├── quality/             # latest.md + latest_interim.md (history, trend, Data Docs git-ignored)
│   └── Delta_Reservations_Capstone.pptx
├── NOTES.md                 # engineering notes: parity, timings, gates, decisions
└── FLAGSHIP_SPEC.md         # the phase-by-phase build spec
```
