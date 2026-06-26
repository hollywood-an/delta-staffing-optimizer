# FLAGSHIP_SPEC.md
## Delta Reservations — Contact Center Staffing & Service-Level Optimizer

> **What this document is.** This is a build specification (a "spec"): the complete
> blueprint for a project, written *before* any code exists. You will hand it to
> Claude Code, which will build the project phase by phase. You (the human) review
> each phase before moving to the next.
>
> **Who it's written for.** It assumes you know basic Python but are NEW to
> call-center / contact-center analytics. Every domain term is defined the first
> time it appears. Read the Glossary (Section 2) first.
>
> **Why this project.** It mirrors the actual Delta Reservations Intern capstone:
> *forecast demand → model staffing → simulate to validate → recommend → present
> to division leaders.* It exercises nearly every "preferred qualification" on the
> posting: SQL-able data, statistical modeling, simulation, data visualization
> (Tableau/Power BI), business modeling, and an executive presentation.

---

## SECTION 0 — HOW TO USE THIS SPEC WITH CLAUDE CODE

1. Create an empty folder, e.g. `delta-staffing-optimizer/`.
2. Put this file inside it as `FLAGSHIP_SPEC.md`.
3. Open Claude Code in that folder.
4. Build **one phase at a time**. Use prompts like:
   - *"Read FLAGSHIP_SPEC.md. Then implement Phase 1 only. Stop when the Phase 1
     'Definition of Done' is met and show me how to verify it."*
   - After you verify it works: *"Now implement Phase 2 only."*
5. **Do not** let it build all phases at once. Reviewing each phase is how you
   learn the material and catch drift. This is also how you'll be able to explain
   every piece in a Delta interview.
6. After each phase, ask Claude Code: *"Explain in plain English what this phase
   does and why, as if to a non-technical interviewer."* Keep those explanations —
   they become your talking points.

**Golden rule for the agent:** *Never invent the Erlang C math.* Use the verified
reference implementation provided in Phase 3 of this spec exactly as written.

---

## SECTION 0.5 — VERSION CONTROL: CREATE THE GITHUB REPO *FIRST*

> Do this **before** writing any code. The phased build gives you natural save
> points, and the public GitHub repo *is* the portfolio artifact recruiters open.
> A clean commit history with clear messages is itself a hiring signal for the
> engineering-heavy roles you're targeting.

### Step 1 — Create the empty repo on GitHub (in the browser)
1. Go to **github.com → New repository**.
2. **Repository name:** `delta-staffing-optimizer`
3. **Description:** *"Forecasting + Erlang C staffing optimization for an airline
   reservations contact center (synthetic data)."*
4. **Visibility:** **Public** (so reviewers can see it).
5. **Do NOT** check "Add a README," ".gitignore," or "license." Create it empty —
   we'll make those files locally. (An empty remote avoids the merge conflict that
   trips up beginners.)
6. Copy the repo URL. It will look like:
   `https://github.com/hollywood-an/delta-staffing-optimizer.git`

### Step 2 — Turn your local folder into a git repo and connect it
Run these inside your project folder (the one holding `FLAGSHIP_SPEC.md`):

```bash
cd delta-staffing-optimizer
git init
git branch -M main
git remote add origin https://github.com/hollywood-an/delta-staffing-optimizer.git
```

> **Auth note:** GitHub no longer accepts your account password on the command line.
> If `git push` asks for credentials, either (a) install the GitHub CLI and run
> `gh auth login`, (b) create a **Personal Access Token** and paste it as the
> password, or (c) set up an SSH key and use the SSH URL
> (`git@github.com:hollywood-an/delta-staffing-optimizer.git`). The `gh` CLI is the
> easiest for a beginner.

### Step 3 — Create `.gitignore` BEFORE the first commit (important)
The Phase 1 dataset is ~4–5 million rows (hundreds of MB) and is **regenerable**,
so it must never be committed — GitHub rejects files over 100 MB anyway, and a bloated
repo is a bad look. Create a file named exactly `.gitignore` with this content:

```gitignore
# Python
__pycache__/
*.pyc
.venv/
venv/
env/

# OS
.DS_Store

# Large / regenerable data — never commit the raw dataset
data/raw/*.csv

# Jupyter
.ipynb_checkpoints/

# Keep the empty raw/ folder tracked
!data/raw/.gitkeep
```

Then create the placeholder so the folder survives a fresh clone:
```bash
mkdir -p data/raw && touch data/raw/.gitkeep
```

### Step 4 — First commit (the scaffold + spec)
```bash
git add .
git commit -m "chore: project scaffold and build spec"
git push -u origin main
```
Refresh the GitHub page — you should see your files. The `-u` flag means future
pushes are just `git push`.

### Step 5 — Commit at every "Definition of Done" (one commit per phase)
After you verify a phase passes its DoD, commit and push it. This gives you a clean,
readable history that walks a reviewer through the project's construction. Use these
messages (a professional "conventional commits" style — `feat:` = feature,
`docs:` = documentation, `chore:` = setup/maintenance):

| After phase | Command |
|---|---|
| Phase 1 | `git add -A && git commit -m "feat: synthetic ATL contact data generator" && git push` |
| Phase 2 | `git add -A && git commit -m "feat: contact volume forecasting (Prophet + SARIMA fallback)" && git push` |
| Phase 3 | `git add -A && git commit -m "feat: verified Erlang C staffing engine with tests" && git push` |
| Phase 4 | `git add -A && git commit -m "feat: SimPy discrete-event validation of Erlang C" && git push` |
| Phase 5 | `git add -A && git commit -m "feat: staffing optimization and cost-savings model" && git push` |
| Phase 6 | `git add -A && git commit -m "feat: dashboard exports (Tableau CSVs + Plotly fallback)" && git push` |
| Phase 7 | `git add -A && git commit -m "feat: executive capstone deck generator" && git push` |
| Phase 8 | `git add -A && git commit -m "docs: README, run_all, and reproducibility polish" && git push` |

> You can either run these commits yourself, or tell Claude Code at each DoD:
> *"Phase N passes its Definition of Done — commit it with the message from Section
> 0.5 and push."* Pick one approach and stay consistent.

### Step 6 (optional level-up) — branch per phase + Pull Requests
If you want the repo to demonstrate a real team workflow (a nice signal for FAANG/
fintech reviewers), do each phase on its own branch and merge via a Pull Request:

```bash
git checkout -b phase-1-data        # start the phase on a branch
# ...build Phase 1, commit as above...
git push -u origin phase-1-data
# On GitHub: open a Pull Request from phase-1-data into main, then "Merge".
git checkout main && git pull       # bring the merge back down locally
```
This is optional — committing straight to `main` is perfectly fine for a solo project.
Only adopt the PR flow if you have the bandwidth; a messy half-done version is worse
than a clean simple one.

### What to commit vs. what NOT to commit
- **Commit:** all code, `config.py`, tests, `README.md`, `requirements.txt`, the
  *small pre-aggregated* dashboard CSVs, the figure PNGs (so the README renders),
  and the final `.pptx` deck (so reviewers can open it without running anything).
  `forecast.csv`, `intraday_profile.csv`, and `staffing_plan.csv` are small — commit
  them too.
- **Never commit:** `data/raw/contacts.csv` (huge, regenerable), `venv/`,
  `__pycache__/`, `.DS_Store`. The `.gitignore` above handles all of these.
- Because `contacts.csv` is git-ignored, a fresh clone won't have it — that's fine:
  `python run_all.py` (Phase 8) regenerates everything from scratch.

---

## SECTION 1 — PROJECT OVERVIEW

### 1.1 The one-sentence pitch
A data product that forecasts incoming contact volume for Delta's Atlanta (ATL)
Reservations contact center, calculates the minimum number of agents needed each
half-hour to hit a target service level, validates that calculation with a
simulation, and packages the result as an executive recommendation that quantifies
the cost savings versus naive flat staffing.

### 1.2 The business story (use this framing in your deck)
ATL is Delta's largest hub and the world's busiest airport. When operations are
smooth, contact volume follows predictable daily and seasonal rhythms. When
operations break — weather, IROPs (Irregular Operations: cancellations and major
delays) — contact volume *spikes*, customers wait, and the experience suffers.
Delta competes on operational reliability and premium customer care, so protecting
the contact experience during disruption is a brand-level priority, not a back-office
detail.

**Two ways a contact center loses money / goodwill:**
- **Understaffing** → long waits, abandoned calls, angry customers, brand damage.
- **Overstaffing** → paying agents to sit idle.

This project finds the staffing curve that threads that needle, and proves it.

### 1.3 What "good" looks like (final deliverables)
1. A realistic synthetic dataset of contact records (we generate it; no real Delta
   data exists for us, and using fake data is the correct, privacy-safe choice).
2. A volume **forecast** model with reported accuracy.
3. A verified **Erlang C** staffing engine (the math real contact centers use).
4. A **simulation** that independently confirms the Erlang C numbers.
5. An **optimization** that outputs the staffing requirement curve + cost savings.
6. A **dashboard** (Tableau Public, with a Plotly fallback).
7. An **executive PowerPoint** — the capstone deliverable.
8. A clean **README** so a reviewer who can't run code still understands it.

---

## SECTION 2 — GLOSSARY (READ THIS FIRST)

| Term | Plain-English meaning |
|---|---|
| **Contact** | One customer interaction (here: a phone call). |
| **Contact volume** | How many contacts arrive in a time window. |
| **Interval** | The time bucket we plan in. We use **30 minutes**. |
| **AHT** (Average Handle Time) | Average seconds an agent spends on one contact (talk + after-call work). |
| **Arrival rate (λ, "lambda")** | Contacts arriving per unit time. |
| **Offered load (A), in "Erlangs"** | Total work arriving = arrival rate × AHT. If 50 calls arrive in 30 min and each takes 180s, the load = 50 × 180 / 1800 = **5 Erlangs**. Think "5 agents' worth of continuous work." |
| **Agents** | Staff handling contacts (a.k.a. "servers" in queueing theory). |
| **Service Level (SL)** | % of contacts answered within a target time. "**80/20**" = answer 80% of calls within 20 seconds. The industry default. |
| **ASA** (Average Speed of Answer) | Average wait before a contact is answered. |
| **Occupancy** | % of time agents are actually busy (load ÷ agents). Too high (>90%) burns agents out; too low wastes money. |
| **Abandonment** | Customer hangs up before being answered. |
| **Shrinkage** | The gap between "scheduled" and "actually available to take contacts" (breaks, training, meetings). Typically ~30%. You must staff *extra* to cover it. |
| **IROP** (Irregular Operations) | Cancellations / major delays (usually weather). Causes contact spikes. |
| **Erlang C** | The standard queueing formula that predicts wait/service level given load and agent count. The heart of this project. |
| **Discrete-event simulation** | A program that imitates calls arriving and agents answering one event at a time, to check our math against "reality." |
| **MAPE / RMSE** | Forecast error metrics (lower = more accurate). |

---

## SECTION 3 — TECH STACK (the agent must use exactly these)

- **Language:** Python 3.11+
- **Core libraries:**
  - `pandas`, `numpy` — data handling
  - `faker` — synthetic identifiers/fields
  - `prophet` — forecasting (primary). Fallback: `statsmodels` SARIMA.
  - `simpy` — discrete-event simulation
  - `matplotlib` — charts inside Python / for the deck
  - `python-pptx` — generate the PowerPoint programmatically
  - `pytest` — tests for the Erlang module
- **Dashboard:** Tableau Public (primary). Fallback if Tableau isn't available:
  a `plotly` + `dash` (or static Plotly HTML) dashboard.
- **Environment:** a `requirements.txt` and a virtual environment (`venv`).
- **No database required**, but design the data so it *could* drop into PostgreSQL
  (flat, normalized, clean column types) — that lets you also write the SQL
  "quick-win" project later against the same CSVs.

---

## SECTION 3.5 — TESTING STRATEGY (read before building)

> **The principle:** test automatically *where the answer is deterministic*; verify
> manually *where the output is legitimately random*. Don't write brittle unit tests
> that assert exact values on Poisson-random data — they'll fail for no real reason.
> But *do* write real, runnable assertions wherever a correct answer exists. The
> resulting story — *"tested where determinism allows, validated where it doesn't"* —
> is more credible than either "I tested everything" or "I only tested one file."

This project has **three tiers** of checking:

1. **Automated unit tests (`pytest`) — the math.**
   - **Phase 3 (Erlang C)** is pure deterministic math with known-correct answers, so
     it gets a full `tests/test_erlang.py` suite. This is the core correctness gate.
   - **Phase 1 (data generation)** gets a small **data-quality** test suite
     (`tests/test_data_quality.py`) — not exact-value tests, but invariants that must
     hold no matter what random seed runs (no nulls, percentages sum to ~100%, handle
     times positive, IROP events present). This is exactly what a real analytics team
     runs on a pipeline.
   - **Phase 4 (simulation)** gets a real assertion that the **simulation agrees with
     Erlang C** within tolerance (`tests/test_validation.py`). This turns your strongest
     interview claim — "I validated the method two independent ways" — into something a
     reviewer can run and watch pass.

2. **Manual verification gates — everything else.** Phases 2, 5, 6, 7, 8 are checked
   against their **Definition of Done** (eyeball the plot, confirm the file exists, read
   the printed MAPE). Forecasts and dashboards don't have a single "correct" number, so
   a human confirms they look right rather than a test asserting an exact value.

3. **End-to-end smoke check — Phase 8.** `run_all.py` must complete from a clean clone
   with no errors. "It runs start to finish" is its own kind of test.

**What `pytest` covers when you run it:** `tests/test_erlang.py` (math correctness),
`tests/test_data_quality.py` (data invariants), `tests/test_validation.py` (sim vs
analytic agreement). Aim for these three files; everything else is a DoD gate.

---

## SECTION 4 — PROJECT STRUCTURE (the agent must create this)

```
delta-staffing-optimizer/
├── FLAGSHIP_SPEC.md           # this file
├── README.md                  # written in Phase 8
├── requirements.txt
├── .gitignore                 # created in Section 0.5 (BEFORE the first commit)
├── config.py                  # all tunable parameters live here
├── data/
│   ├── raw/                   # generated contact records (git-ignored — see 0.5)
│   │   └── .gitkeep           # keeps the empty folder in git after clone
│   └── processed/             # aggregated time series, staffing tables
├── src/
│   ├── __init__.py
│   ├── generate_data.py       # Phase 1
│   ├── forecast.py            # Phase 2
│   ├── erlang.py              # Phase 3  (the verified math)
│   ├── simulate.py            # Phase 4
│   ├── optimize.py            # Phase 5
│   └── build_deck.py          # Phase 7
├── tests/
│   ├── test_erlang.py         # Phase 3 tests (math correctness)
│   ├── test_data_quality.py   # Phase 1 tests (data invariants)
│   └── test_validation.py     # Phase 4 tests (sim vs Erlang C agreement)
├── notebooks/                 # optional exploration
├── outputs/
│   ├── figures/               # PNG charts
│   ├── dashboard/             # CSVs for Tableau + fallback dashboard
│   └── Delta_Reservations_Capstone.pptx
└── run_all.py                 # runs phases 1→5 end to end
```

**`config.py` must centralize every assumption**, so a reviewer can change one
number and re-run. At minimum:

```python
# config.py — all tunable assumptions in ONE place
SEED = 42                     # reproducibility
SIM_START = "2025-01-01"
SIM_DAYS = 365                # one year of history to generate
INTERVAL_MINUTES = 30         # planning bucket
INTERVAL_SECONDS = INTERVAL_MINUTES * 60

# Service target
TARGET_SL = 0.80              # answer 80%...
TARGET_ANSWER_SECONDS = 20    # ...within 20 seconds  (the "80/20" standard)
MAX_OCCUPANCY = 0.90          # don't plan agents above 90% busy

# Workforce realism
SHRINKAGE = 0.30              # 30% of paid time is unavailable
AGENT_HOURLY_COST = 25.0      # $/hour, loaded cost (assumption — cite it)

# Base demand
BASE_DAILY_CONTACTS = 12000   # average contacts on a normal day
```

---

## SECTION 5 — PHASE-BY-PHASE BUILD PLAN

Each phase has: **Goal**, **Steps**, **Beginner notes**, and a **Definition of Done
(DoD)**. Build and verify in order.

---

### PHASE 1 — Generate a realistic synthetic contact dataset
**File:** `src/generate_data.py` → writes `data/raw/contacts.csv`

**Goal.** Produce ~1 year of fake-but-realistic Reservations contacts for ATL, with
believable daily/weekly/seasonal patterns and IROP spikes. This is our "history"
that everything downstream consumes. (Using synthetic data is deliberate and
privacy-safe — call this out in the README; it also showcases the de-identification
instinct.)

> ### 💡 Why synthetic data? (read this — it's a key interview answer)
> **Real Delta Reservations contact data does not exist publicly and never will** —
> contact-center logs contain confidential customer and agent data (the posting even
> flags "highly confidential material"). So pulling a real dataset isn't an option
> being declined for convenience; the thing you'd download doesn't exist. The
> realistic alternatives:
> - **Generic public call-center datasets** (e.g. Kaggle telecom support logs): not
>   airline data, no IROP/seasonality dynamics, wrong structure — you'd fight someone
>   else's messy CSV and *still* lose the Delta/ATL story.
> - **Real adjacent data for the demand signal** — the public **BTS (Bureau of
>   Transportation Statistics)** flight delay/cancellation data is genuine and free,
>   but it's *flight operations* data, not *contact* data: it tells you when
>   disruptions happened, not who called. You can't run Erlang C on it. (It's the
>   right input for the separate IROP supporting project.)
> - **Generate it (this phase)** — the correct choice, because it lets you: control
>   the structure so it feeds Erlang C and the forecast directly; build in the exact
>   phenomena that make the project interesting (IROP spikes, seasonal peaks, twin-peak
>   intraday curves, tier-based handle times); keep it fully **reproducible** (seeded);
>   and stay **privacy-safe** — which echoes the de-identification work and is a
>   *mature* answer, not an apologetic one.
>
> **The honest caveat (say this out loud):** synthetic data is only as good as its
> assumptions — a low forecast error on data *you* generated proves your pipeline is
> internally consistent, not that the method works in the wild. You turn that weakness
> into a strength two ways: **Phase 4's simulation independently validates the Erlang C
> math**, and the demand realism is grounded against real BTS disruption patterns in
> the supporting project. So the one-line framing is: *"Synthetic data to safely
> exercise a real, validated methodology — so the approach holds up the moment real
> data is plugged in."* That's exactly how an internal consultant would answer it.

**The data schema (one row = one contact):**

| Column | Type | Notes |
|---|---|---|
| `contact_id` | string | unique, e.g. `CT-000001` |
| `timestamp` | datetime | arrival time, to the second |
| `date` | date | derived |
| `interval_start` | datetime | floored to the 30-min bucket |
| `hour` | int 0–23 | derived |
| `day_of_week` | string | Mon–Sun |
| `contact_reason` | category | see distribution below |
| `customer_segment` | category | SkyMiles tier (see below) |
| `handle_time_seconds` | int | AHT for THIS contact |
| `irop_flag` | bool | True if during an IROP event |
| `resolved_first_contact` | bool | first-contact resolution |
| `repeat_contact` | bool | a follow-up to an earlier unresolved contact |
| `abandoned` | bool | left before answer (set later, or modeled here) |

**Contact reason distribution (normal days):**
- `Booking_Change` 28%, `IROP_Rebooking` 8%, `Refunds` 12%, `SkyMiles` 14%,
  `Baggage` 10%, `Seat_Upgrade` 13%, `General_Inquiry` 15%.
- During IROP events, `IROP_Rebooking` jumps to ~45% and total volume multiplies.

**Customer segments (SkyMiles tiers):** `General` 60%, `Silver` 18%, `Gold` 12%,
`Platinum` 7%, `Diamond` 3%. Higher tiers tend to get slightly longer, more
white-glove handle times (small multiplier).

**How to build volume (do it in this layered way):**
1. **Daily total** = `BASE_DAILY_CONTACTS` × `dow_factor` × `seasonal_factor`
   × `irop_factor`.
   - `dow_factor`: Mon highest (~1.25), tapering to Sat/Sun lowest (~0.7). (After a
     weekend, people call to fix travel — Mondays spike.)
   - `seasonal_factor`: summer peak (Jun–Aug ~1.3), winter-holiday peak
     (late Nov & Dec ~1.4), shoulder months ~0.9. Use a smooth function or a
     month lookup table.
   - `irop_factor`: 1.0 normally. Randomly inject **IROP events**: ~12–18 events/year,
     each lasting 1–3 days, multiplying volume ×1.8–×3.0. Mark those contacts
     `irop_flag=True`.
2. **Intraday shape:** distribute the daily total across 48 half-hour intervals using
   a realistic curve — very low overnight, rising from ~6am, twin peaks late morning
   (~10–11am) and late afternoon (~4–6pm), tapering at night. Define this as a
   normalized weight vector that sums to 1.
3. **Per-interval counts:** expected interval volume = daily_total × interval_weight.
   Draw the *actual* count from a **Poisson distribution** with that mean (real
   arrivals are Poisson — this is important and realistic).
4. **Place each contact** at a random second within its interval.
5. **Assign reason** (from the distribution; IROP-shifted if `irop_flag`), **segment**,
   and **handle time**. Draw `handle_time_seconds` from a **lognormal** distribution
   (handle times are right-skewed: most calls short, a few very long), with a
   per-reason mean (e.g. `IROP_Rebooking` ~ 6–8 min, `General_Inquiry` ~ 2–3 min).
   Apply the small segment multiplier. Clip to a sane range (30s–3600s).
6. Set `repeat_contact` (~8% normally, higher during IROP) and
   `resolved_first_contact` accordingly.

**Beginner notes.**
- "Poisson" and "lognormal" are just probability shapes. Poisson models *counts of
  random arrivals*; lognormal models *durations that can't go below zero and have a
  long tail*. `numpy` has both: `np.random.default_rng(SEED).poisson(...)` and
  `.lognormal(...)`.
- Seed everything with `SEED` so the data is reproducible.

**Definition of Done.**
- `data/raw/contacts.csv` exists with ~4–5M rows (≈12k/day × 365), all columns
  populated, no nulls.
- A quick summary script prints: total contacts, contacts/day distribution, a table
  of reason %s, and confirms at least ~12 IROP events are present.
- A sanity plot (`outputs/figures/volume_overview.png`) shows daily volume over the
  year with visible weekly seasonality, summer/holiday bumps, and spiky IROP events.
- **`tests/test_data_quality.py` passes** (`pytest tests/test_data_quality.py`). These
  are *invariant* tests — they must hold for any random seed, so assert ranges/rules,
  never exact values. At minimum:
  - no nulls in any column;
  - every `handle_time_seconds` is > 0 and within the clip range (e.g. 30–3600);
  - `contact_reason` percentages sum to ~100% (±0.5);
  - all `customer_segment` and `contact_reason` values are from the allowed sets;
  - at least 10 distinct IROP events are present (rows with `irop_flag=True` exist and
    span multiple date ranges);
  - row count is within an expected band (e.g. between 3.5M and 5.5M);
  - `interval_start` is always ≤ `timestamp` and within the same 30-min bucket.

---

### PHASE 2 — Forecast contact volume
**File:** `src/forecast.py` → writes `data/processed/forecast.csv` + a chart

**Goal.** Predict future contact volume so we can staff *ahead* of demand. Forecast
at two grains: (a) **daily totals**, and (b) a reusable **intraday profile** (the
average shape of a day) so we can turn any daily forecast into 30-min interval
volumes.

**Steps.**
1. Aggregate `contacts.csv` to a daily time series (`date`, `contacts`).
2. **Train/test split:** hold out the last ~30 days as a test set.
3. **Primary model — Prophet:**
   - Fit Prophet on the training series.
   - Add weekly + yearly seasonality (Prophet does this automatically).
   - Add **US holidays** as a built-in holiday regressor (travel spikes around them).
   - Optionally add an `irop` regressor flag (note: in real life you don't know future
     IROPs — so produce TWO forecasts: a "baseline" expected forecast, and document
     that IROP surges are handled separately as a stress scenario in Phase 5).
4. Forecast the test window, then forecast **+30 days** beyond the data.
5. **Evaluate:** compute **MAPE** and **RMSE** on the held-out test set. Print them.
   Target: MAPE comfortably under ~10% on normal days (IROP days will be worse — that's
   expected and worth explaining).
6. **Intraday profile:** from history, compute the average fraction of a day's volume
   that lands in each 30-min interval (group by interval-of-day, normalize to sum 1).
   Save as `data/processed/intraday_profile.csv`.
7. **Fallback model — SARIMA** (`statsmodels`): if Prophet won't install cleanly,
   implement `SARIMAX` with weekly seasonality. The interface (input series → forecast
   + MAPE/RMSE) must be identical so downstream code doesn't care which ran.

**Beginner notes.**
- A **forecast** here = "best guess of future volume + an uncertainty band."
- **MAPE** = average % you were off. **RMSE** = typical size of error in raw units.
- The intraday profile is the trick that lets one daily number become 48 interval
  numbers: `interval_volume = daily_forecast × interval_fraction`.

**Definition of Done.**
- `forecast.csv` has future daily volume with lower/upper bounds.
- Printed MAPE and RMSE on the test set, with a one-line plain-English read of them.
- `outputs/figures/forecast.png` shows history + forecast + confidence band, with the
  test period visibly tracked.
- `intraday_profile.csv` exists and its fractions sum to 1.0.

---

### PHASE 3 — Erlang C staffing engine (THE CORE — use verified code)
**File:** `src/erlang.py` + `tests/test_erlang.py`

**Goal.** Given the offered load and AHT for an interval, compute service level, ASA,
occupancy, and the **minimum agents** needed to hit the target. This is the math real
contact centers run. **The agent must use the reference implementation below verbatim**
(it has been numerically verified). Do not re-derive it.

```python
# src/erlang.py
import math

def erlang_b(n_agents: int, offered_load: float) -> float:
    """Erlang B blocking probability via numerically stable recursion."""
    if offered_load <= 0:
        return 0.0
    B = 1.0  # B(0, A) = 1
    for k in range(1, n_agents + 1):
        B = (offered_load * B) / (k + offered_load * B)
    return B

def erlang_c(n_agents: int, offered_load: float) -> float:
    """Probability an arriving contact must wait (is queued)."""
    if n_agents <= offered_load:
        return 1.0  # unstable: never keeps up, everyone waits
    B = erlang_b(n_agents, offered_load)
    return (n_agents * B) / (n_agents - offered_load * (1 - B))

def service_level(n_agents: int, offered_load: float,
                  aht_seconds: float, target_seconds: float) -> float:
    """Fraction of contacts answered within target_seconds (e.g., 0.80 for 80/20)."""
    if n_agents <= offered_load:
        return 0.0
    C = erlang_c(n_agents, offered_load)
    exponent = -(n_agents - offered_load) * (target_seconds / aht_seconds)
    return 1.0 - C * math.exp(exponent)

def average_speed_of_answer(n_agents: int, offered_load: float,
                            aht_seconds: float) -> float:
    """Average wait (seconds) before answer."""
    if n_agents <= offered_load:
        return float("inf")
    C = erlang_c(n_agents, offered_load)
    return (C * aht_seconds) / (n_agents - offered_load)

def occupancy(n_agents: int, offered_load: float) -> float:
    """Fraction of time agents are busy."""
    if n_agents <= 0:
        return 1.0
    return offered_load / n_agents

def offered_load_erlangs(contacts_in_interval: float, aht_seconds: float,
                         interval_seconds: float) -> float:
    """Convert a contact count + AHT into offered load (Erlangs)."""
    return contacts_in_interval * aht_seconds / interval_seconds

def agents_required(offered_load: float, aht_seconds: float,
                    target_seconds: float, target_sl: float,
                    max_occupancy: float = 0.90) -> int:
    """Smallest agent count meeting BOTH the service-level and occupancy targets."""
    if offered_load <= 0:
        return 0
    n = max(1, math.floor(offered_load) + 1)
    while True:
        sl = service_level(n, offered_load, aht_seconds, target_seconds)
        occ = occupancy(n, offered_load)
        if sl >= target_sl and occ <= max_occupancy:
            return n
        n += 1

def apply_shrinkage(required_agents: int, shrinkage: float) -> int:
    """Gross up required agents to scheduled headcount to cover breaks/training."""
    return math.ceil(required_agents / (1.0 - shrinkage))
```

**Verified behavior (the agent's tests must reproduce this).** For load = 5 Erlangs,
AHT = 180s, target = 20s:

| agents | SL@20s | ASA | occupancy |
|---|---|---|---|
| 6 | 47.4% | 105.8s | 83.3% |
| 7 | 74.0% | 29.2s | 71.4% |
| **8** | **88.0%** | **10.0s** | **62.5%** |
| 9 | 94.8% | 3.6s | 55.6% |

So `agents_required(5, 180, 20, 0.80)` → **8**.

**`tests/test_erlang.py` must assert at least:**
- `agents_required(5, 180, 20, 0.80) == 8`
- `service_level(8, 5, 180, 20)` is within 0.01 of 0.88
- `0.0 <= erlang_c(n, A) <= 1.0` for several `n > A`
- `service_level` increases monotonically as agents increase
- `agents_required` returns 0 when load is 0
- adding agents lowers ASA and occupancy

**Beginner notes.**
- Keep **all times in seconds** to avoid unit bugs (AHT, target, interval).
- "Erlang B" is an intermediate quantity used to compute Erlang C stably — don't be
  thrown by the name.
- `apply_shrinkage` is why a center that *needs* 8 working agents must *schedule*
  ~12 (8 ÷ 0.70 ≈ 11.4 → 12): the other ~30% are on break/training/meetings.

**Definition of Done.**
- `pytest tests/test_erlang.py` passes all assertions.
- A demo prints the verified table above so you can eyeball it.

---

### PHASE 4 — Validate Erlang C with a simulation
**File:** `src/simulate.py` + `tests/test_validation.py`

**Goal.** Independently confirm the Erlang C predictions by *imitating* the contact
center with a discrete-event simulation. If the simulation's measured service level
matches Erlang C's predicted service level, you can trust the model — and you'll have
a powerful "we validated our recommendation two independent ways" slide.

**Steps (using `simpy`).**
1. Model one interval as an **M/M/c queue**: contacts arrive randomly (Poisson /
   exponential gaps) at rate λ; `c` agents each serve at mean AHT (exponential service);
   contacts that find all agents busy wait in a FIFO queue.
2. Run a long simulation (e.g., simulate many hours of steady arrivals) for a given
   (λ, AHT, agents). Record each contact's wait time.
3. Compute **measured** service level (% with wait ≤ target), measured ASA, measured
   occupancy.
4. Compare measured vs Erlang-C-predicted across several scenarios (low/medium/high
   load). Print a comparison table.
5. (Optional, strong) Add **abandonment**: contacts leave if their wait exceeds a
   random patience threshold — this nudges toward the more realistic "Erlang A" model
   and is a great point to mention in interviews.

**Beginner notes.**
- A **discrete-event simulation** advances time event-by-event (arrival, answer,
  finish) rather than second-by-second — it's how you imitate a queue in code.
- Expect simulated SL to land within a couple of percentage points of Erlang C if both
  are correct. Use enough simulated contacts (tens of thousands) so the numbers settle.

**Definition of Done.**
- A printed table: for ≥3 scenarios, `predicted_SL` vs `simulated_SL` agree within
  ~3 percentage points.
- `outputs/figures/validation.png`: bar/line chart of predicted vs simulated SL.
- **`tests/test_validation.py` passes** (`pytest tests/test_validation.py`). It runs
  the simulation for at least 3 load scenarios (low/medium/high) and asserts the
  simulated service level is within tolerance of the Erlang C prediction, e.g.
  `assert abs(simulated_sl - predicted_sl) < 0.03`. Use a fixed seed and enough
  simulated contacts (tens of thousands) so the result is stable run-to-run; if it's
  flaky, raise the contact count before loosening the tolerance. This test is what
  makes "validated two independent ways" a claim a reviewer can verify by running it.

---

### PHASE 5 — Optimize staffing & quantify savings
**File:** `src/optimize.py` → writes `data/processed/staffing_plan.csv`

**Goal.** Turn the forecast into an interval-by-interval staffing requirement, then
show the money: compare the optimized plan against a naive flat-staffing baseline.

**Steps.**
1. Build the forecasted interval volumes: for each future day, multiply the daily
   forecast (Phase 2) by the intraday profile to get 48 interval volumes.
2. Estimate each interval's AHT (use the historical average AHT per interval-of-day,
   or a blended overall AHT — your choice; document it).
3. For each interval:
   - `A = offered_load_erlangs(volume, aht, INTERVAL_SECONDS)`
   - `required = agents_required(A, aht, TARGET_ANSWER_SECONDS, TARGET_SL, MAX_OCCUPANCY)`
   - `scheduled = apply_shrinkage(required, SHRINKAGE)`
   - record predicted SL, ASA, occupancy at `required`.
4. Aggregate to **agent-hours**: `scheduled × (INTERVAL_MINUTES / 60)` summed across
   all intervals = total scheduled agent-hours for the horizon.
5. **Baseline for comparison — naive flat staffing:** staff the *same* number of agents
   every open interval, sized to the peak (a common real-world over-simplification).
   Compute its agent-hours and its service level in slow intervals (it will be massively
   overstaffed off-peak).
6. **Savings = (baseline agent-hours − optimized agent-hours) × AGENT_HOURLY_COST.**
   Report the dollar figure and the % reduction. Also report *service quality*: the
   optimized plan holds ~80/20 across the day while the flat plan swings wildly.
7. **IROP stress scenario:** re-run for an IROP day (apply an IROP multiplier to volume
   and shift reason mix toward longer `IROP_Rebooking` AHT). Show how required staffing
   surges, and frame the recommendation: pre-positioned flex capacity + self-service
   deflection for IROP days.
8. **Self-service deflection mini-model (business framing):** estimate that X% of
   `Booking_Change`/`Seat_Upgrade`/`SkyMiles` contacts could shift to the Fly Delta app;
   recompute required agent-hours and show the incremental savings. Keep assumptions in
   `config.py` and label them clearly as assumptions.

**Beginner notes.**
- **Agent-hours** is the universal currency here: it converts a staffing curve into a
  single comparable number, and × hourly cost into dollars.
- The savings story is the whole point of an "internal consultant" project: not "I built
  a model," but "here's the recommendation and what it's worth."

**Definition of Done.**
- `staffing_plan.csv`: one row per future interval with volume, AHT, load, required
  agents, scheduled agents, predicted SL/ASA/occupancy.
- Printed summary: optimized vs baseline agent-hours, **$ savings**, **% reduction**,
  and the IROP + deflection scenarios.
- `outputs/figures/staffing_curve.png`: optimized vs flat staffing across a day;
  `outputs/figures/savings.png`: the dollar comparison.

---

### PHASE 6 — Dashboard (Tableau primary, Plotly fallback)
**Outputs:** CSVs in `outputs/dashboard/` + a Tableau workbook OR a Plotly dashboard

**Goal.** A non-technical reviewer (and the deck) needs a visual, clickable summary.
Delta lists Tableau/Power BI as a preferred skill — so Tableau Public is the target.

**Steps.**
1. Export tidy CSVs sized for BI tools (don't dump 5M rows — pre-aggregate):
   - `daily_volume.csv` (date, actual, forecast, lower, upper)
   - `interval_staffing.csv` (interval, volume, required, scheduled, SL, occupancy)
   - `reason_mix.csv` (reason, contacts, avg_AHT, repeat_rate)
   - `savings_summary.csv` (scenario, agent_hours, cost, SL)
2. **Build in Tableau Public** (free): publish a dashboard with
   - a volume forecast line (actual vs forecast + band),
   - a **staffing heatmap** (hour-of-day × day-of-week, color = required agents) — this
     is the visual that "looks like real WFM,"
   - a service-level gauge / KPI tiles (overall SL, ASA, occupancy, $ savings),
   - a reason-mix bar (which contact types drive load and repeat contacts).
   Put the **public link** in the README — reviewers who won't run code can still see it.
3. **Fallback (no Tableau):** build the same views as a single self-contained Plotly
   HTML file (`outputs/dashboard/dashboard.html`). The agent should implement the
   fallback automatically if Tableau isn't part of the environment.

**Definition of Done.**
- The four CSVs exist and are clean.
- Either a published Tableau Public link (in README) **or** a working
  `dashboard.html`, showing all four views.

---

### PHASE 7 — Executive PowerPoint (the capstone deliverable)
**File:** `src/build_deck.py` → `outputs/Delta_Reservations_Capstone.pptx`

**Goal.** Generate the exact artifact the internship ends with: a short, exec-ready
presentation to division leaders. Build it programmatically with `python-pptx` so it's
reproducible, then you'll polish it by hand.

**Slide outline (keep it ~6–8 slides, lead with the recommendation):**
1. **Title** — "Optimizing ATL Reservations Staffing for Service & Cost" + your name,
   "Delta Reservations — Capstone."
2. **The problem / business context** — ATL hub, demand swings, the understaffing vs
   overstaffing tradeoff, why it matters to Delta's brand.
3. **Approach (one diagram)** — Forecast → Erlang C → Simulation-validated → Optimize.
   Emphasize "validated two independent ways."
4. **Recommendation (lead with the answer)** — the optimized staffing curve and the
   headline: holds ~80/20 service while cutting agent-hours by **N%**, worth **$X**.
5. **Evidence** — forecast accuracy (MAPE), the predicted-vs-simulated validation chart.
6. **IROP resilience** — staffing surge on disruption days + the flex-capacity /
   self-service deflection recommendation.
7. **Impact & next steps** — $ savings, service stability, what you'd pilot next.
8. **Appendix** — assumptions (cost/hour, shrinkage, deflection %), data note (synthetic).

**Design rules for the agent.**
- Use Delta-ish styling: deep navy + a red accent, clean sans-serif, lots of whitespace.
  (Do **not** claim it's official Delta branding — it's a student project.)
- One idea per slide, big numbers, embed the PNGs from `outputs/figures/`.
- Put the *recommendation before the methodology* — executives want the answer first.

**Definition of Done.**
- `Delta_Reservations_Capstone.pptx` opens cleanly, ~6–8 slides, charts embedded,
  headline savings number present on the recommendation slide.

---

### PHASE 8 — README, repro, and polish
**File:** `README.md` + `run_all.py`

**Goal.** Make the repo legible to a reviewer in 60 seconds and runnable in one command.

**README must include:**
- One-paragraph pitch + the headline result (e.g., "≈N% / $X agent-hour reduction while
  holding 80/20").
- An architecture diagram (Forecast → Erlang C → Sim → Optimize → Deck).
- A **prominent note** that all data is **synthetic** (privacy-safe; no real Delta data).
- The Tableau Public link (or a screenshot of the fallback dashboard).
- How to run: `pip install -r requirements.txt` then `python run_all.py`.
- A short "What I learned / call-center concepts" section (Erlang C, service level,
  shrinkage, IROP) — this doubles as your interview prep.
- Clear labeling of every business assumption.

**`run_all.py`** runs Phases 1→5 end-to-end and regenerates all figures, so the whole
analysis reproduces from scratch.

**Definition of Done.**
- Fresh clone → `pip install -r requirements.txt` → `python run_all.py` reproduces the
  data, forecast, staffing plan, and figures with no manual steps.
- `pytest` passes (all three suites: `test_erlang.py`, `test_data_quality.py`,
  `test_validation.py`).
- README renders cleanly with the headline number and dashboard link visible.

---

## SECTION 6 — INTERVIEW TALKING POINTS (build these as you go)

For each phase, be able to answer, in plain English:
- **Phase 1:** "Why synthetic data, and why Poisson arrivals / lognormal handle times?"
- **Phase 2:** "What's your forecast accuracy, and why do IROP days break it?"
- **Phase 3:** "What is Erlang C and what does 80/20 mean?"
- **Phase 4:** "How did you *prove* the staffing math was right?" (simulation agreement)
- **Phase 5:** "What's the recommendation worth, and what did you assume to get there?"
- **Phase 6/7:** "Walk me through what you'd present to a division leader."

If you can answer those six, you can carry the whole project in an interview — which is
the actual goal: not the code, but being the candidate who clearly understands contact-
center operations.

---

## SECTION 7 — SCOPE GUARDRAILS (so the build stays finishable)

- **In scope:** single site (ATL), phone channel, 30-min intervals, Erlang C + sim,
  one forecast model + fallback, one dashboard, one deck.
- **Out of scope (mention as "future work" only):** multi-channel (chat/social) blending,
  multi-skill routing, shift scheduling/rostering (turning interval requirements into
  actual shifts), real-time intraday re-forecasting. Naming these as future work shows
  maturity without ballooning the build.

**Build order is mandatory:** 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8. Don't start a phase until
the previous phase's Definition of Done is met and you've reviewed it.
