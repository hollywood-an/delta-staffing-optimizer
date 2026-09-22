# Build the dashboard in Tableau Public

This project ships a self-contained **Plotly fallback** (`dashboard.html`) because Tableau
Public is an interactive desktop + cloud app that can't be automated from a headless build.
The four CSVs in this folder are deliberately tidy and pre-aggregated, so they drop straight
into Tableau. Follow these steps to rebuild the same four views and publish a public link.

## 1. Prerequisites
- Install **Tableau Public Desktop** (free): <https://public.tableau.com/app/discover> → *Download*.
- Create a free **Tableau Public account** (required to publish — that's what produces the shareable link).

## 2. Connect the data
In Tableau Public Desktop → **Connect → To a File → Text file**, add these (each becomes a data source):
- `outputs/dashboard/daily_volume.csv`
- `outputs/dashboard/interval_staffing.csv`
- `outputs/dashboard/reason_mix.csv`
- `outputs/dashboard/savings_summary.csv`
- `data/processed/staffing_plan.csv`  ← needed for the heatmap (see the note in view 2)

Tableau auto-detects `date` as a Date and the numeric columns as Measures.

## 3. Build the four worksheets

### View 1 — Volume forecast line  (source: `daily_volume.csv`)
- **Columns:** `date` (set to continuous, exact date).
- **Rows:** `actual` and `forecast` (drag both → they share an axis as two lines).
- **Band:** drag `lower` and `upper` onto the axis as well and make them an **Area** mark (or use
  *Analytics → Reference Band* between `lower` and `upper`) to shade the 95% interval behind the lines.
- Result: full-year actuals (navy) with the forecast (dashed) + shaded band over the holdout/future.

### View 2 — Staffing heatmap  (source: `data/processed/staffing_plan.csv`)
> The spec's `interval_staffing.csv` is the *typical-day average* and has no weekday, so the
> **weekday × hour** heatmap is built from `staffing_plan.csv`, which keeps each date.
- Calculated fields:
  - `Hour` = `INT([Interval Index] / 2)`   *(two 30-min intervals per hour)*
  - `Weekday` = `DATENAME('weekday', [Date])`
- **Columns:** `Hour`   **Rows:** `Weekday` (sort Mon→Sun).
- **Marks:** Square; **Color** = `AVG([Required Agents])` (use a sequential palette like Orange/Red).
- Result: the "looks-like-real-WFM" grid — hot midday/afternoon, cool overnight.

### View 3 — KPI tiles  (sources: `interval_staffing.csv` + `savings_summary.csv`)
Make one small text/number worksheet per KPI:
- **Service level** = `AVG([SL])`  (format %), from `interval_staffing.csv`.
- **Occupancy** = `AVG([occupancy])` (format %), from `interval_staffing.csv`.
- **ASA** = pull from `staffing_plan.csv` `AVG([Predicted Asa Seconds])` (interval_staffing omits ASA).
- **$ saved vs naive** = from `savings_summary.csv`: `SUM(cost)` where `scenario = "Naive flat (peak 24/7)"`
  minus `SUM(cost)` where `scenario = "Optimized (Erlang C)"` (a calculated field, or just show the
  cost-by-scenario bar).

### View 4 — Reason-mix bar  (source: `reason_mix.csv`)
- **Columns:** `reason`   **Rows:** `SUM([contacts])`; sort descending.
- **Label/Tooltip:** `repeat_rate` (and `avg_AHT`).

## 4. Assemble the dashboard
- New **Dashboard**; drag the four worksheets into one layout (KPIs across the top, forecast line
  full-width, heatmap + reason bar along the bottom — mirroring `dashboard.html`).

## 5. Publish & link
- **Server → Tableau Public → Save to Tableau Public As…**, sign in, give it a name, publish.
- Copy the public URL and paste it into the project `README.md` (Phase 8). The Phase-6 DoD accepts
  **either** a Tableau Public link **or** the `dashboard.html` fallback — this gives you both.
