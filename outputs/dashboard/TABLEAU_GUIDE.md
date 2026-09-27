# Build the dashboard in Tableau Public

**Published:** <https://public.tableau.com/app/profile/frank.an5089/viz/DeltaATLReservationsStaffingOptimizer/Dashboard>

Tableau Public is an interactive desktop + cloud app that can't be automated from a headless
build, so the dashboard is authored by hand from the tidy, pre-aggregated CSVs this project
exports (a self-contained Plotly version, `dashboard.html`, is generated automatically). These are
the steps used to build the published version — follow them to rebuild or modify it.

## 1. Prerequisites
- Install **Tableau Public Desktop** (free): <https://public.tableau.com/app/discover> → *Download*.
- Create a free **Tableau Public account** (required to publish — that's what produces the shareable link).

## 2. Connect the data (5 separate sources)
1. Start page → **Connect → To a File → Text file** → `outputs/dashboard/daily_volume.csv`.
   (In the Mac file dialog, **Cmd+Shift+G** lets you paste a folder path.)
2. Add each other file with **Data → New Data Source → Text file**:
   `interval_staffing.csv`, `reason_mix.csv`, `savings_summary.csv` (same folder) and
   `data/processed/staffing_plan.csv`.

> Don't drag the other CSVs from the **Files** list onto the first file's canvas — that relates
> them into one combined source. Each file should be its own data source.

Then check each column's type icon in the preview grid (📅 date, **#** number, **Abc** text):

| Source | Rows | Date | String (Abc) | Everything else |
|---|---|---|---|---|
| daily_volume | 395 | `date` | — | # |
| interval_staffing | 48 | — | `interval` | # |
| reason_mix | 7 | — | `reason` | # |
| savings_summary | 4 | — | `scenario` | # |
| staffing_plan | 1,440 | `date` | `interval_start` | # |

Tableau may parse `interval` / `interval_start` (values like `09:30`) as a date-time and show
`12/30/1899 …` — click the type icon and choose **String**. Nulls in `daily_volume` are expected
(forecast columns exist only for the last 60 days; `actual` is empty for the 30 future days).

## 3. Build the worksheets (7 sheets)

### Forecast  (source: `daily_volume`)
- Drag `Date` to **Columns**; on the pill's ▾ pick **Day** from the *lower* (continuous) list →
  green `DAY(Date)`.
- Drag `Actual` to **Rows**, then drag `Forecast` onto the chart's vertical **axis** (drop on the
  two-ruler icon) → both share one axis via **Measure Values**. Add `Lower` and `Upper` to the
  **Measure Values** card.
- **Color → Edit Colors:** Actual navy, Forecast red, Lower/Upper light gray (the band edges).
  Shrink **Size**; set axis titles *Contacts per day* / *Date*; rename the legend title
  (`Measure Names`) to *Daily volume*.
- The band is drawn as its two edges. *Analytics → Reference Band* does **not** work here: on a
  continuous date axis it computes one band across all dates, not one per day.

### HeatMap  (source: `staffing_plan`)
- `Interval Start` → **Columns** (48 half-hour columns, the project's planning interval).
- `Date` → **Rows**, then pill ▾ → **More → Weekday**.
- Marks type **Square**; `Required Agents` → **Color**, then pill ▾ → **Measure → Average**
  (SUM would add up the ~4 of each weekday in the 30-day horizon).
- Sequential red/orange palette; drag the **Sunday** header below Saturday (Mon→Sun);
  toolbar fit **Entire View**; right-click a time header → **Rotate Label**; hide the field labels
  (right-click → *Hide Field Labels for Rows / Columns*).

### KPI tiles — SL, ASA, Occupancy, Savings  (sources: `staffing_plan` + `savings_summary`)
> Don't use plain `AVG()` for these — it weights a 3am interval with 4 contacts the same as the
> 4pm peak with 600, which understates occupancy (68.6% vs 87.2%) and overstates ASA (14.3s vs 7.9s).
> The formulas below weight by volume and match the numbers in `dashboard.html`.

One worksheet per KPI: **Analysis → Create Calculated Field**, set the number format via the
field's right-click → **Default Properties → Number Format**, change the Marks type to **Text**,
drag the field onto **Text**, and enlarge it (**Text → …** → ~28pt bold).

| Sheet | Source | Formula | Format | Value |
|---|---|---|---|---|
| SL | staffing_plan | `SUM([Predicted Sl] * [Volume]) / SUM([Volume])` | Percentage, 1 dp | 87.8% |
| ASA | staffing_plan | `SUM([Predicted Asa Seconds] * [Volume]) / SUM([Volume])` | Number (Custom), 1 dp, suffix ` s` | 7.9 s |
| Occupancy | staffing_plan | `SUM([Offered Load Erlangs]) / SUM([Required Agents])` | Percentage, 1 dp | 87.2% |
| Savings | savings_summary | see below | **Currency (Custom)**, 0 dp | $845,238 |

```
SUM(IF [Scenario] = "Realistic (8h shifts)" THEN [Cost] END)
- SUM(IF [Scenario] = "Optimized (Erlang C)" THEN [Cost] END)
```
Lead with savings vs the realistic 8h-shift baseline: the naive baseline (peak headcount 24/7) is
a strawman no real center runs. (Swap in `"Naive flat (peak 24/7)"` for the $2,418,238 figure.)

Gotchas:
- A line break pasted *inside* `[Field Name]` or `"…"` breaks the formula — every field name
  should turn **orange** in the editor. Dragging fields from the Data pane into the editor avoids it.
- *Currency (Standard)* has no decimals setting; use *Currency (Custom)*.
- Build Savings on a fresh worksheet (not a duplicate), since it uses a different data source.
- Tableau auto-cleans column names (`predicted_sl` → `Predicted Sl`); if yours kept the raw
  names, use those inside the brackets instead.

### Reasons  (source: `reason_mix`)
- `Reason` → **Columns**, `Contacts` → **Rows**; toolbar **Sort Descending**; navy color.
- `Repeat Rate` → **Label** (Default Properties → Percentage, 0 dp; label text
  `<SUM(Repeat Rate)> repeat`); `avg AHT` → **Tooltip**; fit **Entire View**.

## 4. Assemble the dashboard
New Dashboard → **Size: Fixed, 1200 × 1000** → tick **Show dashboard title**.

```
┌──────────┬──────────┬──────────┬──────────┐
│    SL    │   ASA    │   Occ    │ Savings  │  ← short row
├──────────┴──────────┴────┬─────┴──────────┤
│        Forecast          │    Reasons     │
├──────────────────────────┴────────────────┤
│           HeatMap (full width)            │  ← full width so all 48 time labels fit
└───────────────────────────────────────────┘
```
Drag in this order, watching the gray drop preview: **SL** onto the empty canvas → **ASA**,
**Occupancy**, **Savings** each onto the right half of the previous tile → **Forecast** onto the
bottom edge of the *whole* dashboard (preview spans full width) → **Reasons** onto Forecast's right
edge → **HeatMap** onto the bottom edge of the whole dashboard. Then, for each KPI tile, select it
→ tile ▾ → **Fit → Entire View** (otherwise its title wraps to the width of the number), and drag
the border under the tiles up so that row is short. Legends land in a column on the right.

## 5. Publish & update
- Right-click the dashboard tab → **Hide All Sheets** (viewers land on the dashboard, not 8 tabs).
- Click an empty spot in each chart first — a selected mark is saved with the workbook and greys
  out everything else for viewers.
- **File → Save to Tableau Public As…**, sign in, name it, save (OK to creating extracts).
  Saving again under the same name overwrites the published version and keeps the link.
- Tableau publishes a *snapshot* of the CSVs. After re-running the pipeline, **Data → [source] →
  Refresh** each source, then save to Tableau Public again.
