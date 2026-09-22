# Delta Reservations — Contact Center Staffing & Service-Level Optimizer

> Synthetic-data project: forecast contact volume for Delta's Atlanta (ATL) Reservations
> contact center, size staffing with verified **Erlang C** math, validate it with a
> discrete-event **simulation**, and quantify the cost savings vs. naive flat staffing.
> Built phase by phase per [`FLAGSHIP_SPEC.md`](FLAGSHIP_SPEC.md).
>
> **Status:** Phases 1–6 complete (data → forecast → Erlang C → simulation → optimization →
> dashboard). This README is a stub — Phase 8 expands it with the architecture diagram, dashboard
> link, and headline results.

## Data & modeling assumptions

**All data is synthetic** — privacy-safe; real Delta contact-center logs are confidential and
don't exist publicly. One row = one phone contact; ~4.7M rows for one year, fully reproducible
from `SEED` in [`config.py`](config.py).

Daily contact volume is built from layered multipliers on `BASE_DAILY_CONTACTS`
(see [`src/generate_data.py`](src/generate_data.py)):

- **Day-of-week** — Monday-high, weekend-low.
- **Seasonal curve (smooth)** — broad summer plateau + year-end shape, deepest trough late
  Jan/early Feb; mean-normalized so its amplitude never moves the annual total.
- **Holiday-travel spikes (discrete)** — calendar-fixed multipliers on the heavy travel days
  around Thanksgiving and Christmas, with the holidays themselves quiet. **Volume only** —
  they do not flag IROP or shift the contact-reason mix.
- **Pre-holiday booking surge (smooth)** — see *Passengers vs. contacts* below.
- **IROP events** — random irregular-operations disruptions (weather): the tallest spikes,
  and the only layer that shifts the reason mix toward rebookings.

### Passengers vs. contacts (why the holiday shape is what it is)
Public **TSA checkpoint data measures passengers**, who peak **on** the travel day
(Thanksgiving, Christmas). This project models **contacts** (calls), which peak **before** it:
holiday travel is heavily pre-booked, so booking/change contacts swell in the ~2–3 weeks
*ahead* of the holiday (the `_booking_surge_factor` layer). A normal holiday rush therefore
generates fewer contacts than its passenger volume would imply, and the largest holiday
*contact* spikes come from weather/IROP disruption — not the smooth travel day itself.

### Future enhancements (deliberately deferred)
The synthetic demand is calibrated to TSA seasonal **shape** closely enough to exercise the
methodology — the forecaster learns whatever seasonality is present, so further shape-matching
has diminishing returns. Deferred for that reason: a fully passenger-shaped seasonal curve (an
earlier ~early-July summer peak, an early-December passenger lull) and additional travel-holiday
notches (July 4, Memorial Day, Labor Day, New Year).

## Dashboard
A self-contained **Plotly dashboard** — [`outputs/dashboard/dashboard.html`](outputs/dashboard/dashboard.html)
(opens offline) — shows four views: the volume forecast (actual vs forecast + 95% band), a
weekday × hour staffing **heatmap**, KPI tiles (service level, ASA, occupancy, $ saved), and the
contact reason mix. It's backed by four pre-aggregated CSVs in `outputs/dashboard/`
(`daily_volume`, `interval_staffing`, `reason_mix`, `savings_summary`).

Tableau Public is the spec's primary BI target, but it can't be set up in a headless build — it's
an interactive desktop + cloud app that needs manual authoring and a user account to publish. The
CSVs are Tableau-ready: to build and publish the same dashboard yourself, follow
[`outputs/dashboard/TABLEAU_GUIDE.md`](outputs/dashboard/TABLEAU_GUIDE.md), then paste the public
link here.

## Reproduce (Phase 1)
```bash
pip install -r requirements.txt
python -m src.generate_data        # regenerates data/raw/contacts.csv + the sanity plot
pytest tests/test_data_quality.py  # data-quality invariants (11 tests)
```
