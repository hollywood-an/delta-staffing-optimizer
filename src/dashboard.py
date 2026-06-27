"""Phase 6 — dashboard exports + self-contained Plotly fallback.

Tableau Public is the primary target (Delta lists Tableau/Power BI as preferred), but it is
not set up in this environment, so this builds the spec's FALLBACK: four small, pre-aggregated
CSVs sized for any BI tool, plus a single self-contained Plotly HTML dashboard
(outputs/dashboard/dashboard.html) that opens offline and shows all four views:

  1. Volume forecast line (actual vs forecast + 95% band)
  2. Staffing heatmap (weekday x hour-of-day, colour = required agents) — "looks like real WFM"
  3. KPI tiles (service level gauge, ASA, occupancy, $ saved)
  4. Reason-mix bar (which contact types drive load)

CSVs (outputs/dashboard/): daily_volume.csv, interval_staffing.csv, reason_mix.csv,
savings_summary.csv. We pre-aggregate — never dump the ~4.7M raw rows.

Run:  python -m src.dashboard
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402
from src.generate_data import RAW_CSV_PATH  # noqa: E402
from src.forecast import FORECAST_CSV, INTRADAY_CSV  # noqa: E402
from src.optimize import STAFFING_CSV, build_staffing_plan, cost_summary, agent_hours  # noqa: E402
from src.erlang import service_level  # noqa: E402

DASH_DIR = PROJECT_ROOT / "outputs" / "dashboard"
DAILY_VOLUME_CSV = DASH_DIR / "daily_volume.csv"
INTERVAL_STAFFING_CSV = DASH_DIR / "interval_staffing.csv"
REASON_MIX_CSV = DASH_DIR / "reason_mix.csv"
SAVINGS_SUMMARY_CSV = DASH_DIR / "savings_summary.csv"
HTML_PATH = DASH_DIR / "dashboard.html"

WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
INTERVALS_PER_HOUR = 60 // config.INTERVAL_MINUTES        # 2
INTERVALS_PER_DAY = 24 * INTERVALS_PER_HOUR              # 48


# --------------------------------------------------------------------------------------
# Aggregations from the raw history
# --------------------------------------------------------------------------------------
def _read_contacts():
    return pd.read_csv(RAW_CSV_PATH, usecols=[
        "date", "interval_start", "handle_time_seconds", "contact_reason",
        "repeat_contact", "irop_flag"])


def _daily_actuals(contacts):
    return contacts.groupby("date").size().rename("actual")


def _reason_mix(contacts):
    rep = contacts["repeat_contact"]
    if rep.dtype != bool:                      # robust if the CSV reader inferred strings
        rep = rep.map({True: True, False: False, "True": True, "False": False})
    df = contacts.assign(_repeat=rep.astype(bool))
    g = df.groupby("contact_reason", observed=True)
    out = pd.DataFrame({
        "contacts": g.size(),
        "avg_AHT": g["handle_time_seconds"].mean().round(1),
        "repeat_rate": g["_repeat"].mean().round(4),
    }).reset_index().rename(columns={"contact_reason": "reason"})
    return out.sort_values("contacts", ascending=False)


def _aht_and_deflectable(contacts):
    """Per-bucket normal AHT (48) + non-IROP deflectable share (for the deflection scenario)."""
    ist = contacts["interval_start"].astype(str)
    bucket = (ist.str.slice(11, 13).astype(int) * INTERVALS_PER_HOUR
              + (ist.str.slice(14, 16) == "30").astype(int))
    irop = contacts["irop_flag"]
    if irop.dtype != bool:                     # robust if the CSV reader inferred strings
        irop = irop.map({True: True, False: False, "True": True, "False": False})
    is_irop = irop.astype(bool)
    normal_ht = contacts.loc[~is_irop, "handle_time_seconds"]
    aht = (normal_ht.groupby(bucket[~is_irop]).mean()
           .reindex(range(INTERVALS_PER_DAY)).fillna(normal_ht.mean()))
    share = float(contacts.loc[~is_irop, "contact_reason"].isin(config.DEFLECTABLE_REASONS).mean())
    return aht.to_numpy(), share


# --------------------------------------------------------------------------------------
# Service-level helpers for the baseline scenarios
# --------------------------------------------------------------------------------------
def _vw(df, col):
    return float(np.average(df[col], weights=df["volume"]))


def _baseline_vw_sl(plan, working):
    """Volume-weighted service level if `working` agents (scalar or per-row Series) staff
    each interval against its offered load + AHT."""
    work = np.asarray(working if hasattr(working, "__len__") else [working] * len(plan))
    sl = [service_level(int(w), a, h, config.TARGET_ANSWER_SECONDS)
          for w, a, h in zip(work, plan["offered_load_erlangs"], plan["aht_seconds"])]
    return float(np.average(sl, weights=plan["volume"]))


# --------------------------------------------------------------------------------------
# Build the four CSVs
# --------------------------------------------------------------------------------------
def build_exports():
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    print("Aggregating history for dashboard exports...")
    contacts = _read_contacts()
    daily_actual = _daily_actuals(contacts)
    reason_mix = _reason_mix(contacts)
    aht_by_bucket, deflectable_share = _aht_and_deflectable(contacts)
    del contacts

    # --- daily_volume.csv: full actual history + forecast (+ 95% band) ---
    fc = pd.read_csv(FORECAST_CSV)
    daily = daily_actual.reset_index()
    daily["date"] = daily["date"].astype(str)
    fc_small = fc[["date", "forecast", "forecast_lower", "forecast_upper"]].rename(
        columns={"forecast_lower": "lower", "forecast_upper": "upper"})
    daily_volume = (daily.merge(fc_small, on="date", how="outer")
                    .sort_values("date").reset_index(drop=True))
    for col in ("actual", "forecast", "lower", "upper"):   # counts are integers, not 10980.0
        daily_volume[col] = daily_volume[col].round().astype("Int64")
    daily_volume.to_csv(DAILY_VOLUME_CSV, index=False)

    # --- interval_staffing.csv: typical day (mean over the future horizon by interval-of-day) ---
    plan = pd.read_csv(STAFFING_CSV)
    interval_staffing = (plan.groupby("interval_index").agg(
        interval=("interval_start", "first"),
        volume=("volume", "mean"),
        required=("required_agents", "mean"),
        scheduled=("scheduled_agents", "mean"),
        SL=("predicted_sl", "mean"),
        occupancy=("predicted_occupancy", "mean"),
    ).reset_index(drop=True))   # interval-of-day already sorted; drop the numeric key
    interval_staffing = interval_staffing.round(
        {"volume": 1, "required": 1, "scheduled": 1, "SL": 4, "occupancy": 4})
    interval_staffing.to_csv(INTERVAL_STAFFING_CSV, index=False)

    # --- reason_mix.csv ---
    reason_mix.to_csv(REASON_MIX_CSV, index=False)

    # --- savings_summary.csv: 4 scenarios (matches savings.png) ---
    cost = cost_summary(plan)
    fut = fc[fc["segment"] == "future"][["date", "forecast"]].copy()
    fut["forecast"] = fut["forecast"].astype(float)
    profile = pd.read_csv(INTRADAY_CSV).sort_values("interval_index")
    fractions, labels = profile["fraction"].to_numpy(), profile["interval_start"].tolist()
    deflection_factor = 1.0 - deflectable_share * config.DEFLECTION_RATE
    plan_def = build_staffing_plan(fut, fractions, labels, aht_by_bucket,
                                   volume_scale=deflection_factor)
    def_hours = agent_hours(plan_def["scheduled_agents"])

    peak_required = int(plan["required_agents"].max())
    block = plan["interval_index"] // (config.REALISTIC_BASELINE_BLOCK_HOURS * INTERVALS_PER_HOUR)
    block_peak_req = plan.groupby([plan["date"], block])["required_agents"].transform("max")
    savings = pd.DataFrame([
        {"scenario": "Naive flat (peak 24/7)", "agent_hours": cost["flat_hours"],
         "SL": _baseline_vw_sl(plan, peak_required)},
        {"scenario": f"Realistic ({config.REALISTIC_BASELINE_BLOCK_HOURS}h shifts)",
         "agent_hours": cost["realistic_hours"], "SL": _baseline_vw_sl(plan, block_peak_req)},
        {"scenario": "Optimized (Erlang C)", "agent_hours": cost["opt_hours"],
         "SL": _vw(plan, "predicted_sl")},
        {"scenario": "Optimized + deflection", "agent_hours": def_hours,
         "SL": _vw(plan_def, "predicted_sl")},
    ])
    savings["agent_hours"] = savings["agent_hours"].round(0)
    savings["cost"] = (savings["agent_hours"] * config.AGENT_HOURLY_COST).round(0)  # reconciles
    savings["SL"] = savings["SL"].round(4)
    savings = savings[["scenario", "agent_hours", "cost", "SL"]]
    savings.to_csv(SAVINGS_SUMMARY_CSV, index=False)

    kpis = {
        "service_level": _vw(plan, "predicted_sl"),
        "asa": _vw(plan, "predicted_asa_seconds"),
        # occupancy is an agent-TIME metric: total offered load / total required agents.
        "occupancy": float(plan["offered_load_erlangs"].sum() / plan["required_agents"].sum()),
        "saved_vs_naive": cost["saved_dollars"],
        "saved_vs_realistic": cost["realistic_saved_dollars"],
    }
    return daily_volume, plan, reason_mix, savings, kpis


# --------------------------------------------------------------------------------------
# Build the self-contained Plotly dashboard
# --------------------------------------------------------------------------------------
def _build_figure(daily_volume, plan, reason_mix, kpis):
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    # staffing heatmap data: weekday x hour-of-day, mean required agents
    p = plan.copy()
    p["weekday"] = pd.to_datetime(p["date"]).dt.day_name()
    p["hour"] = p["interval_index"] // INTERVALS_PER_HOUR
    grid = (p.groupby(["weekday", "hour"])["required_agents"].mean()
            .unstack("hour").reindex(WEEKDAYS).reindex(columns=range(24)))

    specs = [
        [{"type": "indicator"}, {"type": "indicator"}, {"type": "indicator"}, {"type": "indicator"}],
        [{"type": "xy", "colspan": 4}, None, None, None],
        [{"type": "heatmap", "colspan": 2}, None, {"type": "xy", "colspan": 2}, None],
    ]
    fig = make_subplots(
        rows=3, cols=4, specs=specs, row_heights=[0.16, 0.44, 0.40],
        vertical_spacing=0.11, horizontal_spacing=0.08,
        subplot_titles=("", "", "", "",
                        "Daily contact volume — actual vs forecast (95% band)",
                        "Required agents — weekday × hour-of-day",
                        "Contact reason mix"))

    navy, red = "#1b2a4a", "#c8102e"
    # --- KPI tiles ---
    fig.add_trace(go.Indicator(
        mode="gauge+number", value=kpis["service_level"] * 100,
        number={"suffix": "%", "valueformat": ".1f"}, title={"text": "Service level (80/20)"},
        gauge={"axis": {"range": [0, 100]}, "bar": {"color": navy},
               "threshold": {"line": {"color": red, "width": 3}, "value": config.TARGET_SL * 100}}),
        row=1, col=1)
    fig.add_trace(go.Indicator(mode="number", value=kpis["asa"],
        number={"suffix": " s", "valueformat": ".1f"}, title={"text": "Avg speed of answer"}),
        row=1, col=2)
    fig.add_trace(go.Indicator(mode="number", value=kpis["occupancy"] * 100,
        number={"suffix": "%", "valueformat": ".0f"}, title={"text": "Occupancy"}),
        row=1, col=3)
    fig.add_trace(go.Indicator(mode="number", value=kpis["saved_vs_naive"],
        number={"prefix": "$", "valueformat": ",.0f"},
        title={"text": "Saved vs naive (30d)"}), row=1, col=4)

    # --- forecast line + band ---
    dv = daily_volume.copy()
    dv["date"] = pd.to_datetime(dv["date"])
    band = dv.dropna(subset=["lower", "upper"])
    fig.add_trace(go.Scatter(x=band["date"], y=band["upper"], line={"width": 0},
                             showlegend=False, hoverinfo="skip"), row=2, col=1)
    fig.add_trace(go.Scatter(x=band["date"], y=band["lower"], fill="tonexty",
                             fillcolor="rgba(200,16,46,0.12)", line={"width": 0},
                             name="95% band"), row=2, col=1)
    fig.add_trace(go.Scatter(x=dv["date"], y=dv["actual"], line={"color": navy, "width": 1},
                             name="Actual"), row=2, col=1)
    fig.add_trace(go.Scatter(x=dv["date"], y=dv["forecast"], line={"color": red, "dash": "dash"},
                             name="Forecast"), row=2, col=1)

    # --- staffing heatmap ---
    fig.add_trace(go.Heatmap(z=grid.to_numpy(), x=[f"{h:02d}:00" for h in range(24)],
                             y=WEEKDAYS, colorscale="YlOrRd",
                             colorbar={"title": "agents", "len": 0.32, "y": 0.16, "x": 0.46}),
                  row=3, col=1)

    # --- reason-mix bar ---
    fig.add_trace(go.Bar(x=reason_mix["reason"], y=reason_mix["contacts"], marker_color=navy,
                         text=[f"{r*100:.0f}% repeat" for r in reason_mix["repeat_rate"]],
                         textposition="outside", name="Contacts", showlegend=False), row=3, col=3)

    fig.update_yaxes(title_text="contacts/day", row=2, col=1)
    fig.update_yaxes(title_text="contacts", row=3, col=3)
    fig.update_xaxes(tickangle=-30, row=3, col=3)
    fig.update_layout(
        title={"text": "Delta ATL Reservations — Staffing & Service-Level Dashboard "
                       "<sub>(synthetic data; Plotly fallback for Tableau)</sub>", "x": 0.5},
        height=1080, width=1500, showlegend=True,
        legend={"orientation": "h", "y": 0.81, "yanchor": "middle", "x": 0.5, "xanchor": "center"},
        margin={"t": 90, "b": 40}, font={"family": "Helvetica, Arial, sans-serif"})
    return fig


def build_html(daily_volume, plan, reason_mix, kpis, path=HTML_PATH):
    fig = _build_figure(daily_volume, plan, reason_mix, kpis)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(path), include_plotlyjs=True, full_html=True)
    return path


def run_dashboard():
    daily_volume, plan, reason_mix, savings, kpis = build_exports()
    html = build_html(daily_volume, plan, reason_mix, kpis)
    print("=" * 64)
    print("DASHBOARD EXPORTS")
    print("=" * 64)
    for f in (DAILY_VOLUME_CSV, INTERVAL_STAFFING_CSV, REASON_MIX_CSV, SAVINGS_SUMMARY_CSV):
        print(f"  {f.relative_to(PROJECT_ROOT)}")
    print(f"  {html.relative_to(PROJECT_ROOT)}  ({html.stat().st_size/1e6:.1f} MB, self-contained)")
    print("-" * 64)
    print("KPIs (optimized plan, volume-weighted):")
    print(f"  service level {kpis['service_level']*100:.1f}% | ASA {kpis['asa']:.1f}s | "
          f"occupancy {kpis['occupancy']*100:.0f}% | saved vs naive ${kpis['saved_vs_naive']:,.0f}")
    print("Reason mix (top):")
    for _, r in reason_mix.head(3).iterrows():
        print(f"  {r['reason']:<16} {int(r['contacts']):>9,} contacts | "
              f"AHT {r['avg_AHT']:.0f}s | repeat {r['repeat_rate']*100:.0f}%")
    print("=" * 64)
    return savings


def main() -> None:
    run_dashboard()


if __name__ == "__main__":
    main()
