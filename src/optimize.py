"""Phase 5 — optimize staffing and quantify the savings.

Turns the Phase-2 forecast into an interval-by-interval staffing requirement (via the verified
Phase-3 Erlang C engine), then shows the money: optimized staffing vs a naive flat baseline,
plus an IROP stress scenario and a self-service deflection mini-model.

Pipeline (per spec):
  daily forecast x intraday profile -> interval volumes
  -> offered load -> agents_required (Erlang C) -> scheduled (grossed up for shrinkage)
  -> agent-hours -> $ vs a peak-sized flat baseline.

Every business assumption (hourly cost, shrinkage, deflection %, IROP multiplier) lives in
config.py and is echoed in the printed summary, so a reviewer can change one number and re-run.

Run:  python -m src.optimize
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
from src.erlang import (  # noqa: E402
    offered_load_erlangs, agents_required, apply_shrinkage,
    service_level, average_speed_of_answer, occupancy,
)

PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
FORECAST_CSV = PROCESSED_DIR / "forecast.csv"
INTRADAY_CSV = PROCESSED_DIR / "intraday_profile.csv"
STAFFING_CSV = PROCESSED_DIR / "staffing_plan.csv"
FIG_DIR = PROJECT_ROOT / "outputs" / "figures"

INTERVALS_PER_DAY = 24 * 60 // config.INTERVAL_MINUTES        # 48
INTERVAL_HOURS = config.INTERVAL_MINUTES / 60.0              # 0.5 h


# --------------------------------------------------------------------------------------
# Historical handle time + reason mix (from the Phase-1 dataset)
# --------------------------------------------------------------------------------------
def _historical_aht_and_mix():
    """Return (aht_by_bucket[48] for normal days, aht_irop scalar, deflectable_share)."""
    df = pd.read_csv(RAW_CSV_PATH,
                     usecols=["interval_start", "handle_time_seconds", "irop_flag", "contact_reason"])
    ist = df["interval_start"].astype(str)
    bucket = ist.str.slice(11, 13).astype(int) * 2 + (ist.str.slice(14, 16) == "30").astype(int)
    is_irop = df["irop_flag"].astype(bool)

    normal_ht = df.loc[~is_irop, "handle_time_seconds"]
    aht_by_bucket = normal_ht.groupby(bucket[~is_irop]).mean().reindex(range(INTERVALS_PER_DAY))
    aht_by_bucket = aht_by_bucket.fillna(normal_ht.mean()).to_numpy()
    aht_irop = float(df.loc[is_irop, "handle_time_seconds"].mean())
    # Share is measured on NORMAL days only, to match the non-IROP AHT basis and the
    # normal-day forecast the deflection scenario is applied to.
    deflectable_share = float(
        df.loc[~is_irop, "contact_reason"].isin(config.DEFLECTABLE_REASONS).mean())
    return aht_by_bucket, aht_irop, deflectable_share


# --------------------------------------------------------------------------------------
# Erlang C staffing for one interval / one day
# --------------------------------------------------------------------------------------
def interval_plan(volume: float, aht: float) -> dict:
    """Erlang-C staffing + service metrics for a single 30-min interval."""
    load = offered_load_erlangs(volume, aht, config.INTERVAL_SECONDS)
    required = agents_required(load, aht, config.TARGET_ANSWER_SECONDS,
                               config.TARGET_SL, config.MAX_OCCUPANCY)
    scheduled = apply_shrinkage(required, config.SHRINKAGE)
    if required > 0:
        sl = service_level(required, load, aht, config.TARGET_ANSWER_SECONDS)
        asa = average_speed_of_answer(required, load, aht)
        occ = occupancy(required, load)
    else:
        sl, asa, occ = 1.0, 0.0, 0.0
    return {"offered_load_erlangs": load, "required_agents": required,
            "scheduled_agents": scheduled, "predicted_sl": sl,
            "predicted_asa_seconds": asa, "predicted_occupancy": occ}


def _day_rows(date, daily_total, fractions, labels, aht):
    """Build the 48 interval rows for one day. `aht` is a per-bucket array or a scalar."""
    rows = []
    for b in range(INTERVALS_PER_DAY):
        volume = daily_total * fractions[b]
        a = float(aht[b]) if hasattr(aht, "__len__") else float(aht)
        m = interval_plan(volume, a)
        rows.append({
            "date": date, "interval_index": b, "interval_start": labels[b],
            "volume": round(volume, 1), "aht_seconds": round(a, 1),
            "offered_load_erlangs": round(m["offered_load_erlangs"], 2),
            "required_agents": m["required_agents"], "scheduled_agents": m["scheduled_agents"],
            "predicted_sl": round(m["predicted_sl"], 4),
            "predicted_asa_seconds": round(m["predicted_asa_seconds"], 1),
            "predicted_occupancy": round(m["predicted_occupancy"], 4),
        })
    return rows


def build_staffing_plan(fut_daily, fractions, labels, aht_by_bucket, volume_scale=1.0):
    """One row per future interval (n_days x 48)."""
    records = []
    for _, day in fut_daily.iterrows():
        records.extend(_day_rows(day["date"], float(day["forecast"]) * volume_scale,
                                 fractions, labels, aht_by_bucket))
    return pd.DataFrame(records)


# --------------------------------------------------------------------------------------
# Money
# --------------------------------------------------------------------------------------
def agent_hours(scheduled) -> float:
    return float(np.sum(scheduled) * INTERVAL_HOURS)


def cost_summary(plan: pd.DataFrame) -> dict:
    """Optimized vs naive flat baseline (peak-staffed every interval)."""
    opt_hours = agent_hours(plan["scheduled_agents"])
    peak_scheduled = int(plan["scheduled_agents"].max())
    flat_hours = peak_scheduled * len(plan) * INTERVAL_HOURS
    saved_hours = flat_hours - opt_hours
    return {
        "opt_hours": opt_hours, "flat_hours": flat_hours, "peak_scheduled": peak_scheduled,
        "saved_hours": saved_hours,
        "opt_cost": opt_hours * config.AGENT_HOURLY_COST,
        "flat_cost": flat_hours * config.AGENT_HOURLY_COST,
        "saved_dollars": saved_hours * config.AGENT_HOURLY_COST,
        "pct_reduction": saved_hours / flat_hours if flat_hours else 0.0,
    }


# --------------------------------------------------------------------------------------
# Plots
# --------------------------------------------------------------------------------------
def plot_staffing_curve(plan, peak_scheduled, irop_curve, labels, path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    avg = plan.groupby("interval_index")["scheduled_agents"].mean().reindex(range(INTERVALS_PER_DAY))
    x = np.arange(INTERVALS_PER_DAY)
    fig, ax = plt.subplots(figsize=(13, 5))
    ax.step(x, avg.to_numpy(), where="mid", color="#1b2a4a", lw=1.8,
            label="Optimized (Erlang C, per interval)")
    ax.axhline(peak_scheduled, color="#c8102e", ls="--", lw=1.5,
               label=f"Flat baseline (peak = {peak_scheduled} agents, all day)")
    ax.step(x, irop_curve, where="mid", color="#d4a017", lw=1.5, alpha=0.9,
            label="Optimized on an IROP day (x volume, longer AHT)")
    ax.fill_between(x, avg.to_numpy(), peak_scheduled, step="mid", color="#c8102e", alpha=0.08)
    tick = range(0, INTERVALS_PER_DAY, 4)
    ax.set_xticks(list(tick))
    ax.set_xticklabels([labels[i] for i in tick], rotation=45, ha="right", fontsize=8)
    ax.set_xlabel("Time of day (30-min interval)")
    ax.set_ylabel("Scheduled agents")
    ax.set_title("Staffing curve — optimized vs naive flat (shaded = off-peak overstaffing)")
    ax.legend(loc="upper left", fontsize=8)
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=120)
    plt.close(fig)


def plot_savings(cost, deflect_cost, path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    labels = ["Flat baseline\n(peak-staffed)", "Optimized\n(Erlang C)", "Optimized\n+ deflection"]
    costs = [cost["flat_cost"], cost["opt_cost"], deflect_cost]
    colors = ["#c8102e", "#1b2a4a", "#0f7a3d"]
    x = np.arange(3)
    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(x, costs, color=colors, width=0.6)
    for b, c in zip(bars, costs):
        ax.annotate(f"${c/1000:,.0f}k", (b.get_x() + b.get_width() / 2, c),
                    ha="center", va="bottom", fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel(f"Agent cost over horizon (${config.AGENT_HOURLY_COST:.0f}/h)")
    ax.set_title(f"Cost vs naive flat staffing — {cost['pct_reduction']*100:.0f}% reduction, "
                 f"${cost['saved_dollars']/1000:,.0f}k saved")
    ax.set_ylim(0, max(costs) * 1.15)
    ax.grid(True, axis="y", alpha=0.25)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=120)
    plt.close(fig)


# --------------------------------------------------------------------------------------
# Pipeline
# --------------------------------------------------------------------------------------
def run_optimize() -> dict:
    print("Loading forecast, intraday profile, and historical handle times...")
    forecast = pd.read_csv(FORECAST_CSV)
    fut = forecast[forecast["segment"] == "future"][["date", "forecast"]].copy()
    fut["forecast"] = fut["forecast"].astype(float)
    n_days = len(fut)

    profile = pd.read_csv(INTRADAY_CSV).sort_values("interval_index")
    fractions = profile["fraction"].to_numpy()
    labels = profile["interval_start"].tolist()
    aht_by_bucket, aht_irop, deflectable_share = _historical_aht_and_mix()

    # --- optimized plan ---
    plan = build_staffing_plan(fut, fractions, labels, aht_by_bucket)
    plan.to_csv(STAFFING_CSV, index=False)
    cost = cost_summary(plan)

    # --- service quality: optimized meets SL by construction; flat wastes off-peak ---
    opt_min_sl = float(plan["predicted_sl"].min())
    opt_mean_occ = float(plan.loc[plan["required_agents"] > 0, "predicted_occupancy"].mean())
    peak_required = int(plan["required_agents"].max())
    flat_occ = plan["offered_load_erlangs"] / peak_required          # if peak-staffed everywhere
    flat_occ_min, flat_occ_mean = float(flat_occ.min()), float(flat_occ.mean())
    flat_occ_max = float(flat_occ.max())

    # --- self-service deflection mini-model ---
    deflection_factor = 1.0 - deflectable_share * config.DEFLECTION_RATE
    plan_def = build_staffing_plan(fut, fractions, labels, aht_by_bucket, volume_scale=deflection_factor)
    def_hours = agent_hours(plan_def["scheduled_agents"])
    def_cost = def_hours * config.AGENT_HOURLY_COST
    def_saved_hours = cost["opt_hours"] - def_hours
    def_saved_dollars = def_saved_hours * config.AGENT_HOURLY_COST

    # --- IROP stress day (representative average day x multiplier, longer AHT) ---
    avg_daily = float(fut["forecast"].mean())
    normal_day = pd.DataFrame(_day_rows("normal", avg_daily, fractions, labels, aht_by_bucket))
    irop_day = pd.DataFrame(_day_rows("irop", avg_daily * config.IROP_VOLUME_MULTIPLIER,
                                      fractions, labels, aht_irop))
    irop_peak = int(irop_day["required_agents"].max())
    normal_peak = int(normal_day["required_agents"].max())
    irop_day_hours = agent_hours(irop_day["scheduled_agents"])
    normal_day_hours = agent_hours(normal_day["scheduled_agents"])

    # --- figures ---
    plot_staffing_curve(plan, cost["peak_scheduled"],
                        irop_day["scheduled_agents"].to_numpy(), labels, FIG_DIR / "staffing_curve.png")
    plot_savings(cost, def_cost, FIG_DIR / "savings.png")

    results = {
        "n_days": n_days, "cost": cost, "deflectable_share": deflectable_share,
        "deflection_factor": deflection_factor, "def_hours": def_hours,
        "def_saved_hours": def_saved_hours, "def_saved_dollars": def_saved_dollars,
        "aht_irop": aht_irop, "irop_peak": irop_peak, "normal_peak": normal_peak,
        "irop_day_hours": irop_day_hours, "normal_day_hours": normal_day_hours,
        "opt_min_sl": opt_min_sl, "opt_mean_occ": opt_mean_occ,
        "flat_occ_min": flat_occ_min, "flat_occ_mean": flat_occ_mean, "flat_occ_max": flat_occ_max,
    }
    _print_summary(results, plan)
    return results


def _print_summary(r: dict, plan: pd.DataFrame) -> None:
    c = r["cost"]
    print("=" * 72)
    print(f"OPTIMIZED STAFFING & SAVINGS — horizon {r['n_days']} days "
          f"({plan['date'].iloc[0]} .. {plan['date'].iloc[-1]})")
    print("=" * 72)
    print("Assumptions (config.py): "
          f"cost ${config.AGENT_HOURLY_COST:.0f}/h | shrinkage {config.SHRINKAGE*100:.0f}% | "
          f"deflection {config.DEFLECTION_RATE*100:.0f}% of {config.DEFLECTABLE_REASONS} | "
          f"IROP x{config.IROP_VOLUME_MULTIPLIER:g}")
    print("-" * 72)
    print("AGENT-HOURS over the horizon:")
    print(f"  Flat baseline (naive: {c['peak_scheduled']} agents every interval, incl. overnight): "
          f"{c['flat_hours']:>11,.0f} h  -> ${c['flat_cost']:>13,.0f}")
    print(f"  Optimized  (Erlang C, interval-by-interval)                  : "
          f"{c['opt_hours']:>11,.0f} h  -> ${c['opt_cost']:>13,.0f}")
    print(f"  >>> SAVINGS: {c['saved_hours']:,.0f} agent-hours = "
          f"${c['saved_dollars']:,.0f}  ({c['pct_reduction']*100:.1f}% reduction)")
    print("-" * 72)
    print("SERVICE QUALITY:")
    print(f"  Optimized: sized so every interval meets >= {config.TARGET_SL*100:.0f}% by "
          f"construction (min {r['opt_min_sl']*100:.1f}%; the method is independently validated in "
          f"Phase 4), mean occupancy {r['opt_mean_occ']*100:.1f}% (efficient)")
    print(f"  Flat: SL is ~100% off-peak (overstaffed, not better service) -- the waste shows as "
          f"idle occupancy swinging {r['flat_occ_min']*100:.1f}% to {r['flat_occ_max']*100:.0f}% "
          f"(mean {r['flat_occ_mean']*100:.1f}%)")
    print("-" * 72)
    print(f"DEFLECTION scenario ({config.DEFLECTION_RATE*100:.0f}% of deflectable contacts -> "
          f"Fly Delta app):")
    print(f"  Deflectable share of contacts: {r['deflectable_share']*100:.1f}% "
          f"-> volume -{(1-r['deflection_factor'])*100:.1f}%")
    print(f"  Optimized + deflection: {r['def_hours']:,.0f} agent-hours "
          f"(incremental ${r['def_saved_dollars']:,.0f}, {r['def_saved_hours']/c['opt_hours']*100:.1f}% "
          f"further). Volume-only -> a modest UPPER bound: residual AHT rises a little as the "
          f"shorter deflected calls leave.")
    print("-" * 72)
    print(f"IROP STRESS DAY (x{config.IROP_VOLUME_MULTIPLIER:g} volume, AHT {r['aht_irop']:.0f}s) "
          f"vs an average forecast day:")
    print(f"  Peak required agents surges {r['normal_peak']} -> {r['irop_peak']} "
          f"(+{(r['irop_peak']/r['normal_peak']-1)*100:.0f}%); IROP-day agent-hours "
          f"{r['irop_day_hours']:,.0f} vs {r['normal_day_hours']:,.0f} "
          f"(x{r['irop_day_hours']/r['normal_day_hours']:.1f})")
    print("  Recommendation: pre-positioned flex capacity + self-service deflection for IROP days.")
    print("-" * 72)
    print(f"Wrote {STAFFING_CSV.relative_to(PROJECT_ROOT)} ({len(plan):,} interval rows), "
          f"{(FIG_DIR/'staffing_curve.png').relative_to(PROJECT_ROOT)}, "
          f"{(FIG_DIR/'savings.png').relative_to(PROJECT_ROOT)}")
    print("=" * 72)


def main() -> None:
    run_optimize()


if __name__ == "__main__":
    main()
