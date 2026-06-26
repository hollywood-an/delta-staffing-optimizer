"""Phase 1 — generate a realistic synthetic contact dataset for ATL Reservations.

One row = one phone contact. The dataset is fully reproducible (seeded) and is the
"history" every downstream phase consumes. It is intentionally *synthetic*: real
contact-center logs are confidential and do not exist publicly, so we generate data
whose structure feeds Erlang C and the forecast directly while building in the
phenomena that make the problem interesting (IROP spikes, weekly/seasonal peaks,
twin-peak intraday curve, tier-based handle times).

Generation is fully vectorized with numpy so ~4-5M rows build in seconds rather than
via a per-row Python loop. Run this file directly to (re)generate the dataset, print a
summary, and save the sanity plot:

    python -m src.generate_data        # from the project root
    # or
    python src/generate_data.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Make `import config` work whether this is run as a script or as `-m src.generate_data`.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402  (path set up above)

# --------------------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------------------
RAW_CSV_PATH = PROJECT_ROOT / "data" / "raw" / "contacts.csv"
FIG_PATH = PROJECT_ROOT / "outputs" / "figures" / "volume_overview.png"

# --------------------------------------------------------------------------------------
# Domain assumptions (all tunable; the "knobs" live here and in config.py)
# --------------------------------------------------------------------------------------
INTERVALS_PER_DAY = 24 * 60 // config.INTERVAL_MINUTES          # 48 half-hour buckets
INTERVAL_SECONDS = config.INTERVAL_SECONDS                      # 1800

# Day-of-week multipliers (Mon=0 .. Sun=6). Mondays spike (people fix weekend travel);
# weekends are quietest. (Combined with the seasonal, holiday and IROP factors the
# realized mean lands near ~12.9k contacts/day, ~4.7M rows/year.)
DOW_FACTOR = {0: 1.25, 1: 1.08, 2: 1.03, 3: 1.03, 4: 0.95, 5: 0.70, 6: 0.65}

# ---- Seasonal model: THREE layers, calibrated to real TSA checkpoint throughput -------
# Layer 1 (smooth): a mean-normalized seasonal curve — a broad SUMMER PLATEAU (Jun-Aug)
# plus a GENTLE year-end rise, with the deepest trough in late Jan / early Feb. In real
# TSA data summer is a gentle swell (peak/trough only ~1.4x), NOT a spike, so the amplitude
# is modest (summer ~1.23, winter ~0.84). Mean-normalized to 1.0 so the curve's amplitude
# never moves the annual total — the row-count test band stays valid at any amplitude.
SEASONAL_SUMMER_CENTER_DOY = 197      # mid-July
SEASONAL_SUMMER_WIDTH = 62            # broad flat-topped plateau ~Jun-Aug
SEASONAL_YEAREND_CENTER_DOY = 360     # gentle rise into late December
SEASONAL_YEAREND_WIDTH = 24
SEASONAL_WINTER_CENTER_DOY = 34       # deepest dip, late Jan / early Feb
SEASONAL_WINTER_WIDTH = 18
SEASONAL_BASE = 0.88
SEASONAL_SUMMER_AMP = 0.34
SEASONAL_YEAREND_AMP = 0.17
SEASONAL_WINTER_AMP = 0.045

# Layer 2 (discrete): calendar-fixed HOLIDAY-TRAVEL multipliers. Real holiday demand is
# not a smooth swell — it is sharp spikes on the heavy travel days around Thanksgiving and
# Christmas, while the holidays themselves are quiet. Weekend travel days are boosted so
# they overcome the low weekend DOW factor (the Sunday after Thanksgiving is, in reality,
# the busiest travel day of the year). These scale VOLUME ONLY — they do not set irop_flag
# or shift the reason mix (a holiday rush is busy *normal* operations, not a disruption).
THANKSGIVING_WED_MULT = 1.65          # Wednesday before Thanksgiving (departure peak)
THANKSGIVING_DAY_MULT = 0.55          # Thanksgiving Day itself (quiet)
THANKSGIVING_SUN_MULT = 2.55          # Sunday after (weekend-boosted; the annual peak)
CHRISTMAS_MULTS = {                   # by day of December
    22: 1.40, 23: 1.45,               # pre-Christmas departure peak
    24: 0.95, 25: 0.50,               # Christmas Eve winding down / Christmas Day quiet
    26: 1.50, 27: 2.15,               # post-Christmas return peak (27th weekend-boosted)
}

# Layer 4 (smooth pre-holiday booking surge). THE passenger-vs-contact distinction: public
# TSA throughput measures PASSENGERS, who peak ON the travel day; we model CONTACTS, which
# peak BEFORE it. Holiday travel is heavily pre-booked, so booking/change contacts swell in
# the ~2-3 weeks AHEAD of Thanksgiving and Christmas — the contact peak LEADS the throughput
# peak. A Gaussian centred a few days before each holiday, tapering after; scales VOLUME only.
BOOKING_SURGE_AMP = 0.22              # peak pre-holiday elevation (~+22%)
BOOKING_SURGE_LEAD_DAYS = 9          # surge peaks ~9 days before the holiday
BOOKING_SURGE_WIDTH_DAYS = 7         # Gaussian width -> spans ~2-3 weeks

# Layer 3 (IROP events) is configured just below and is unchanged.

# IROP (Irregular Operations) events: 12-18 per year, each 1-3 days, multiplying volume
# 1.8x-3.0x and shifting the reason mix toward rebookings. Events are kept apart by
# MIN_EVENT_GAP_DAYS so they read as distinct spikes.
IROP_EVENTS_MIN = 12
IROP_EVENTS_MAX = 18
IROP_DURATION_MIN_DAYS = 1
IROP_DURATION_MAX_DAYS = 3
IROP_MULT_MIN = 1.8
IROP_MULT_MAX = 3.0
MIN_EVENT_GAP_DAYS = 5

# Contact reasons and their share on a normal day (sums to 1.00).
REASONS = [
    "Booking_Change", "IROP_Rebooking", "Refunds", "SkyMiles",
    "Baggage", "Seat_Upgrade", "General_Inquiry",
]
NORMAL_REASON_PROBS = np.array([0.28, 0.08, 0.12, 0.14, 0.10, 0.13, 0.15])
IROP_REBOOK_SHARE = 0.45  # during IROP, IROP_Rebooking jumps to ~45%

# SkyMiles tiers and their share (sums to 1.00). Higher tiers get a small handle-time
# premium (more white-glove service).
SEGMENTS = ["General", "Silver", "Gold", "Platinum", "Diamond"]
SEGMENT_PROBS = np.array([0.60, 0.18, 0.12, 0.07, 0.03])
SEGMENT_MULT = np.array([1.00, 1.03, 1.06, 1.10, 1.15])

# Baseline mean handle time (AHT) per reason, in seconds, for a General-tier contact;
# higher SkyMiles tiers add a small premium via SEGMENT_MULT (so the realized population
# mean is ~2.4% above these). Drawn from a lognormal so durations are right-skewed (most
# calls short, a few very long). IROP rebookings are the longest.
REASON_MEAN_SECONDS = {
    "Booking_Change": 300, "IROP_Rebooking": 420, "Refunds": 330, "SkyMiles": 240,
    "Baggage": 270, "Seat_Upgrade": 210, "General_Inquiry": 150,
}
HANDLE_TIME_SIGMA = 0.5            # lognormal shape (same skew for every reason)
HANDLE_TIME_MIN = 30              # clip floor (seconds)
HANDLE_TIME_MAX = 3600           # clip ceiling (seconds)

# Behavioural rates (normal day, IROP day). Repeats and abandons rise during disruption.
REPEAT_RATE = (0.08, 0.18)
RESOLVE_RATE = (0.85, 0.68)       # first-contact resolution falls during IROP
ABANDON_RATE = (0.03, 0.12)

# Stable Monday-first weekday order for the day_of_week category.
DAYS_OF_WEEK_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
                      "Saturday", "Sunday"]

# Final column order (one row = one contact).
COLUMNS = [
    "contact_id", "timestamp", "date", "interval_start", "hour", "day_of_week",
    "contact_reason", "customer_segment", "handle_time_seconds", "irop_flag",
    "resolved_first_contact", "repeat_contact", "abandoned",
]


# --------------------------------------------------------------------------------------
# Building blocks
# --------------------------------------------------------------------------------------
def _intraday_weights(n_intervals: int) -> np.ndarray:
    """Normalized weight per half-hour interval: very low overnight, rising from ~6am,
    twin peaks late morning (~10-11am) and late afternoon (~4-6pm)."""
    hours = np.arange(n_intervals) * (config.INTERVAL_MINUTES / 60.0)
    morning = 0.60 * np.exp(-((hours - 10.5) ** 2) / (2 * 1.8 ** 2))
    afternoon = 0.80 * np.exp(-((hours - 16.5) ** 2) / (2 * 2.2 ** 2))
    midday = 0.15 * np.exp(-((hours - 13.0) ** 2) / (2 * 1.5 ** 2))
    weights = morning + afternoon + midday + 0.005  # small overnight floor
    return weights / weights.sum()


def _seasonal_factor(dates: pd.DatetimeIndex) -> np.ndarray:
    """Layer 1: smooth, mean-normalized seasonal curve (broad summer plateau + gentle
    year-end rise, deepest trough late Jan / early Feb). The flat-topped summer term is a
    super-Gaussian (^4). Mean-normalized to 1.0 so its amplitude never shifts the annual
    total (the row-count test band holds for any amplitude)."""
    doy = dates.dayofyear.to_numpy().astype(float)
    summer = np.exp(-((np.abs(doy - SEASONAL_SUMMER_CENTER_DOY) / SEASONAL_SUMMER_WIDTH) ** 4))
    yearend = np.exp(-(((doy - SEASONAL_YEAREND_CENTER_DOY) / SEASONAL_YEAREND_WIDTH) ** 2))
    winter = np.exp(-(((doy - SEASONAL_WINTER_CENTER_DOY) / SEASONAL_WINTER_WIDTH) ** 2))
    curve = (SEASONAL_BASE + SEASONAL_SUMMER_AMP * summer
             + SEASONAL_YEAREND_AMP * yearend - SEASONAL_WINTER_AMP * winter)
    return curve / curve.mean()


def _thanksgiving(dates: pd.DatetimeIndex, year: int):
    """US Thanksgiving (4th Thursday of November) for `year`, or None if Nov isn't covered."""
    nov_thu = dates[(dates.year == year) & (dates.month == 11) & (dates.dayofweek == 3)]
    return nov_thu[3] if len(nov_thu) >= 4 else None


def _holiday_factor(dates: pd.DatetimeIndex):
    """Layer 2: discrete, calendar-fixed holiday-travel multipliers (volume only).

    Returns (factor, peak_mask): `factor` multiplies daily volume; `peak_mask` flags the
    heavy-travel spike days (for plotting). The quiet days (Thanksgiving / Christmas Day)
    are not peaks. Robust to partial or multi-year date ranges."""
    factor = np.ones(len(dates))
    peak = np.zeros(len(dates), dtype=bool)
    pos = {ts: i for i, ts in enumerate(dates.normalize())}

    def setf(ts, mult, is_peak):
        i = pos.get(pd.Timestamp(ts).normalize())
        if i is not None:
            factor[i] = mult
            peak[i] = peak[i] or is_peak

    for year in pd.unique(dates.year):
        tg = _thanksgiving(dates, int(year))
        if tg is not None:
            setf(tg - pd.Timedelta(days=1), THANKSGIVING_WED_MULT, True)
            setf(tg, THANKSGIVING_DAY_MULT, False)
            setf(tg + pd.Timedelta(days=3), THANKSGIVING_SUN_MULT, True)
        for day, mult in CHRISTMAS_MULTS.items():
            setf(pd.Timestamp(int(year), 12, day), mult, mult > 1.0)
    return factor, peak


def _booking_surge_factor(dates: pd.DatetimeIndex) -> np.ndarray:
    """Layer 4: smooth multi-week PRE-HOLIDAY booking surge (contacts, not passengers).

    Holiday travel is pre-booked, so booking/change contacts swell in the ~2-3 weeks BEFORE
    Thanksgiving and Christmas — the contact peak leads the travel-day passenger peak. A
    Gaussian centred BOOKING_SURGE_LEAD_DAYS before each anchor, using signed day-distance
    from the real anchor date (robust across years). Scales volume only; returns a >= 1.0
    multiplier. Not mean-normalized — it intentionally adds real pre-holiday contact volume."""
    factor = np.ones(len(dates))
    for year in pd.unique(dates.year):
        anchors = []
        tg = _thanksgiving(dates, int(year))
        if tg is not None:
            anchors.append(tg)
        anchors.append(pd.Timestamp(int(year), 12, 25))   # Christmas
        for anchor in anchors:
            center = anchor - pd.Timedelta(days=BOOKING_SURGE_LEAD_DAYS)
            delta = ((dates - center) / pd.Timedelta(days=1)).to_numpy().astype(float)
            factor += BOOKING_SURGE_AMP * np.exp(-((delta / BOOKING_SURGE_WIDTH_DAYS) ** 2))
    return factor


def _irop_reason_probs() -> np.ndarray:
    """Reason distribution during IROP: IROP_Rebooking at IROP_REBOOK_SHARE, the rest
    scaled down proportionally to their normal weights."""
    probs = NORMAL_REASON_PROBS.copy()
    rebook_idx = REASONS.index("IROP_Rebooking")
    others = np.ones(len(REASONS), dtype=bool)
    others[rebook_idx] = False
    scale = (1.0 - IROP_REBOOK_SHARE) / NORMAL_REASON_PROBS[others].sum()
    probs[others] = NORMAL_REASON_PROBS[others] * scale
    probs[rebook_idx] = IROP_REBOOK_SHARE
    return probs


def _place_irop_events(rng: np.random.Generator, sim_days: int):
    """Return (irop_factor[sim_days], irop_day[sim_days] bool) with non-overlapping,
    well-separated IROP events."""
    irop_factor = np.ones(sim_days, dtype=float)
    irop_day = np.zeros(sim_days, dtype=bool)
    occupied = np.zeros(sim_days, dtype=bool)  # event footprint + gap, prevents merges

    n_events = int(rng.integers(IROP_EVENTS_MIN, IROP_EVENTS_MAX + 1))
    placed, attempts = 0, 0
    while placed < n_events and attempts < 5000:
        attempts += 1
        dur = int(rng.integers(IROP_DURATION_MIN_DAYS, IROP_DURATION_MAX_DAYS + 1))
        high = sim_days - dur + 1   # +1: integers() high is exclusive, so the last valid
        if high <= 0:              # start day (sim_days - dur) stays reachable
            break
        start = int(rng.integers(0, high))
        end = start + dur
        lo = max(0, start - MIN_EVENT_GAP_DAYS)
        hi = min(sim_days, end + MIN_EVENT_GAP_DAYS)
        if occupied[lo:hi].any():
            continue
        mult = float(rng.uniform(IROP_MULT_MIN, IROP_MULT_MAX))
        irop_factor[start:end] = mult
        irop_day[start:end] = True
        occupied[lo:hi] = True
        placed += 1
    return irop_factor, irop_day


# --------------------------------------------------------------------------------------
# Core generator
# --------------------------------------------------------------------------------------
def generate_contacts(
    seed: int = config.SEED,
    sim_start: str = config.SIM_START,
    sim_days: int = config.SIM_DAYS,
    base_daily: int = config.BASE_DAILY_CONTACTS,
) -> pd.DataFrame:
    """Generate the full synthetic contact dataset as a tidy DataFrame.

    Layered build (per spec): daily totals -> intraday shape -> Poisson per-interval
    counts -> one row per contact with reason / segment / handle time / flags.
    """
    rng = np.random.default_rng(seed)
    n_intervals = INTERVALS_PER_DAY

    # --- day-level demand drivers ---------------------------------------------------
    dates = pd.date_range(sim_start, periods=sim_days, freq="D")
    dow = dates.dayofweek.to_numpy()
    dow_name = dates.day_name().to_numpy().astype(object)

    dow_factor = np.array([DOW_FACTOR[d] for d in dow])
    seasonal_factor = _seasonal_factor(dates)                  # Layer 1 (smooth curve)
    holiday_factor, _ = _holiday_factor(dates)                 # Layer 2 (discrete day-of spikes)
    booking_factor = _booking_surge_factor(dates)              # Layer 4 (pre-holiday booking)
    irop_factor, irop_day = _place_irop_events(rng, sim_days)  # Layer 3 (disruptions)

    daily_expected = (base_daily * dow_factor * seasonal_factor
                      * booking_factor * holiday_factor * irop_factor)   # (D,)

    # --- expected volume per (day, interval) and Poisson actuals --------------------
    weights = _intraday_weights(n_intervals)                       # (I,)
    expected = daily_expected[:, None] * weights[None, :]          # (D, I)
    counts = rng.poisson(expected)                                 # (D, I) integer
    counts_flat = counts.ravel()                                   # C-order: day outer
    total = int(counts_flat.sum())

    # --- cell-level attributes (length D*I), then expand to one row per contact -----
    base_epoch = np.datetime64(pd.Timestamp(sim_start)).astype("datetime64[s]").astype(np.int64)
    day_idx = np.repeat(np.arange(sim_days), n_intervals)
    interval_idx = np.tile(np.arange(n_intervals), sim_days)
    cell_start_sec = base_epoch + day_idx * 86400 + interval_idx * INTERVAL_SECONDS
    cell_irop = np.repeat(irop_day, n_intervals)
    cell_dow = np.repeat(dow_name, n_intervals)

    c_start_sec = np.repeat(cell_start_sec, counts_flat)
    c_irop = np.repeat(cell_irop, counts_flat)
    c_dow = np.repeat(cell_dow, counts_flat)
    del cell_start_sec, cell_irop, cell_dow, day_idx, interval_idx

    # --- timestamps: place each contact at a random second within its interval ------
    offset = rng.integers(0, INTERVAL_SECONDS, size=total)
    c_ts_sec = c_start_sec + offset
    timestamp = c_ts_sec.astype("datetime64[s]")
    interval_start = c_start_sec.astype("datetime64[s]")
    date_str = c_start_sec.astype("datetime64[s]").astype("datetime64[D]").astype(str)
    hour = ((c_ts_sec % 86400) // 3600).astype(np.int16)

    # --- reasons: normal vs IROP distribution ---------------------------------------
    reason_code = np.empty(total, dtype=np.int8)
    norm_mask = ~c_irop
    n_norm = int(norm_mask.sum())
    n_irop = total - n_norm
    reason_code[norm_mask] = rng.choice(len(REASONS), size=n_norm, p=NORMAL_REASON_PROBS)
    reason_code[c_irop] = rng.choice(len(REASONS), size=n_irop, p=_irop_reason_probs())

    # --- segments --------------------------------------------------------------------
    segment_code = rng.choice(len(SEGMENTS), size=total, p=SEGMENT_PROBS)

    # --- handle time: lognormal per reason, x segment premium, clipped --------------
    reason_mean = np.array([REASON_MEAN_SECONDS[r] for r in REASONS])
    mu_log = np.log(reason_mean) - HANDLE_TIME_SIGMA ** 2 / 2.0     # so E[X] = mean
    mu_arr = mu_log[reason_code]
    handle = rng.lognormal(mean=mu_arr, sigma=HANDLE_TIME_SIGMA)
    handle *= SEGMENT_MULT[segment_code]
    handle = np.clip(np.rint(handle), HANDLE_TIME_MIN, HANDLE_TIME_MAX).astype(np.int32)

    # --- behavioural flags (rates differ on IROP days; coupled so no impossible rows) ---
    repeat_p = np.where(c_irop, REPEAT_RATE[1], REPEAT_RATE[0])
    resolve_p = np.where(c_irop, RESOLVE_RATE[1], RESOLVE_RATE[0])
    abandon_p = np.where(c_irop, ABANDON_RATE[1], ABANDON_RATE[0])
    abandoned = rng.random(total) < abandon_p
    repeat_contact = rng.random(total) < repeat_p
    # First-contact resolution only applies to answered, non-repeat contacts, so an
    # abandoned call or a follow-up is never marked resolved (spec: set it "accordingly").
    # FCR still falls on IROP days via the lower RESOLVE_RATE.
    resolved_first_contact = (rng.random(total) < resolve_p) & ~abandoned & ~repeat_contact

    # --- ids -------------------------------------------------------------------------
    # astype(str) sizes the dtype to the actual integer width (no silent truncation past
    # 9,999,999 rows); zfill keeps a minimum 7-digit pad -> CT-0000001.
    seq = np.arange(1, total + 1)
    contact_id = np.char.add("CT-", np.char.zfill(seq.astype(str), 7))

    df = pd.DataFrame({
        "contact_id": contact_id,
        "timestamp": timestamp,
        "date": pd.Categorical(date_str),
        "interval_start": interval_start,
        "hour": hour,
        "day_of_week": pd.Categorical(c_dow, categories=DAYS_OF_WEEK_ORDER, ordered=True),
        "contact_reason": pd.Categorical.from_codes(reason_code, categories=REASONS),
        "customer_segment": pd.Categorical.from_codes(segment_code, categories=SEGMENTS),
        "handle_time_seconds": handle,
        "irop_flag": c_irop,
        "resolved_first_contact": resolved_first_contact,
        "repeat_contact": repeat_contact,
        "abandoned": abandoned,
    })
    return df[COLUMNS]


# --------------------------------------------------------------------------------------
# IO / reporting helpers
# --------------------------------------------------------------------------------------
def write_contacts(df: pd.DataFrame, path: Path = RAW_CSV_PATH) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return path


def count_irop_events(df: pd.DataFrame) -> int:
    """Count distinct IROP events = runs of consecutive IROP dates separated by gaps."""
    irop_dates = df.loc[df["irop_flag"] == True, "date"]  # noqa: E712 (works on bool & str)
    if len(irop_dates) == 0:
        return 0
    days = pd.to_datetime(pd.Index(irop_dates.unique())).sort_values()
    if len(days) == 1:
        return 1
    gaps = days.to_series().diff().dt.days.to_numpy()[1:]
    return int((gaps > 1).sum() + 1)


def summarize(df: pd.DataFrame) -> None:
    """Print the Phase-1 DoD summary: totals, per-day distribution, reason mix, IROP."""
    n = len(df)
    daily = df.groupby("date", observed=True).size()
    n_events = count_irop_events(df)
    irop_rows = int((df["irop_flag"] == True).sum())  # noqa: E712

    print("=" * 64)
    print("SYNTHETIC CONTACT DATASET — SUMMARY")
    print("=" * 64)
    print(f"Total contacts            : {n:,}")
    print(f"Days covered              : {daily.size}")
    print(f"Contacts/day  mean        : {daily.mean():,.0f}")
    print(f"              median      : {daily.median():,.0f}")
    print(f"              min / max   : {daily.min():,.0f} / {daily.max():,.0f}")
    print(f"              std         : {daily.std():,.0f}")
    print()
    print("Contact reason mix (% of all contacts):")
    reason_pct = (df["contact_reason"].value_counts(normalize=True) * 100).round(2)
    for reason, pct in reason_pct.items():
        print(f"   {reason:<16} {pct:6.2f}%")
    print(f"   {'TOTAL':<16} {reason_pct.sum():6.2f}%")
    print()
    print("Customer segment mix (% of all contacts):")
    seg_pct = (df["customer_segment"].value_counts(normalize=True) * 100).round(2)
    for seg, pct in seg_pct.items():
        print(f"   {seg:<16} {pct:6.2f}%")
    print()
    print(f"IROP events detected      : {n_events}")
    print(f"IROP-flagged contacts     : {irop_rows:,} ({100*irop_rows/n:.1f}% of total)")
    print(f"Mean handle time (s)      : {df['handle_time_seconds'].mean():.1f}")

    # Winter holiday-travel peaks (Layer 2) — for comparing December against real data.
    daily_dt = df.groupby("date", observed=True).size()
    daily_dt.index = pd.to_datetime(daily_dt.index)
    daily_dt = daily_dt.sort_index()
    _, holiday_peak = _holiday_factor(pd.DatetimeIndex(daily_dt.index))
    peak_dates = daily_dt.index[holiday_peak]
    if len(peak_dates):
        print()
        print("Holiday-travel peaks (volume only, normal reason mix):")
        for d in peak_dates:
            print(f"   {d.date()} ({d.day_name()[:3]}): {int(daily_dt.loc[d]):>7,} contacts")
    print("=" * 64)


def plot_overview(df: pd.DataFrame, path: Path = FIG_PATH) -> Path:
    """Save a daily-volume-over-the-year sanity plot with IROP days highlighted."""
    import matplotlib
    matplotlib.use("Agg")  # headless / no display
    import matplotlib.pyplot as plt

    daily = df.groupby("date", observed=True).size()
    daily.index = pd.to_datetime(daily.index)
    daily = daily.sort_index()
    irop_by_day = df.groupby("date", observed=True)["irop_flag"].any()
    irop_by_day.index = pd.to_datetime(irop_by_day.index)
    irop_by_day = irop_by_day.sort_index().reindex(daily.index).fillna(False)
    _, holiday_peak = _holiday_factor(pd.DatetimeIndex(daily.index))

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(daily.index, daily.values, color="#1b2a4a", lw=1.0, label="Daily contacts")
    hol_idx = daily.index[holiday_peak]
    ax.scatter(hol_idx, daily.loc[hol_idx].values, color="#d4a017", s=30, marker="D",
               zorder=4, label="Holiday-travel peak")
    irop_idx = daily.index[irop_by_day.to_numpy().astype(bool)]
    ax.scatter(irop_idx, daily.loc[irop_idx].values, color="#c8102e", s=18, zorder=5,
               label="IROP day")
    ax.set_title("ATL Reservations — synthetic daily contact volume (one year)")
    ax.set_xlabel("Date")
    ax.set_ylabel("Contacts per day")
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=120)
    plt.close(fig)
    return path


def main() -> None:
    print("Generating synthetic contact dataset (seed="
          f"{config.SEED}, {config.SIM_DAYS} days)...")
    df = generate_contacts()
    out = write_contacts(df)
    print(f"Wrote {len(df):,} rows -> {out.relative_to(PROJECT_ROOT)}")
    fig = plot_overview(df)
    print(f"Saved sanity plot -> {fig.relative_to(PROJECT_ROOT)}")
    print()
    summarize(df)


if __name__ == "__main__":
    main()
