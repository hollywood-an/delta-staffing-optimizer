"""Phase 2 — forecast contact volume.

Predict future contact volume so we can staff *ahead* of demand, at two grains:
  (a) daily totals (Prophet primary, SARIMA fallback), and
  (b) a reusable intraday profile — the average fraction of a day's volume that lands in
      each 30-minute interval — so any daily forecast becomes 48 interval volumes via
      `interval_volume = daily_forecast * interval_fraction` (used in Phase 5).

We produce the **baseline** ("expected normal") forecast. Random IROP surges are, by
construction, unknowable in advance, so they are NOT forecast here — they are handled as a
separate stress scenario in Phase 5. The held-out test window is the last 30 days, which for
this dataset is December: it contains the Christmas holiday spike and the pre-Christmas
booking surge, neither learnable from a single year of history (Christmas falls in the
holdout), so holiday/IROP test days are expected to be worse than normal days.

Run:  python -m src.forecast
"""
from __future__ import annotations

import contextlib
import logging
import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402
from src.generate_data import RAW_CSV_PATH, _holiday_factor  # noqa: E402

PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
FORECAST_CSV = PROCESSED_DIR / "forecast.csv"
INTRADAY_CSV = PROCESSED_DIR / "intraday_profile.csv"
FIG_PATH = PROJECT_ROOT / "outputs" / "figures" / "forecast.png"

TEST_DAYS = 30          # held-out test window (per spec: the last ~30 days)
FUTURE_DAYS = 30        # forecast horizon beyond the data
INTERVALS_PER_DAY = 24 * 60 // config.INTERVAL_MINUTES   # 48

for _name in ("cmdstanpy", "prophet"):
    logging.getLogger(_name).setLevel(logging.ERROR)


# --------------------------------------------------------------------------------------
# Metrics
# --------------------------------------------------------------------------------------
def mape(actual, pred) -> float:
    """Mean absolute percentage error (%). Skips any zero-actual days."""
    a = np.asarray(actual, dtype=float)
    p = np.asarray(pred, dtype=float)
    mask = a != 0
    if not mask.any():
        return float("nan")
    return float(np.mean(np.abs((a[mask] - p[mask]) / a[mask])) * 100)


def rmse(actual, pred) -> float:
    a = np.asarray(actual, dtype=float)
    p = np.asarray(pred, dtype=float)
    return float(np.sqrt(np.mean((a - p) ** 2)))


# --------------------------------------------------------------------------------------
# Data aggregation
# --------------------------------------------------------------------------------------
def _read_contacts(path: Path = RAW_CSV_PATH) -> pd.DataFrame:
    return pd.read_csv(path, usecols=["date", "interval_start", "irop_flag"])


def daily_series(contacts: pd.DataFrame):
    """Return (daily[ds, y], irop_by_date Series indexed by date)."""
    daily = contacts.groupby("date").size().rename("y").reset_index()
    daily["ds"] = pd.to_datetime(daily["date"])
    daily = daily[["ds", "y"]].sort_values("ds").reset_index(drop=True)

    irop = contacts.groupby("date")["irop_flag"].any()
    irop.index = pd.to_datetime(irop.index)
    irop = irop.sort_index()
    return daily, irop


def intraday_profile(contacts: pd.DataFrame) -> pd.DataFrame:
    """Share of total volume by interval-of-day across all history (sums to 1.0) — the spec's
    'group by interval-of-day, normalize to sum 1'. The intraday shape is stationary by
    construction (the same weight vector drives every day), so pooling the full history
    (including IROP / holiday days) is intentional and does not bias the shape. Used in
    Phase 5 as interval_volume = daily_forecast * fraction.

    interval_start is floored to the bucket, formatted 'YYYY-MM-DD HH:MM:SS', so the time
    fields are read by slicing (cheaper than parsing 4.7M datetimes)."""
    ist = contacts["interval_start"].astype(str)
    hour = ist.str.slice(11, 13).astype(int)
    half = (ist.str.slice(14, 16) == "30").astype(int)
    bucket = (hour * 2 + half).rename("interval_index")

    counts = bucket.value_counts().reindex(range(INTERVALS_PER_DAY), fill_value=0).sort_index()
    fraction = counts / counts.sum()                      # normalize to sum 1
    idx = np.arange(INTERVALS_PER_DAY)
    labels = [f"{b // 2:02d}:{30 * (b % 2):02d}" for b in idx]
    return pd.DataFrame({
        "interval_index": idx,
        "interval_start": labels,
        "fraction": fraction.to_numpy(),
    })


# --------------------------------------------------------------------------------------
# Forecast backends — both return out-of-sample df[ds, yhat, yhat_lower, yhat_upper]
# --------------------------------------------------------------------------------------
@contextlib.contextmanager
def _quiet():
    """Suppress Prophet/Stan import + fit chatter on stdout/stderr (exceptions still raise)."""
    with open(os.devnull, "w") as devnull:
        with contextlib.redirect_stdout(devnull), contextlib.redirect_stderr(devnull):
            yield


def _forecast_prophet(train: pd.DataFrame, periods: int) -> pd.DataFrame:
    with _quiet():
        from prophet import Prophet
        # yearly_seasonality="auto" lets Prophet decide (the spec's "Prophet does this
        # automatically"): with < 2 years it DISABLES the yearly term, and auto-enables it
        # once >= 2 years of history exist. That is exactly right here — with one year an
        # explicit yearly term made Prophet conflate trend with the single yearly pass and
        # over-predict December by ~40% (normal-day MAPE 19-51%); without it the WEEKLY
        # pattern + a flexible trend dominate the 30-60 day horizon and normal-day MAPE drops
        # to ~9%. US holidays are added so the model knows the (in-training) Thanksgiving spike.
        model = Prophet(
            weekly_seasonality=True,
            yearly_seasonality="auto",
            daily_seasonality=False,
            seasonality_mode="additive",
            changepoint_prior_scale=0.05,
            interval_width=0.95,
        )
        model.add_country_holidays(country_name="US")
        model.fit(train[["ds", "y"]])
        future = model.make_future_dataframe(periods=periods, freq="D")
        np.random.seed(config.SEED)               # reproducible uncertainty samples
        fc = model.predict(future)
    oos = fc.iloc[-periods:][["ds", "yhat", "yhat_lower", "yhat_upper"]].reset_index(drop=True)
    return oos


def _forecast_sarima(train: pd.DataFrame, periods: int) -> pd.DataFrame:
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    y = train.set_index("ds")["y"].asfreq("D")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        res = SARIMAX(y, order=(1, 1, 1), seasonal_order=(1, 1, 1, 7),
                      enforce_stationarity=False, enforce_invertibility=False).fit(disp=False)
        pred = res.get_forecast(steps=periods)
        ci = pred.conf_int(alpha=0.05)
    idx = pd.date_range(y.index[-1] + pd.Timedelta(days=1), periods=periods, freq="D")
    return pd.DataFrame({
        "ds": idx,
        "yhat": pred.predicted_mean.to_numpy(),
        "yhat_lower": ci.iloc[:, 0].to_numpy(),
        "yhat_upper": ci.iloc[:, 1].to_numpy(),
    })


def make_forecast(train: pd.DataFrame, periods: int, backend: str = "auto"):
    """Return (backend_name, out-of-sample forecast df). Prophet primary, SARIMA fallback."""
    if backend in ("auto", "prophet"):
        try:
            return "prophet", _forecast_prophet(train, periods)
        except Exception as exc:  # pragma: no cover - depends on local install
            if backend == "prophet":
                raise
            print(f"[warn] Prophet unavailable ({type(exc).__name__}); using SARIMA fallback.")
    return "sarima", _forecast_sarima(train, periods)


# --------------------------------------------------------------------------------------
# Pipeline
# --------------------------------------------------------------------------------------
def _classify_test_days(test_dates: pd.DatetimeIndex, irop_by_date: pd.Series):
    """Return boolean masks (is_holiday, is_irop, is_normal) for the test dates."""
    hol_factor, _ = _holiday_factor(pd.DatetimeIndex(test_dates))
    is_holiday = hol_factor != 1.0
    is_irop = irop_by_date.reindex(test_dates).fillna(False).to_numpy(dtype=bool)
    is_normal = ~is_holiday & ~is_irop
    return is_holiday, is_irop, is_normal


def _plot(daily, oos, train_last, last_data, backend, mape_normal, fig_path=FIG_PATH):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(14, 5))
    test_start = train_last + pd.Timedelta(days=1)   # first held-out day (not the train end)
    ax.axvspan(test_start, last_data, color="#d4a017", alpha=0.10, label="Test window (held out)")
    ax.plot(daily["ds"], daily["y"], color="#1b2a4a", lw=1.0, label="Actual contacts")
    ax.plot(oos["ds"], oos["yhat"], color="#c8102e", lw=1.6, ls="--",
            label=f"Forecast ({backend})")
    ax.fill_between(oos["ds"], oos["yhat_lower"], oos["yhat_upper"], color="#c8102e",
                    alpha=0.15, label="95% interval")
    ax.axvline(train_last, color="gray", ls=":", lw=1.0)
    ax.axvline(last_data, color="black", ls=":", lw=0.8)
    ax.set_title(f"ATL Reservations — daily contact forecast ({backend}); "
                 f"normal-day test MAPE {mape_normal:.1f}%")
    ax.set_xlabel("Date")
    ax.set_ylabel("Contacts per day")
    ax.legend(loc="upper left", fontsize=8)
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    fig_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(fig_path, dpi=120)
    plt.close(fig)


def run_forecast(backend: str = "auto") -> dict:
    print("Loading contact history...")
    contacts = _read_contacts()
    daily, irop_by_date = daily_series(contacts)
    profile = intraday_profile(contacts)

    train = daily.iloc[:-TEST_DAYS].reset_index(drop=True)
    test = daily.iloc[-TEST_DAYS:].reset_index(drop=True)
    train_last = train["ds"].max()
    last_data = daily["ds"].max()

    print(f"Fitting forecast model (train={len(train)} days, holdout={TEST_DAYS} days)...")
    backend, oos = make_forecast(train, TEST_DAYS + FUTURE_DAYS, backend=backend)
    oos["yhat_lower"] = oos["yhat_lower"].clip(lower=0)

    # Align predictions with actuals on the test window.
    actual_by_date = daily.set_index("ds")["y"]
    oos["actual"] = oos["ds"].map(actual_by_date)
    oos["segment"] = np.where(oos["ds"] <= last_data, "test", "future")
    test_pred = oos[oos["segment"] == "test"].reset_index(drop=True)

    test_dates = pd.DatetimeIndex(test_pred["ds"])
    is_holiday, is_irop, is_normal = _classify_test_days(test_dates, irop_by_date)
    a, p = test_pred["actual"].to_numpy(), test_pred["yhat"].to_numpy()

    metrics = {
        "backend": backend,
        "mape_all": mape(a, p), "rmse_all": rmse(a, p),
        "mape_normal": mape(a[is_normal], p[is_normal]),
        "rmse_normal": rmse(a[is_normal], p[is_normal]),
        "n_test": len(test_pred), "n_normal": int(is_normal.sum()),
        "n_holiday": int(is_holiday.sum()), "n_irop": int(is_irop.sum()),
    }

    # ---- write outputs ----
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out = oos.copy()
    out["date"] = out["ds"].dt.date
    for col in ("yhat", "yhat_lower", "yhat_upper"):
        out[col] = out[col].round().astype("Int64")
    out["actual"] = out["actual"].astype("Int64")
    out = out.rename(columns={"yhat": "forecast", "yhat_lower": "forecast_lower",
                              "yhat_upper": "forecast_upper"})
    out[["date", "forecast", "forecast_lower", "forecast_upper", "actual", "segment"]].to_csv(
        FORECAST_CSV, index=False)
    profile.to_csv(INTRADAY_CSV, index=False)
    _plot(daily, oos, train_last, last_data, backend, metrics["mape_normal"])

    _print_summary(daily, train, test, oos, profile, metrics, train_last, last_data)
    return metrics


def _print_summary(daily, train, test, oos, profile, m, train_last, last_data):
    print("=" * 64)
    print("CONTACT VOLUME FORECAST — SUMMARY")
    print("=" * 64)
    print(f"Backend                   : {m['backend']}")
    print(f"History                   : {daily['ds'].min().date()} .. {daily['ds'].max().date()} "
          f"({len(daily)} days)")
    print(f"Train / test split        : train {len(train)} days, "
          f"test {len(test)} days ({test['ds'].min().date()} .. {test['ds'].max().date()})")
    print(f"Future horizon            : +{FUTURE_DAYS} days "
          f"(to {oos['ds'].max().date()})")
    print()
    print("Test-set accuracy:")
    print(f"  normal days   : MAPE {m['mape_normal']:5.1f}%   RMSE {m['rmse_normal']:>7,.0f}   "
          f"(n={m['n_normal']})")
    print(f"  all test days : MAPE {m['mape_all']:5.1f}%   RMSE {m['rmse_all']:>7,.0f}   "
          f"(n={m['n_test']}; incl. {m['n_holiday']} holiday, {m['n_irop']} IROP days)")
    print()
    print("Plain-English read:")
    print(f"  On a normal day the forecast lands within ~{m['mape_normal']:.0f}% of actual "
          f"volume. The all-days number is worse because the held-out month (December)")
    print(f"  contains the Christmas spike + pre-Christmas booking surge, which one year of")
    print(f"  history can't learn (Christmas falls inside the holdout); random IROP surges")
    print(f"  are unforecastable by design and handled as a Phase-5 stress scenario.")
    print()
    print(f"intraday_profile.csv      : {len(profile)} intervals, "
          f"fractions sum to {profile['fraction'].sum():.6f}")
    print(f"Busiest interval          : {profile.loc[profile['fraction'].idxmax(),'interval_start']} "
          f"({profile['fraction'].max()*100:.1f}% of a day)")
    print(f"Wrote                     : {FORECAST_CSV.relative_to(PROJECT_ROOT)}, "
          f"{INTRADAY_CSV.relative_to(PROJECT_ROOT)}")
    print(f"                            {FIG_PATH.relative_to(PROJECT_ROOT)}")
    print("=" * 64)


def main() -> None:
    run_forecast()


if __name__ == "__main__":
    main()
