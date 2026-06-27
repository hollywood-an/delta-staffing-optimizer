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

# ----------------------------------------------------------------------------
# Phase 5 — business-case ASSUMPTIONS (every value here is an assumption;
# outputs label them as such so a reviewer can change one number and re-run).
# ----------------------------------------------------------------------------
# Self-service deflection: a share of app-handleable contact types shifts to
# the Fly Delta app, removing that volume from the phone queue.
DEFLECTABLE_REASONS = ["Booking_Change", "Seat_Upgrade", "SkyMiles"]  # ASSUMPTION
DEFLECTION_RATE = 0.20        # ASSUMPTION: 20% of deflectable contacts self-serve

# IROP stress scenario: a major disruption day multiplies contact volume and
# lengthens handle time (the reason mix shifts toward longer IROP_Rebooking).
IROP_VOLUME_MULTIPLIER = 2.5  # ASSUMPTION: ~2.5x contact volume on an IROP day

# Realistic (less-naive) staffing baseline: a center running fixed shifts staffs each
# shift to its peak interval (no intraday flexing) — the "vs realistic" savings comparison,
# distinct from the naive "peak headcount every interval, 24/7" baseline.
REALISTIC_BASELINE_BLOCK_HOURS = 8  # ASSUMPTION: 8h shift blocks for the realistic baseline
