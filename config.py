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
