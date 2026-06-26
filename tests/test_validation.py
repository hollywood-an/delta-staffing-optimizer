"""Phase 4 — validation test: the SimPy M/M/c simulation agrees with Erlang C.

This is the runnable proof behind the strongest interview claim — "I validated the staffing
method two independent ways." For each load scenario it AVERAGES several independent M/M/c
replications and asserts the measured MEAN service level is within tolerance of the Erlang-C
PREDICTED service level.

Why replications? At high occupancy a single short run's SL is noisy (queue waits are highly
autocorrelated, so the effective sample size << N), and a lone run lands the peak scenario
outside 3pp on ~18% of seeds. Averaging replications shrinks that spread so the agreement is
robust to the seed (per Section 3.5: raise the sample count if flaky — never loosen the
tolerance). Constants are imported from src.simulate so the test and the reported table/figure
share one source of truth.
"""
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402
from src.erlang import service_level  # noqa: E402
from src.simulate import (  # noqa: E402
    SCENARIOS, simulate_scenario, N_CONTACTS, WARMUP, N_REPLICATIONS,
)

TOLERANCE = 0.03          # 3 percentage points (spec)


def test_at_least_three_scenarios():
    assert len(SCENARIOS) >= 3, "need >= 3 load scenarios (low/medium/high)"


@pytest.mark.parametrize("k,sc", list(enumerate(SCENARIOS)), ids=[s.name for s in SCENARIOS])
def test_simulation_agrees_with_erlang_c(k, sc):
    sim = simulate_scenario(
        sc, n_contacts=N_CONTACTS, warmup=WARMUP,
        n_replications=N_REPLICATIONS, base_seed=config.SEED + k,
    )
    predicted = service_level(sc.n_agents, sc.offered_load, sc.aht_seconds, sc.target_seconds)
    diff = abs(sim["sim_sl"] - predicted)
    assert diff < TOLERANCE, (
        f"{sc.name}: simulated mean SL {sim['sim_sl']:.3f} +/- {sim['sim_sl_std']:.3f} "
        f"vs Erlang C {predicted:.3f} (diff {diff*100:.2f}pp over {sim['n_replications']} "
        f"replications, tolerance {TOLERANCE*100:.0f}pp)"
    )
