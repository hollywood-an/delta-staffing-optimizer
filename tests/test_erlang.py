"""Phase 3 — Erlang C correctness tests (the core math gate).

Pure deterministic math with known-correct answers, so these are exact-value assertions
(unlike the data/forecast phases). They reproduce the verified table in FLAGSHIP_SPEC.md
Phase 3 and cover every assertion the spec requires.
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.erlang import (  # noqa: E402
    erlang_c, service_level, average_speed_of_answer, occupancy,
    offered_load_erlangs, agents_required, apply_shrinkage,
)

LOAD, AHT, TARGET = 5.0, 180.0, 20.0

# Verified table (FLAGSHIP_SPEC Phase 3): agents -> (SL@20s, ASA seconds, occupancy).
VERIFIED = {
    6: (0.474, 105.8, 0.833),
    7: (0.740, 29.2, 0.714),
    8: (0.880, 10.0, 0.625),
    9: (0.948, 3.6, 0.556),
}


def test_agents_required_matches_spec():
    assert agents_required(LOAD, AHT, TARGET, 0.80) == 8


def test_service_level_at_8_within_001_of_088():
    assert abs(service_level(8, LOAD, AHT, TARGET) - 0.88) < 0.01


def test_verified_table_reproduced():
    for n, (sl, asa, occ) in VERIFIED.items():
        assert abs(service_level(n, LOAD, AHT, TARGET) - sl) < 0.005, f"SL@{n} agents"
        assert abs(average_speed_of_answer(n, LOAD, AHT) - asa) < 0.5, f"ASA@{n} agents"
        assert abs(occupancy(n, LOAD) - occ) < 0.005, f"occupancy@{n} agents"


def test_erlang_c_in_unit_interval():
    for n in range(6, 20):                       # several n > load (= 5)
        assert 0.0 <= erlang_c(n, LOAD) <= 1.0


def test_service_level_monotonic_increasing_in_agents():
    sls = [service_level(n, LOAD, AHT, TARGET) for n in range(6, 20)]
    assert all(later > earlier for earlier, later in zip(sls, sls[1:]))


def test_agents_required_zero_when_load_zero():
    assert agents_required(0.0, AHT, TARGET, 0.80) == 0


def test_adding_agents_lowers_asa_and_occupancy():
    asas = [average_speed_of_answer(n, LOAD, AHT) for n in range(6, 20)]
    occs = [occupancy(n, LOAD) for n in range(6, 20)]
    assert all(later < earlier for earlier, later in zip(asas, asas[1:]))
    assert all(later < earlier for earlier, later in zip(occs, occs[1:]))


def test_offered_load_erlangs_matches_glossary_example():
    # 50 calls of 180s in a 1800s (30-min) interval = 5 Erlangs (spec glossary).
    assert abs(offered_load_erlangs(50, 180, 1800) - 5.0) < 1e-9


def test_apply_shrinkage_grosses_up_headcount():
    # Needing 8 working agents at 30% shrinkage -> ceil(8 / 0.70) = 12 scheduled.
    assert apply_shrinkage(8, 0.30) == 12


def test_unstable_when_agents_not_greater_than_load():
    assert erlang_c(5, 5.0) == 1.0                       # n <= A -> everyone queues
    assert service_level(5, 5.0, AHT, TARGET) == 0.0
    assert average_speed_of_answer(5, 5.0, AHT) == float("inf")
