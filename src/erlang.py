"""Phase 3 — Erlang C staffing engine (THE CORE).

This is the VERIFIED reference implementation copied VERBATIM from FLAGSHIP_SPEC.md Phase 3.
Per the spec's golden rule, the Erlang math is NOT re-derived or modified here — it has been
numerically verified. The `python -m src.erlang` demo and tests/test_erlang.py reproduce the
spec's verified table. Keep all times in seconds (AHT, target, interval) to avoid unit bugs.
"""
import math


def erlang_b(n_agents: int, offered_load: float) -> float:
    """Erlang B blocking probability via numerically stable recursion."""
    if offered_load <= 0:
        return 0.0
    B = 1.0  # B(0, A) = 1
    for k in range(1, n_agents + 1):
        B = (offered_load * B) / (k + offered_load * B)
    return B


def erlang_c(n_agents: int, offered_load: float) -> float:
    """Probability an arriving contact must wait (is queued)."""
    if n_agents <= offered_load:
        return 1.0  # unstable: never keeps up, everyone waits
    B = erlang_b(n_agents, offered_load)
    return (n_agents * B) / (n_agents - offered_load * (1 - B))


def service_level(n_agents: int, offered_load: float,
                  aht_seconds: float, target_seconds: float) -> float:
    """Fraction of contacts answered within target_seconds (e.g., 0.80 for 80/20)."""
    if n_agents <= offered_load:
        return 0.0
    C = erlang_c(n_agents, offered_load)
    exponent = -(n_agents - offered_load) * (target_seconds / aht_seconds)
    return 1.0 - C * math.exp(exponent)


def average_speed_of_answer(n_agents: int, offered_load: float,
                            aht_seconds: float) -> float:
    """Average wait (seconds) before answer."""
    if n_agents <= offered_load:
        return float("inf")
    C = erlang_c(n_agents, offered_load)
    return (C * aht_seconds) / (n_agents - offered_load)


def occupancy(n_agents: int, offered_load: float) -> float:
    """Fraction of time agents are busy."""
    if n_agents <= 0:
        return 1.0
    return offered_load / n_agents


def offered_load_erlangs(contacts_in_interval: float, aht_seconds: float,
                         interval_seconds: float) -> float:
    """Convert a contact count + AHT into offered load (Erlangs)."""
    return contacts_in_interval * aht_seconds / interval_seconds


def agents_required(offered_load: float, aht_seconds: float,
                    target_seconds: float, target_sl: float,
                    max_occupancy: float = 0.90) -> int:
    """Smallest agent count meeting BOTH the service-level and occupancy targets."""
    if offered_load <= 0:
        return 0
    n = max(1, math.floor(offered_load) + 1)
    while True:
        sl = service_level(n, offered_load, aht_seconds, target_seconds)
        occ = occupancy(n, offered_load)
        if sl >= target_sl and occ <= max_occupancy:
            return n
        n += 1


def apply_shrinkage(required_agents: int, shrinkage: float) -> int:
    """Gross up required agents to scheduled headcount to cover breaks/training."""
    return math.ceil(required_agents / (1.0 - shrinkage))


# --------------------------------------------------------------------------------------
# Demo — prints the spec's verified table so it can be eyeballed (Phase 3 DoD).
# Verified: for load=5 Erlangs, AHT=180s, target=20s -> agents_required = 8.
# --------------------------------------------------------------------------------------
def _demo() -> None:
    load, aht, target = 5.0, 180.0, 20.0
    print(f"Erlang C — verified table  (load={load:g} Erlangs, AHT={aht:g}s, target={target:g}s)")
    print(f"{'agents':>7} | {'SL@20s':>7} | {'ASA(s)':>7} | {'occupancy':>9}")
    print("-" * 42)
    for n in range(6, 10):
        sl = service_level(n, load, aht, target)
        asa = average_speed_of_answer(n, load, aht)
        occ = occupancy(n, load)
        print(f"{n:>7} | {sl * 100:>6.1f}% | {asa:>7.1f} | {occ * 100:>8.1f}%")
    req = agents_required(load, aht, target, 0.80)
    print("-" * 42)
    print(f"agents_required({load:g}, {aht:g}, {target:g}, 0.80) = {req}   "
          f"-> apply_shrinkage({req}, 0.30) = {apply_shrinkage(req, 0.30)} scheduled")


if __name__ == "__main__":
    _demo()
