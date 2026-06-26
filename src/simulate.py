"""Phase 4 — validate the Erlang C math with a SimPy discrete-event simulation.

We imitate one staffing interval as an **M/M/c queue**: contacts arrive as a Poisson process
(exponential gaps) at rate lambda = offered_load / AHT; `c` agents each serve at a mean AHT
(exponential service); contacts that find every agent busy wait in a FIFO queue. We record
each contact's wait, then compare the *measured* service level against the *predicted*
service level from src/erlang.py across several load scenarios.

Why exponential service (not the lognormal of Phase 1)? Erlang C assumes M/M/c — Markovian
(memoryless, i.e. exponential) service. To validate the FORMULA on its own terms the sim must
use the same assumption. Agreement means "two independent methods give the same answer," which
is the strongest claim in the deck. (A real-world sim would use lognormal service + abandonment;
the optional `patience_seconds` arg turns this into an Erlang-A model for that discussion.)

Run:  python -m src.simulate
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import simpy

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402
from src.erlang import service_level, average_speed_of_answer, occupancy  # noqa: E402

FIG_PATH = PROJECT_ROOT / "outputs" / "figures" / "validation.png"
DEFAULT_AHT = 180.0
DEFAULT_TARGET = float(config.TARGET_ANSWER_SECONDS)   # 20s ("80/20")

# A single short run does not pin the measured SL tightly at high occupancy (queue waits are
# strongly autocorrelated, so the effective sample size << N: a lone 60k run lands the peak
# scenario outside 3pp on ~18% of seeds). We therefore AVERAGE several independent replications
# per scenario; the replication MEAN is compared to Erlang C and the spread (std) is reported
# as the simulation's uncertainty. (Per spec Section 3.5: raise the sample count if flaky,
# never loosen the tolerance.)
N_CONTACTS = 50_000      # contacts per replication
WARMUP = 10_000          # discard the empty-queue start-up transient
N_REPLICATIONS = 8       # independent runs averaged per scenario


@dataclass(frozen=True)
class Scenario:
    name: str
    offered_load: float    # Erlangs
    n_agents: int
    aht_seconds: float = DEFAULT_AHT
    target_seconds: float = DEFAULT_TARGET


# Low / medium / high / peak occupancy — all stable (agents > load) so Erlang C applies.
SCENARIOS = [
    Scenario("low",    2.0,  4),     # occupancy 0.50
    Scenario("medium", 8.0,  10),    # occupancy 0.80
    Scenario("high",   15.0, 18),    # occupancy 0.83
    Scenario("peak",   25.0, 29),    # occupancy 0.86
]


def simulate_interval(offered_load: float, n_agents: int, aht_seconds: float,
                      target_seconds: float, n_contacts: int = N_CONTACTS,
                      warmup: int = WARMUP, seed: int = config.SEED,
                      patience_seconds: float | None = None) -> dict:
    """Simulate an M/M/c queue and return measured {sim_sl, sim_asa, sim_occ, ...}.

    patience_seconds=None -> pure M/M/c (matches Erlang C). A value -> Erlang A: a contact
    abandons if its wait would exceed an exponential patience draw."""
    rng = np.random.default_rng(seed)
    env = simpy.Environment()
    agents = simpy.Resource(env, capacity=n_agents)
    lam = offered_load / aht_seconds            # arrivals per second

    waits: list[tuple[int, float, bool]] = []   # (arrival_index, wait_seconds, answered)
    service_total = [0.0]                        # sum of service durations (for occupancy)
    n_abandoned = [0]

    def contact(idx: int, arrival: float):
        with agents.request() as req:
            if patience_seconds is None:
                yield req
                granted = True
            else:
                patience = rng.exponential(patience_seconds)
                results = yield req | env.timeout(patience)
                granted = req in results
            wait = env.now - arrival
            if granted:
                service = rng.exponential(aht_seconds)
                service_total[0] += service
                waits.append((idx, wait, True))
                yield env.timeout(service)
            else:                                # reneged: `with` cancels the queued request
                n_abandoned[0] += 1
                waits.append((idx, wait, False))

    def source():
        for i in range(n_contacts):
            yield env.timeout(rng.exponential(1.0 / lam))
            env.process(contact(i, env.now))

    env.process(source())
    env.run()

    measured = [(w, ans) for (i, w, ans) in waits if i >= warmup]
    w = np.array([wait for wait, _ in measured])
    answered = np.array([ans for _, ans in measured], dtype=bool)
    in_sl = (w <= target_seconds) & answered     # answered within target
    return {
        "offered_load": offered_load,
        "n_agents": n_agents,
        "sim_sl": float(np.mean(in_sl)),
        "sim_asa": float(np.mean(w[answered])) if answered.any() else float("inf"),
        "sim_occ": float(service_total[0] / (n_agents * env.now)),
        "n_measured": len(measured),
        "abandon_rate": n_abandoned[0] / len(waits) if waits else 0.0,
    }


def simulate_scenario(sc: "Scenario", n_contacts: int = N_CONTACTS, warmup: int = WARMUP,
                      n_replications: int = N_REPLICATIONS,
                      base_seed: int = config.SEED) -> dict:
    """Average n_replications independent M/M/c runs of `sc`; return the replication mean and
    spread. Comparing the MEAN (not a single noisy run) to Erlang C is what makes the
    agreement robust to the seed at high occupancy."""
    sls, asas, occs = [], [], []
    for r in range(n_replications):
        sim = simulate_interval(sc.offered_load, sc.n_agents, sc.aht_seconds, sc.target_seconds,
                                n_contacts=n_contacts, warmup=warmup, seed=base_seed + 1000 * r)
        sls.append(sim["sim_sl"])
        asas.append(sim["sim_asa"])
        occs.append(sim["sim_occ"])
    sls = np.array(sls)
    return {
        "sim_sl": float(sls.mean()),
        "sim_sl_std": float(sls.std(ddof=1)),                       # per-replication spread
        "sim_sl_se": float(sls.std(ddof=1) / np.sqrt(n_replications)),  # uncertainty of the mean
        "sim_asa": float(np.mean(asas)),
        "sim_occ": float(np.mean(occs)),
        "n_replications": n_replications,
        "n_measured_total": n_replications * (n_contacts - warmup),
    }


def validate(scenarios=SCENARIOS, n_contacts: int = N_CONTACTS, warmup: int = WARMUP,
             n_replications: int = N_REPLICATIONS, base_seed: int = config.SEED,
             verbose: bool = True) -> list[dict]:
    """Run each scenario (averaged over replications) and compare the measured mean service
    level against the Erlang-C prediction."""
    rows = []
    for k, sc in enumerate(scenarios):
        sim = simulate_scenario(sc, n_contacts, warmup, n_replications, base_seed=base_seed + k)
        rows.append({
            "name": sc.name, "load": sc.offered_load, "agents": sc.n_agents,
            "pred_sl": service_level(sc.n_agents, sc.offered_load, sc.aht_seconds, sc.target_seconds),
            "sim_sl": sim["sim_sl"], "sim_sl_std": sim["sim_sl_std"], "sim_sl_se": sim["sim_sl_se"],
            "pred_asa": average_speed_of_answer(sc.n_agents, sc.offered_load, sc.aht_seconds),
            "sim_asa": sim["sim_asa"],
            "pred_occ": occupancy(sc.n_agents, sc.offered_load),
            "sim_occ": sim["sim_occ"],
            "n": sim["n_measured_total"], "reps": sim["n_replications"],
        })
    for r in rows:
        r["sl_diff"] = abs(r["sim_sl"] - r["pred_sl"])
    if verbose:
        _print_table(rows)
    return rows


def _print_table(rows: list[dict]) -> None:
    reps = rows[0].get("reps", 1) if rows else 1
    per_rep = (rows[0]["n"] // reps) if rows else 0
    width = 84
    print("=" * width)
    print(f"ERLANG C VALIDATION — simulated M/M/c ({reps} reps x {per_rep:,} contacts) "
          f"vs predicted SL")
    print("=" * width)
    print(f"{'scenario':>8} {'load/agents':>11} | {'pred SL':>7}  {'sim SL (mean+/-SE)':>19}  "
          f"{'|diff|':>6} | {'ASA pred/sim':>14} | {'occ pred/sim':>13}")
    print("-" * width)
    for r in rows:
        print(f"{r['name']:>8} {r['load']:>4.0f}E /{r['agents']:>3}a | "
              f"{r['pred_sl']*100:>6.1f}%  {r['sim_sl']*100:>6.1f}% +/-{r['sim_sl_se']*100:>4.2f}pp  "
              f"{r['sl_diff']*100:>5.1f}pp | {r['pred_asa']:>5.1f}/{r['sim_asa']:<5.1f}s | "
              f"{r['pred_occ']*100:>4.1f}/{r['sim_occ']*100:<4.1f}%")
    print("-" * width)
    worst = max(r["sl_diff"] for r in rows)
    print(f"Worst |mean sim SL - Erlang C|: {worst*100:.2f} pp "
          f"({'PASS' if worst < 0.03 else 'FAIL'} at 3pp tolerance) across {len(rows)} scenarios.")
    print("=" * width)


def plot_validation(rows: list[dict], path: Path = FIG_PATH) -> Path:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    names = [r["name"] for r in rows]
    x = np.arange(len(rows))
    width = 0.38
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(x - width / 2, [r["pred_sl"] * 100 for r in rows], width, color="#1b2a4a",
           label="Erlang C (predicted)")
    ax.bar(x + width / 2, [r["sim_sl"] * 100 for r in rows], width, color="#c8102e",
           yerr=[r.get("sim_sl_se", 0) * 100 for r in rows], capsize=4,
           error_kw={"ecolor": "#333", "lw": 1},
           label=f"Simulation (mean of {rows[0].get('reps', 1)} reps ± SE)")
    ax.axhline(config.TARGET_SL * 100, color="gray", ls=":", lw=1.0,
               label=f"{config.TARGET_SL*100:.0f}/{config.TARGET_ANSWER_SECONDS:.0f} target")
    for xi, r in zip(x, rows):
        ax.annotate(f"{r['sl_diff']*100:.1f}pp", (xi, max(r["pred_sl"], r["sim_sl"]) * 100 + 1),
                    ha="center", fontsize=8, color="#444")
    ax.set_xticks(x)
    ax.set_xticklabels([f"{n}\n({r['load']:.0f}E / {r['agents']}a)" for n, r in zip(names, rows)])
    ax.set_ylabel("Service level (% answered within target)")
    ax.set_title("Erlang C validated by simulation — predicted vs measured service level")
    ax.set_ylim(0, 105)
    ax.legend(loc="lower right", fontsize=8)
    ax.grid(True, axis="y", alpha=0.25)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=120)
    plt.close(fig)
    return path


def main() -> None:
    print(f"Simulating {len(SCENARIOS)} scenarios "
          f"({N_REPLICATIONS} reps x {N_CONTACTS:,} contacts, {WARMUP:,} warm-up each)...")
    rows = validate()
    fig = plot_validation(rows)
    print(f"Saved {fig.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
