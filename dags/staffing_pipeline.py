"""Airflow DAG — the staffing pipeline on the Spark engine, one task per stage, with data-quality gates.

    generate_data ──► spark_etl ──► validate_interim ──► forecast ──► optimize ──► dashboard ──► validate_outputs ──► build_deck
                                                      erlang_check ──┘                            ▲
                                                      simulate ───────────────────────────────────┘

The same stages as `python run_all.py --engine spark --validate`, with the dependencies explicit:
- generate_data -> spark_etl -> forecast -> optimize -> dashboard -> build_deck: each stage reads the
  files the previous one wrote (contacts.csv -> Spark aggregates -> forecast.csv ->
  staffing_plan.csv -> dashboard CSVs -> deck).
- validate_interim (raw + interim suites) gates forecast, and validate_outputs (processed + outputs
  suites, including cross-file consistency) gates build_deck. They run Great Expectations
  (src/quality.py) with retries=0, so a failed expectation stops everything downstream visibly.
- erlang_check gates optimize: the verified Erlang C table is re-checked before any staffing is sized.
- simulate needs no data, so it starts immediately; validate_outputs checks its validation.csv.

Lineage: every task's inlets and outlets are Airflow Assets derived from metadata/catalog.yaml (the
single source of truth for datasets), so the UI's asset graph shows dataset -> task -> dataset
lineage without a path being typed twice.

Pipeline tasks run in the project's virtualenv (.venv) and the gates in .venv-gx, both via
@task.external_python: Airflow, the pipeline and the validator never share an environment.
STAFFING_PYTHON / STAFFING_GX_PYTHON override the interpreters.

Start it, trigger a run and find the logs: NOTES.md, or `make airflow-up` then `make airflow-trigger`.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pendulum
from airflow.sdk import Asset, dag, task

REPO = str(Path(__file__).resolve().parents[1])
PROJECT_PYTHON = os.environ.get("STAFFING_PYTHON", f"{REPO}/.venv/bin/python")
GX_PYTHON = os.environ.get("STAFFING_GX_PYTHON", f"{REPO}/.venv-gx/bin/python")

if REPO not in sys.path:                 # src.catalog needs only the standard library and PyYAML
    sys.path.insert(0, REPO)
from src import catalog  # noqa: E402

CATALOG = catalog.load_valid(Path(REPO) / "metadata" / "catalog.yaml")
ASSETS = {d["id"]: Asset(name=d["id"], uri=f"file://{REPO}/{d['path']}", group="staffing_pipeline",
                         extra={"layer": d["layer"], "format": d["format"]})
          for d in CATALOG["datasets"]}
TASK_IO = catalog.task_io(CATALOG)


def lineage(task_id: str) -> dict:
    """The task's inlets and outlets as Assets, straight from the catalog."""
    io = TASK_IO[task_id]
    return {"task_id": task_id, "inlets": [ASSETS[i] for i in io["inlets"]],
            "outlets": [ASSETS[o] for o in io["outlets"]]}


# Task bodies are shipped to their interpreter as source, so they import everything inside.
@task.external_python(python=PROJECT_PYTHON, expect_airflow=False)
def stage(repo: str, module: str, entry: str = "main", kwargs: dict | None = None) -> None:
    """Import src.<module> in the project venv and call its entry point."""
    import importlib
    import sys

    sys.path.insert(0, repo)
    getattr(importlib.import_module(f"src.{module}"), entry)(**(kwargs or {}))


@task.external_python(python=PROJECT_PYTHON, expect_airflow=False)
def erlang_check(repo: str) -> None:
    """Print the spec's verified Erlang C table; fail the run if the engine no longer matches it."""
    import sys

    sys.path.insert(0, repo)
    from src import erlang

    erlang._demo()
    required = erlang.agents_required(5.0, 180.0, 20.0, 0.80)
    if required != 8:
        raise ValueError(f"Erlang C drifted: agents_required(5, 180, 20, 0.80) = {required}, "
                         "expected 8 (FLAGSHIP_SPEC.md Phase 3)")


@task.external_python(python=GX_PYTHON, expect_airflow=False)
def quality_gate(repo: str, suites: list) -> None:
    """Validate these Great Expectations suites against the files written so far; raise (failing
    the task and everything downstream) if a gating expectation fails."""
    import sys

    sys.path.insert(0, repo)
    from src import quality

    quality.gate(suites, root=repo)


@dag(
    dag_id="staffing_pipeline",
    schedule=None,                  # on demand: `make airflow-trigger` or the UI's Trigger button
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,              # every run rewrites the same output files
    default_args={"retries": 0},    # a failure should surface, not be retried away
    tags=["staffing", "spark", "data-quality"],
    doc_md=__doc__,
)
def staffing_pipeline():
    spark = {"engine": "spark"}
    generate_data = stage.override(**lineage("generate_data"))(REPO, "generate_data")
    spark_etl = stage.override(**lineage("spark_etl"))(REPO, "spark_etl", "run")
    validate_interim = quality_gate.override(**lineage("validate_interim"))(REPO, ["raw", "interim"])
    forecast = stage.override(**lineage("forecast"))(REPO, "forecast", kwargs=spark)
    gate = erlang_check.override(**lineage("erlang_check"))(REPO)
    simulate = stage.override(**lineage("simulate"))(REPO, "simulate")
    optimize = stage.override(**lineage("optimize"))(REPO, "optimize", kwargs=spark)
    dashboard = stage.override(**lineage("dashboard"))(REPO, "dashboard", kwargs=spark)
    validate_outputs = quality_gate.override(**lineage("validate_outputs"))(REPO, ["processed", "outputs"])
    build_deck = stage.override(**lineage("build_deck"))(REPO, "build_deck")

    (generate_data >> spark_etl >> validate_interim >> forecast >> optimize >> dashboard
     >> validate_outputs >> build_deck)
    gate >> optimize
    simulate >> validate_outputs


staffing_pipeline()
