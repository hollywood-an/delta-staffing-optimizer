"""Airflow DAG — the staffing pipeline on the Spark engine, one task per stage.

    generate_data ──► spark_etl ──► forecast ──► optimize ──► dashboard ──► build_deck
                                  erlang_check ──┘                             ▲
                                  simulate ────────────────────────────────────┘

The same stages as `python run_all.py --engine spark`, with the dependencies made explicit:
- generate_data -> spark_etl -> forecast -> optimize -> dashboard -> build_deck: each stage reads
  the files the previous one wrote (contacts.csv -> Spark aggregates -> forecast.csv ->
  staffing_plan.csv -> dashboard CSVs -> deck).
- erlang_check gates optimize: the verified Erlang C table is re-checked before any staffing is
  sized, and the run fails if the engine has drifted from it.
- simulate needs no data, so it starts immediately, in parallel; build_deck waits for it because
  the deck reads validation.csv.

Every task runs in the PROJECT's virtualenv (.venv) via @task.external_python: Airflow lives in
.venv-airflow and the pipeline's pinned stack in .venv, and they never share an environment. A task
body only imports a stage module and calls its existing entry point, so outputs land exactly where
run_all.py writes them. STAFFING_PYTHON overrides the interpreter.

Start it, trigger a run and find the logs: NOTES.md (Phase 3), or `make airflow-up` then
`make airflow-trigger` in a second terminal.
"""
from __future__ import annotations

import os
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

REPO = str(Path(__file__).resolve().parents[1])
PROJECT_PYTHON = os.environ.get("STAFFING_PYTHON", f"{REPO}/.venv/bin/python")


# Task bodies are shipped to PROJECT_PYTHON as source, so they import everything they need inside.
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


@dag(
    dag_id="staffing_pipeline",
    schedule=None,                  # on demand: `make airflow-trigger` or the UI's Trigger button
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,              # every run rewrites the same output files
    default_args={"retries": 0},    # a failure should surface, not be retried away
    tags=["staffing", "spark"],
    doc_md=__doc__,
)
def staffing_pipeline():
    spark = {"engine": "spark"}
    generate_data = stage.override(task_id="generate_data")(REPO, "generate_data")
    spark_etl = stage.override(task_id="spark_etl")(REPO, "spark_etl", "run")
    forecast = stage.override(task_id="forecast")(REPO, "forecast", kwargs=spark)
    gate = erlang_check(REPO)
    simulate = stage.override(task_id="simulate")(REPO, "simulate")
    optimize = stage.override(task_id="optimize")(REPO, "optimize", kwargs=spark)
    dashboard = stage.override(task_id="dashboard")(REPO, "dashboard", kwargs=spark)
    build_deck = stage.override(task_id="build_deck")(REPO, "build_deck")

    generate_data >> spark_etl >> forecast >> optimize >> dashboard >> build_deck
    gate >> optimize
    simulate >> build_deck


staffing_pipeline()
