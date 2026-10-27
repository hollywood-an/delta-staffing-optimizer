# Airflow orchestration for the staffing pipeline — the Spark + Airflow path only; plain
# `python run_all.py` needs none of this. Airflow lives in .venv-airflow; the DAG's tasks run in the
# pipeline's .venv. All Airflow state (SQLite DB, logs, generated login) stays in ./airflow_home.
#
#   make airflow-install   one-time: create .venv-airflow, install pinned Airflow, set up its DB
#   make airflow-up        start Airflow (UI: http://localhost:8080); leave it running, Ctrl-C stops
#   make airflow-trigger   from a second terminal: unpause + trigger a run of staffing_pipeline
#   make airflow-test      one-shot run of every task in this terminal (no scheduler or UI needed)
#
# Data-quality gates (Great Expectations) live in their own .venv-gx — they only read files:
#   make gx-install        one-time: create .venv-gx with the pinned validator stack
#   make validate          validate the files of the last pipeline run (all layers; Spark engine)

PYTHON311    ?= $(HOME)/.pyenv/versions/3.11.14/bin/python
GX_VENV      ?= .venv-gx
AIRFLOW_VENV ?= .venv-airflow
AIRFLOW      := $(AIRFLOW_VENV)/bin/airflow
DAG_ID       := staffing_pipeline

export AIRFLOW_HOME                 := $(CURDIR)/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER   := $(CURDIR)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES := False
# UI/API reachable from this laptop only (Airflow's default, 0.0.0.0, listens on every interface)
export AIRFLOW__API__HOST           := 127.0.0.1
# macOS: Airflow forks its task processes; these two settings stop the Objective-C runtime and the
# system proxy lookup from crashing a forked child (standard Airflow-on-macOS workarounds).
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY := YES
export no_proxy := *

.PHONY: airflow-install airflow-up airflow-trigger airflow-test gx-install validate

gx-install:
	$(PYTHON311) -m venv $(GX_VENV)
	$(GX_VENV)/bin/pip install -r requirements-gx.txt

validate:
	$(GX_VENV)/bin/python -m src.quality --engine spark

airflow-install:
	$(PYTHON311) -m venv $(AIRFLOW_VENV)
	$(AIRFLOW_VENV)/bin/pip install -r requirements-airflow.txt
	$(AIRFLOW) db migrate

# standalone starts its scheduler, API server, etc. by running `airflow`, so its bin/ goes on PATH.
airflow-up:
	PATH="$(CURDIR)/$(AIRFLOW_VENV)/bin:$$PATH" $(AIRFLOW) standalone

airflow-trigger:
	$(AIRFLOW) dags unpause $(DAG_ID)
	$(AIRFLOW) dags trigger $(DAG_ID)

airflow-test:
	$(AIRFLOW) dags test $(DAG_ID)
