"""Data catalog — valid, true to the real files, true to the code, and true to the Airflow DAG.

- The catalog validates, and its constants and allowed-value lists match the code they mirror.
- Schema drift: every column in every real file the pipeline produces is in the catalog with a
  matching type and nullability, and vice versa. Committed files are read in place; contacts.csv is
  sampled; the interim layer comes from the shared full-data Spark ETL run (conftest.py).
- Lineage: every dataset has a producer and is consumed or marked terminal; the graph is acyclic.
- The catalog's tasks are exactly the DAG's tasks, and the DAG orders every producer before its
  consumers. The DAG is loaded by Airflow itself through .venv-airflow (skipped if not installed).
- DATA_DICTIONARY.md is exactly what the catalog renders.
"""
import copy
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402
from src import catalog as cat  # noqa: E402
from src import forecast, generate_data, simulate  # noqa: E402

CATALOG = cat.load(cat.CATALOG_PATH)
STRUCTURED = [d for d in CATALOG["datasets"] if d["format"] in cat.STRUCTURED]
BINARY = [d for d in CATALOG["datasets"] if d["format"] not in cat.STRUCTURED]
AIRFLOW_PYTHON = PROJECT_ROOT / ".venv-airflow" / "bin" / "python"
RAW_SAMPLE_ROWS = 200_000          # dtype inference on contacts.csv needs no more
SPARK_TO_LOGICAL = {"string": "string", "timestamp_ntz": "datetime", "date": "date",
                    "smallint": "integer", "int": "integer", "bigint": "integer",
                    "double": "float", "float": "float", "boolean": "boolean"}
FILE_SIGNATURE = {"png": bytes([0x89, 0x50, 0x4E, 0x47]), "pptx": b"PK", "html": b"<html"}


def _module_from(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem + "_for_catalog", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# --- the catalog itself -------------------------------------------------------------------
def test_catalog_is_valid():
    assert cat.validate(copy.deepcopy(CATALOG)) == []


def test_constants_match_the_code():
    tests_dir = PROJECT_ROOT / "tests"
    c = CATALOG["constants"]
    expected = {
        "SIM_DAYS": config.SIM_DAYS,
        "INTERVALS_PER_DAY": 24 * 60 // config.INTERVAL_MINUTES,
        "TEST_DAYS": forecast.TEST_DAYS,
        "FUTURE_DAYS": forecast.FUTURE_DAYS,
        "N_REASONS": len(generate_data.REASONS),
        "N_SEGMENTS": len(generate_data.SEGMENTS),
        "N_SIM_SCENARIOS": len(simulate.SCENARIOS),
        "N_STAFFING_SCENARIOS": len(cat.dataset(CATALOG, "savings_summary")["columns"][0]["allowed"]),
        "CONTACTS_MIN": _module_from(tests_dir / "test_data_quality.py").ROW_COUNT_MIN,
        "CONTACTS_MAX": _module_from(tests_dir / "test_data_quality.py").ROW_COUNT_MAX,
        "HANDLE_TIME_MIN": generate_data.HANDLE_TIME_MIN,
        "HANDLE_TIME_MAX": generate_data.HANDLE_TIME_MAX,
        "TARGET_SL": config.TARGET_SL,
        "TARGET_ANSWER_SECONDS": config.TARGET_ANSWER_SECONDS,
        "MAX_OCCUPANCY": config.MAX_OCCUPANCY,
        "SIM_SL_TOLERANCE": _module_from(tests_dir / "test_validation.py").TOLERANCE,
    }
    assert c == expected


def test_contacts_columns_and_allowed_values_match_the_generator():
    contacts = {col["name"]: col for col in cat.dataset(CATALOG, "contacts")["columns"]}
    assert list(contacts) == generate_data.COLUMNS
    assert contacts["contact_reason"]["allowed"] == generate_data.REASONS
    assert contacts["customer_segment"]["allowed"] == generate_data.SEGMENTS
    assert contacts["day_of_week"]["allowed"] == generate_data.DAYS_OF_WEEK_ORDER
    parquet = [col["name"] for col in cat.dataset(CATALOG, "contacts_parquet")["columns"]]
    assert parquet == generate_data.COLUMNS + ["interval_index"]


# --- schema drift: the catalog vs the files a pipeline run produces ------------------------------
def _locate(ds, request) -> Path:
    """Where a dataset's file is for this test run (the interim layer lives in the ETL temp dir)."""
    if ds["layer"] == "interim":
        root = request.getfixturevalue("spark_etl_full")["root"]
        return root / ds["path"].removeprefix("data/interim/")
    path = PROJECT_ROOT / ds["path"]
    if ds["id"] == "contacts" and not path.exists():
        generate_data.write_contacts(generate_data.generate_contacts(), path)
    return path


def _csv_type_problem(s: pd.Series, col: dict):
    if not col["nullable"] and s.isna().any():
        return "has nulls but is declared non-nullable"
    vals, kind, t = s.dropna(), s.dtype.kind, col["type"]
    if t == "integer":   # a nullable integer column reads as float64 with NaN
        ok = kind in "iu" or (kind == "f" and col["nullable"] and bool((vals == vals.round()).all()))
    elif t == "float":
        ok = kind in "fiu"
    elif t == "boolean":
        ok = kind == "b"
    else:
        ok = pd.api.types.is_string_dtype(s)
        fmt = {"date": "%Y-%m-%d", "datetime": "%Y-%m-%d %H:%M:%S"}.get(t)
        if ok and fmt:
            ok = bool(pd.to_datetime(vals, format=fmt, errors="coerce").notna().all())
        if ok and t == "time_of_day":
            ok = bool(vals.str.fullmatch("[0-9]{2}:[0-9]{2}").all())
    return None if ok else f"dtype {s.dtype} doesn't fit catalog type {t}"


def _json_type_ok(value, t: str) -> bool:
    if isinstance(value, bool):
        return t == "boolean"
    return {"integer": isinstance(value, int), "float": isinstance(value, (int, float)),
            "string": isinstance(value, str)}.get(t, False)


def _flatten(obj, prefix=""):
    out = {}
    for k, v in obj.items():
        key = f"{prefix}{k}"
        out.update(_flatten(v, key + ".") if isinstance(v, dict) else {key: v})
    return out


@pytest.mark.parametrize("ds", STRUCTURED, ids=[d["id"] for d in STRUCTURED])
def test_file_columns_match_catalog(ds, request):
    path = _locate(ds, request)
    declared = {col["name"]: col for col in ds["columns"]}
    problems = []
    if ds["format"] == "csv":
        df = pd.read_csv(path, nrows=RAW_SAMPLE_ROWS if ds["id"] == "contacts" else None)
        assert list(df.columns) == list(declared), "columns (or their order) drifted from the catalog"
        problems = [f"{c}: {p}" for c in df.columns if (p := _csv_type_problem(df[c], declared[c]))]
    elif ds["format"] == "json":
        fields = _flatten(json.loads(path.read_text()))
        assert set(fields) == set(declared), "JSON fields drifted from the catalog"
        problems = [f"{k}: {type(v).__name__} doesn't fit {declared[k]['type']}"
                    for k, v in fields.items() if not _json_type_ok(v, declared[k]["type"])]
    elif ds["format"] == "parquet":
        spark = request.getfixturevalue("spark")
        dtypes = spark.read.parquet(str(path)).dtypes
        assert [n for n, _ in dtypes] == list(declared), "Parquet columns drifted from the catalog"
        problems = [f"{n}: spark {t} doesn't fit {declared[n]['type']}" for n, t in dtypes
                    if SPARK_TO_LOGICAL.get(t) != declared[n]["type"]]
    assert not problems, problems


@pytest.mark.parametrize("ds", BINARY, ids=[d["id"] for d in BINARY])
def test_binary_outputs_exist_with_their_format(ds):
    path = PROJECT_ROOT / ds["path"]
    head = path.read_bytes()[:16]
    if ds["format"] == "html":
        head = head.lower()
    assert head.startswith(FILE_SIGNATURE[ds["format"]]), f"{path.name} isn't a {ds['format']}"


# --- lineage --------------------------------------------------------------------------------
def test_every_dataset_is_produced_and_consumed_or_terminal():
    tasks = set(CATALOG["tasks"])
    for d in CATALOG["datasets"]:
        assert d["produced_by"] in tasks, d["id"]
        assert d["consumed_by"] or d["terminal"], f"{d['id']} is neither consumed nor terminal"
        assert set(d["consumed_by"]) <= tasks, d["id"]


def test_every_task_is_part_of_the_lineage():
    touched = {n.split(":", 1)[1] for e in cat.lineage_edges(CATALOG) for n in e[:2] if n.startswith("t:")}
    assert touched == set(CATALOG["tasks"])


def test_lineage_graph_is_acyclic():
    assert cat.find_cycle(cat.lineage_edges(CATALOG)) is None


def test_validation_catches_a_lineage_cycle():
    bad = copy.deepcopy(CATALOG)
    deck = cat.dataset(bad, "deck")
    deck["consumed_by"], deck["terminal"] = ["generate_data"], False     # deck feeds the start
    errors = cat.validate(bad)
    assert any(e.startswith("lineage has a cycle") for e in errors), errors


def test_validation_catches_broken_references():
    bad = copy.deepcopy(CATALOG)
    cat.dataset(bad, "forecast")["consumed_by"].append("no_such_task")
    cat.dataset(bad, "staffing_plan")["row_count"] = {"exact": "UNKNOWN_CONSTANT * 2"}
    cat.dataset(bad, "validation")["columns"][0]["type"] = "text"
    errors = cat.validate(bad)
    assert any("'no_such_task' is not a task" in e for e in errors), errors
    assert any("UNKNOWN_CONSTANT" in e for e in errors), errors
    assert any("type 'text'" in e for e in errors), errors


# --- the catalog vs the Airflow DAG --------------------------------------------------------------
_DAG_SCRIPT = """
import json, sys
from airflow.dag_processing.dagbag import DagBag
bag = DagBag(dag_folder=sys.argv[1])
assert not bag.import_errors, bag.import_errors
dag = bag.dags["staffing_pipeline"]          # parsed in memory; no metadata DB needed
print("DAG_JSON=" + json.dumps({t.task_id: sorted(t.upstream_task_ids) for t in dag.tasks}))
"""


@pytest.fixture(scope="module")
def dag_upstream(tmp_path_factory):
    """{task_id: [upstream task ids]} as Airflow itself parses dags/staffing_pipeline.py."""
    if not AIRFLOW_PYTHON.exists():
        pytest.skip(".venv-airflow not installed (make airflow-install)")
    home = tmp_path_factory.mktemp("airflow_home")             # never touch ~/airflow or ./airflow_home
    env = {**os.environ, "AIRFLOW_HOME": str(home), "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
           "AIRFLOW__CORE__DAGS_FOLDER": str(PROJECT_ROOT / "dags")}
    proc = subprocess.run([str(AIRFLOW_PYTHON), "-c", _DAG_SCRIPT, str(PROJECT_ROOT / "dags")],
                          capture_output=True, text=True, env=env, timeout=300)
    line = next((ln for ln in proc.stdout.splitlines() if ln.startswith("DAG_JSON=")), None)
    assert proc.returncode == 0 and line, proc.stderr[-2000:]
    return json.loads(line.removeprefix("DAG_JSON="))


def test_catalog_tasks_equal_dag_tasks(dag_upstream):
    assert set(dag_upstream) == set(CATALOG["tasks"])


def test_dag_orders_every_producer_before_its_consumers(dag_upstream):
    def ancestors(task, seen=None):
        seen = set() if seen is None else seen
        for up in dag_upstream[task]:
            if up not in seen:
                seen.add(up)
                ancestors(up, seen)
        return seen
    for d in CATALOG["datasets"]:
        for consumer in d["consumed_by"]:
            assert d["produced_by"] in ancestors(consumer), \
                f"{d['id']}: {d['produced_by']} must run before {consumer}"
    for task, spec in CATALOG["tasks"].items():
        for gated in (spec or {}).get("gates", []):
            assert task in dag_upstream[gated], f"{task} must gate {gated}"


# --- generated documentation ---------------------------------------------------------------
def test_data_dictionary_is_up_to_date():
    assert cat.DICTIONARY_PATH.read_text() == cat.render_dictionary(cat.load_valid()), \
        "DATA_DICTIONARY.md is stale — run `python -m src.catalog`"
