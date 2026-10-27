"""Data catalog — load and validate metadata/catalog.yaml, render DATA_DICTIONARY.md.

metadata/catalog.yaml is the single source of truth for every dataset the pipeline reads or writes:
path, layer, producing and consuming tasks, grain, primary key, row-count rule and columns. This
module
  - loads it (resolving `columns_like` / `extra_columns`) and validates its structure and
    references,
  - evaluates row-count and column-range rules, which may name the catalog's constants,
  - derives lineage (dataset -> task -> dataset edges, plus task -> task gates) and checks it is
    acyclic,
  - gives each task its inlets and outlets (read by the Airflow DAG to declare Assets),
  - renders DATA_DICTIONARY.md deterministically: the same catalog always gives the same bytes.

It imports only the standard library and PyYAML, so the Airflow DAG (.venv-airflow) and the
validation layer (.venv-gx) use it as well.

Run:  python -m src.catalog           # validate, then (re)write DATA_DICTIONARY.md
      python -m src.catalog --check   # exit 1 if DATA_DICTIONARY.md is out of date
"""
from __future__ import annotations

import argparse
import ast
import copy
import operator
import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = PROJECT_ROOT / "metadata" / "catalog.yaml"
DICTIONARY_PATH = PROJECT_ROOT / "DATA_DICTIONARY.md"

LAYERS = ("raw", "interim", "processed", "output")
FORMATS = ("csv", "parquet", "json", "png", "html", "pptx")
TABULAR = ("csv", "parquet")                     # need grain, primary key, row count, columns
STRUCTURED = TABULAR + ("json",)                 # have columns (JSON: dotted field paths)
TYPES = ("string", "integer", "float", "boolean", "date", "datetime", "time_of_day")
DATASET_KEYS = {"id", "path", "layer", "format", "committed", "produced_by", "consumed_by",
                "pandas_engine_consumers", "external_consumers", "terminal", "grain",
                "primary_key", "row_count", "description", "columns", "columns_like",
                "extra_columns"}
COLUMN_KEYS = {"name", "type", "unit", "nullable", "allowed", "min", "max", "pattern",
               "description"}


class CatalogError(ValueError):
    """The catalog failed validation; the message lists every problem found."""


# --------------------------------------------------------------------------------------
# Loading
# --------------------------------------------------------------------------------------
def load(path: Path = CATALOG_PATH) -> dict:
    """Parse the catalog and resolve `columns_like` (+ `extra_columns`) into full column lists."""
    catalog = yaml.safe_load(Path(path).read_text())
    by_id = {d.get("id"): d for d in catalog.get("datasets", [])}
    for d in catalog.get("datasets", []):
        if "columns_like" in d:
            base = by_id.get(d["columns_like"], {})
            d["columns"] = copy.deepcopy(base.get("columns", [])) + d.get("extra_columns", [])
    return catalog


def dataset(catalog: dict, dataset_id: str) -> dict:
    return next(d for d in catalog["datasets"] if d["id"] == dataset_id)


# --------------------------------------------------------------------------------------
# Rules: arithmetic over the catalog's constants
# --------------------------------------------------------------------------------------
_OPS = {ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul,
        ast.Div: operator.truediv, ast.FloorDiv: operator.floordiv}


def evaluate(rule, constants: dict):
    """A number, or arithmetic (+ - * / //) over numbers and constant names, e.g.
    "FUTURE_DAYS * INTERVALS_PER_DAY". Anything else raises CatalogError."""
    if isinstance(rule, bool):
        raise CatalogError(f"rule {rule!r} is not a number")
    if isinstance(rule, (int, float)):
        return rule
    if not isinstance(rule, str):
        raise CatalogError(f"rule {rule!r} is not a number or expression")

    def ev(node):
        if isinstance(node, ast.Expression):
            return ev(node.body)
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)) \
                and not isinstance(node.value, bool):
            return node.value
        if isinstance(node, ast.Name):
            if node.id not in constants:
                raise CatalogError(f"unknown constant {node.id!r} in rule {rule!r}")
            return constants[node.id]
        if isinstance(node, ast.BinOp) and type(node.op) in _OPS:
            return _OPS[type(node.op)](ev(node.left), ev(node.right))
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
            return -ev(node.operand)
        raise CatalogError(f"unsupported syntax in rule {rule!r}")

    try:
        tree = ast.parse(rule, mode="eval")
    except SyntaxError as exc:
        raise CatalogError(f"rule {rule!r} is not valid arithmetic") from exc
    return ev(tree)


def row_bounds(ds: dict, catalog: dict):
    """(min_rows, max_rows) for a tabular dataset; None if it has no row-count rule."""
    rule = ds.get("row_count")
    if not rule:
        return None
    c = catalog["constants"]
    if "exact" in rule:
        n = evaluate(rule["exact"], c)
        return n, n
    return evaluate(rule["min"], c), evaluate(rule["max"], c)


def column_bounds(col: dict, catalog: dict):
    """(min, max) of a column's declared range; either side may be None."""
    c = catalog["constants"]
    lo = evaluate(col["min"], c) if "min" in col else None
    hi = evaluate(col["max"], c) if "max" in col else None
    return lo, hi


# --------------------------------------------------------------------------------------
# Lineage
# --------------------------------------------------------------------------------------
def task_io(catalog: dict) -> dict:
    """{task: {"inlets": [dataset ids it reads], "outlets": [dataset ids it writes]}}."""
    io = {t: {"inlets": [], "outlets": []} for t in catalog["tasks"]}
    for d in catalog["datasets"]:
        io[d["produced_by"]]["outlets"].append(d["id"])
        for t in d["consumed_by"]:
            io[t]["inlets"].append(d["id"])
    return io


def lineage_edges(catalog: dict) -> list[tuple[str, str, str]]:
    """(source, target, kind) with nodes "t:<task>" / "d:<dataset>" and kind "data" or "gate"."""
    edges = []
    for d in catalog["datasets"]:
        edges.append((f"t:{d['produced_by']}", f"d:{d['id']}", "data"))
        edges.extend((f"d:{d['id']}", f"t:{t}", "data") for t in d["consumed_by"])
    for task, spec in catalog["tasks"].items():
        edges.extend((f"t:{task}", f"t:{g}", "gate") for g in (spec or {}).get("gates", []))
    return edges


def find_cycle(edges) -> list[str] | None:
    """A cycle as a node list (first node repeated at the end), or None if the graph is a DAG."""
    graph: dict[str, list[str]] = {}
    for src, dst, *_ in edges:
        graph.setdefault(src, []).append(dst)
        graph.setdefault(dst, [])
    state, stack = {}, []                       # state: 1 = on the current path, 2 = finished

    def visit(node):
        state[node] = 1
        stack.append(node)
        for nxt in graph[node]:
            if state.get(nxt) == 1:
                return stack[stack.index(nxt):] + [nxt]
            if nxt not in state:
                found = visit(nxt)
                if found:
                    return found
        stack.pop()
        state[node] = 2
        return None

    for node in graph:
        if node not in state:
            found = visit(node)
            if found:
                return found
    return None


# --------------------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------------------
def validate(catalog: dict) -> list[str]:
    """Every structural and referential problem in the catalog (empty list = valid)."""
    errors: list[str] = []
    for key in ("version", "constants", "tasks", "datasets"):
        if key not in catalog:
            errors.append(f"missing top-level key {key!r}")
    if errors:
        return errors
    constants, tasks = catalog["constants"], catalog["tasks"]
    for name, value in constants.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            errors.append(f"constant {name!r} must be a number")
    for task, spec in tasks.items():
        for g in (spec or {}).get("gates", []):
            if g not in tasks:
                errors.append(f"task {task!r} gates unknown task {g!r}")

    seen_ids, seen_paths = set(), set()
    for d in catalog["datasets"]:
        did = d.get("id", "<no id>")
        where = f"dataset {did!r}"
        for key in ("id", "path", "layer", "format", "committed", "produced_by", "consumed_by",
                    "terminal", "description"):
            if key not in d:
                errors.append(f"{where}: missing {key!r}")
        for key in set(d) - DATASET_KEYS:
            errors.append(f"{where}: unknown key {key!r}")
        if did in seen_ids:
            errors.append(f"{where}: duplicate id")
        if d.get("path") in seen_paths:
            errors.append(f"{where}: duplicate path {d.get('path')!r}")
        seen_ids.add(did)
        seen_paths.add(d.get("path"))
        if d.get("layer") not in LAYERS:
            errors.append(f"{where}: layer {d.get('layer')!r} not in {LAYERS}")
        fmt = d.get("format")
        if fmt not in FORMATS:
            errors.append(f"{where}: format {fmt!r} not in {FORMATS}")
        if d.get("produced_by") not in tasks:
            errors.append(f"{where}: produced_by {d.get('produced_by')!r} is not a task")
        consumers = d.get("consumed_by", [])
        for t in consumers + d.get("pandas_engine_consumers", []):
            if t not in tasks:
                errors.append(f"{where}: consumer {t!r} is not a task")
        if d.get("produced_by") in consumers:
            errors.append(f"{where}: {d['produced_by']!r} both produces and consumes it")
        if bool(d.get("terminal")) != (len(consumers) == 0):
            errors.append(f"{where}: terminal must be true exactly when no task consumes it")
        if "columns_like" in d and d["columns_like"] not in {x.get("id") for x in catalog["datasets"]}:
            errors.append(f"{where}: columns_like {d['columns_like']!r} is not a dataset")

        if fmt in TABULAR:
            for key in ("grain", "primary_key", "row_count"):
                if key not in d:
                    errors.append(f"{where}: tabular datasets need {key!r}")
            try:
                bounds = row_bounds(d, catalog)
                if bounds and bounds[0] > bounds[1]:
                    errors.append(f"{where}: row_count min > max")
            except (CatalogError, KeyError) as exc:
                errors.append(f"{where}: bad row_count ({exc})")
        if fmt in STRUCTURED:
            cols = d.get("columns") or []
            if not cols:
                errors.append(f"{where}: needs columns")
            names = [c.get("name") for c in cols]
            if len(names) != len(set(names)):
                errors.append(f"{where}: duplicate column names")
            for k in d.get("primary_key") or []:
                if k not in names:
                    errors.append(f"{where}: primary key column {k!r} is not a column")
            for c in cols:
                cw = f"{where} column {c.get('name')!r}"
                for key in ("name", "type", "nullable", "description"):
                    if key not in c:
                        errors.append(f"{cw}: missing {key!r}")
                for key in set(c) - COLUMN_KEYS:
                    errors.append(f"{cw}: unknown key {key!r}")
                if c.get("type") not in TYPES:
                    errors.append(f"{cw}: type {c.get('type')!r} not in {TYPES}")
                try:
                    lo, hi = column_bounds(c, catalog)
                    if lo is not None and hi is not None and lo > hi:
                        errors.append(f"{cw}: min > max")
                except CatalogError as exc:
                    errors.append(f"{cw}: {exc}")
        elif d.get("columns"):
            errors.append(f"{where}: {fmt} datasets have no columns")

    if not errors:
        cycle = find_cycle(lineage_edges(catalog))
        if cycle:
            errors.append("lineage has a cycle: " + " -> ".join(cycle))
    return errors


def load_valid(path: Path = CATALOG_PATH) -> dict:
    """load() + validate(); raises CatalogError listing every problem."""
    catalog = load(path)
    errors = validate(catalog)
    if errors:
        raise CatalogError("invalid catalog:\n  " + "\n  ".join(errors))
    return catalog


# --------------------------------------------------------------------------------------
# DATA_DICTIONARY.md
# --------------------------------------------------------------------------------------
_LAYER_TITLE = {"raw": "Raw", "interim": "Interim (Spark engine)", "processed": "Processed",
                "output": "Outputs"}
_LAYER_STYLE = {"raw": "fill:#f6e3c5,stroke:#b07d2b,color:#1b2a4a",
                "interim": "fill:#e4dcf2,stroke:#6c4fa3,color:#1b2a4a",
                "processed": "fill:#d6e6f5,stroke:#2f6aa3,color:#1b2a4a",
                "output": "fill:#d9efe0,stroke:#2e7d4f,color:#1b2a4a"}


def _cell(text) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def _num(x) -> str:
    if isinstance(x, float) and x.is_integer():
        x = int(x)
    return f"{x:,}" if isinstance(x, int) else f"{x:g}"


def _label(path: str) -> str:
    """Last two path parts, enough to tell same-named files apart (spark/ vs processed/)."""
    parts = path.split("/")
    return "/".join(parts[-2:])


def _rule_text(rule, constants) -> str:
    value = evaluate(rule, constants)
    return _num(value) if isinstance(rule, (int, float)) else f"{_num(value)} (`{rule}`)"


def _range_text(col: dict, catalog: dict) -> str:
    if "allowed" in col:
        return ", ".join(f"`{v}`" for v in col["allowed"])
    parts = []
    if "min" in col or "max" in col:
        c = catalog["constants"]
        lo = _rule_text(col["min"], c) if "min" in col else ""
        hi = _rule_text(col["max"], c) if "max" in col else ""
        parts.append(f"{lo} – {hi}" if lo and hi else (f"≥ {lo}" if lo else f"≤ {hi}"))
    if "pattern" in col:
        parts.append(f"`{col['pattern']}`")
    return "; ".join(parts)


def render_mermaid(catalog: dict) -> str:
    """Lineage graph: datasets (cylinders, coloured by layer) -> tasks (boxes) -> datasets;
    dashed arrows are gates (control dependencies, no data)."""
    lines = ["flowchart LR"]
    for layer, style in _LAYER_STYLE.items():
        lines.append(f"    classDef {layer} {style}")
    lines.append("    classDef task fill:#1b2a4a,stroke:#1b2a4a,color:#ffffff")
    for task in catalog["tasks"]:
        lines.append(f'    t_{task}["{task}"]:::task')
    for d in catalog["datasets"]:
        lines.append(f'    d_{d["id"]}[("{_label(d["path"])}")]:::{d["layer"]}')
    for src, dst, kind in lineage_edges(catalog):
        a, b = src.replace(":", "_", 1), dst.replace(":", "_", 1)
        lines.append(f"    {a} -. gates .-> {b}" if kind == "gate" else f"    {a} --> {b}")
    return "\n".join(lines)


def render_dictionary(catalog: dict) -> str:
    c = catalog["constants"]
    out = [
        "# Data dictionary",
        "",
        "Generated from [`metadata/catalog.yaml`](metadata/catalog.yaml) by `python -m src.catalog`; "
        "do not edit by hand (`tests/test_catalog.py` fails if this file is out of date).",
        "",
        "Every dataset the pipeline reads or writes, with its producer, consumers, grain, key, "
        "row-count rule and columns. Lineage follows the Airflow DAG (Spark engine); on the default "
        "pandas engine, the stages listed as pandas-engine consumers read `contacts.csv` directly "
        "and the interim layer is not produced. All data is synthetic.",
        "",
        "## Lineage",
        "",
        "Cylinders are datasets (coloured by layer), boxes are tasks, dashed arrows are gates.",
        "",
        "```mermaid",
        render_mermaid(catalog),
        "```",
        "",
        "## Datasets",
        "",
        "| Dataset | Layer | Path | Produced by | Consumed by | Rows | Committed |",
        "|---|---|---|---|---|---|---|",
    ]
    for d in catalog["datasets"]:
        bounds = row_bounds(d, catalog)
        rows = "" if bounds is None else (
            _num(bounds[0]) if bounds[0] == bounds[1] else f"{_num(bounds[0])} – {_num(bounds[1])}")
        consumers = ", ".join(f"`{t}`" for t in d["consumed_by"]) or "— (terminal)"
        out.append(f"| [`{d['id']}`](#{d['id']}) | {d['layer']} | `{d['path']}` | "
                   f"`{d['produced_by']}` | {consumers} | {rows} | {'yes' if d['committed'] else 'no'} |")

    for layer in LAYERS:
        members = [d for d in catalog["datasets"] if d["layer"] == layer]
        if not members:
            continue
        out += ["", f"## {_LAYER_TITLE[layer]}"]
        for d in members:
            out += ["", f"### {d['id']}", "", _cell(d["description"]), ""]
            out.append(f"- **Path:** `{d['path']}` ({d['format']}; "
                       f"{'committed' if d['committed'] else 'git-ignored, regenerated'})")
            out.append(f"- **Produced by:** `{d['produced_by']}`")
            consumers = ", ".join(f"`{t}`" for t in d["consumed_by"]) or "none (terminal)"
            if d.get("pandas_engine_consumers"):
                consumers += " · pandas engine: " + ", ".join(
                    f"`{t}`" for t in d["pandas_engine_consumers"])
            if d.get("external_consumers"):
                consumers += " · external: " + ", ".join(d["external_consumers"])
            out.append(f"- **Consumed by:** {consumers}")
            if d.get("grain"):
                out.append(f"- **Grain:** {d['grain']}")
            if d.get("primary_key"):
                out.append("- **Primary key:** " + ", ".join(f"`{k}`" for k in d["primary_key"]))
            rule = d.get("row_count")
            if rule:
                text = (_rule_text(rule["exact"], c) if "exact" in rule else
                        f"{_rule_text(rule['min'], c)} to {_rule_text(rule['max'], c)}")
                out.append(f"- **Rows:** {text}")
            if d.get("columns"):
                out += ["", "| Column | Type | Unit | Allowed / range | Nullable | Description |",
                        "|---|---|---|---|---|---|"]
                for col in d["columns"]:
                    out.append(f"| `{col['name']}` | {col['type']} | {_cell(col.get('unit', ''))} | "
                               f"{_cell(_range_text(col, catalog))} | "
                               f"{'yes' if col['nullable'] else 'no'} | {_cell(col['description'])} |")
    return "\n".join(out) + "\n"


def write_dictionary(catalog: dict, path: Path = DICTIONARY_PATH) -> Path:
    path.write_text(render_dictionary(catalog))
    return path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Validate the data catalog and render the dictionary.")
    parser.add_argument("--check", action="store_true",
                        help="exit 1 if DATA_DICTIONARY.md doesn't match the catalog")
    args = parser.parse_args(argv)
    catalog = load_valid()
    rendered = render_dictionary(catalog)
    n = len(catalog["datasets"])
    if args.check:
        current = DICTIONARY_PATH.read_text() if DICTIONARY_PATH.exists() else ""
        if current != rendered:
            print(f"{DICTIONARY_PATH.name} is out of date — run `python -m src.catalog`.")
            sys.exit(1)
        print(f"Catalog valid ({n} datasets); {DICTIONARY_PATH.name} is up to date.")
        return
    write_dictionary(catalog)
    print(f"Catalog valid ({n} datasets, {len(catalog['tasks'])} tasks); "
          f"wrote {DICTIONARY_PATH.relative_to(PROJECT_ROOT)}.")


if __name__ == "__main__":
    main()
