"""Shared fixtures. The Spark session is created once per test run (one JVM for every Spark test)
and only when a test asks for it, so the pandas-only suites never import pyspark."""
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.errors import PySparkRuntimeError
    from src import spark_etl
    try:
        session = spark_etl.get_spark("staffing-etl-tests")
    except PySparkRuntimeError as exc:          # e.g. JAVA_GATEWAY_EXITED: no usable JDK
        pytest.skip(f"no working Java runtime for Spark: {exc}")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def spark_etl_full(spark, tmp_path_factory):
    """The Spark ETL run once on the full data/raw/contacts.csv (generated with the seeded defaults
    if missing) into a temp dir laid out like data/interim/: <root>/contacts.parquet, <root>/spark/."""
    from src import spark_etl
    from src.generate_data import RAW_CSV_PATH, generate_contacts, write_contacts
    if not RAW_CSV_PATH.exists():
        write_contacts(generate_contacts(), RAW_CSV_PATH)
    root = tmp_path_factory.mktemp("spark_etl_full")
    result = spark_etl.run_etl(RAW_CSV_PATH, root / "contacts.parquet", root / "spark", spark=spark)
    return {"root": root, "parquet": root / "contacts.parquet", "result": result}
