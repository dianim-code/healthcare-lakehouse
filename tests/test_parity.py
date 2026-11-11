"""Parity test: the PySpark gold tables must reproduce sample-data/gold/*.csv value for value.

The sample is built by the pandas reference (scripts/build_sample.py) for 2021-2023. Running the Spark
builders on the same raw files and comparing every cell proves that both implement rules C1-C7 identically.

The test needs the raw files in data/raw/. Without them it is skipped with the command that fetches them.
"""

from __future__ import annotations

import datetime
import math
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml
from pyspark.sql import DataFrame, SparkSession
from support import (
    CONTRACT_PATH,
    GOLD_SAMPLE_DIR,
    RAW_DIR,
    REPO_ROOT,
    beds_raw_frame,
    population_raw_frame_from_file,
)

from healthcare_lakehouse.gold import build_gold_tables

FROM_YEAR, TO_YEAR = 2021, 2023
BEDS_FILES = [RAW_DIR / f"beds_{year}.csv" for year in range(FROM_YEAR, TO_YEAR + 1)]
POPULATION_FILE = RAW_DIR / "population_istat.csv"
MISSING = [str(p.relative_to(REPO_ROOT)) for p in [*BEDS_FILES, POPULATION_FILE] if not p.exists()]

pytestmark = pytest.mark.skipif(
    bool(MISSING),
    reason=(
        f"raw sources missing ({', '.join(MISSING)}). Run: uv run scripts/fetch_sources.py "
        "--only beds_2021 beds_2022 beds_2023 population"
    ),
)


def as_text(value: Any) -> str:
    """Render one cell the way the CSV writer does: null as empty, booleans as true/false, ISO dates."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    if isinstance(value, bool | np.bool_):
        return "true" if value else "false"
    if isinstance(value, datetime.date):
        return value.isoformat()
    return str(value)


def load_contract() -> dict[str, Any]:
    return yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def spark_gold(spark: SparkSession, backend: str) -> dict[str, DataFrame]:
    beds = beds_raw_frame(spark, BEDS_FILES, backend)
    population = population_raw_frame_from_file(spark, POPULATION_FILE, backend)
    return build_gold_tables(spark, beds, population, FROM_YEAR, TO_YEAR)


@pytest.mark.parametrize("table", list(load_contract()["tables"]))
def test_gold_table_matches_sample_value_for_value(spark_gold: dict[str, DataFrame], table: str) -> None:
    spec = load_contract()["tables"][table]
    columns = [c["name"] for c in spec["columns"]]
    keys = spec["primary_key"]

    expected = pd.read_csv(GOLD_SAMPLE_DIR / f"{table}.csv", dtype=str, keep_default_na=False)
    actual = spark_gold[table].toPandas().map(as_text)

    assert list(actual.columns) == columns
    assert list(expected.columns) == columns
    assert len(actual) == len(expected), f"{table}: {len(actual)} rows, sample has {len(expected)}"

    expected = expected.sort_values(keys).reset_index(drop=True)
    actual = actual.sort_values(keys).reset_index(drop=True)
    pd.testing.assert_frame_equal(actual, expected, check_dtype=False)
