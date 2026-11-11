"""Contract test: the gold DataFrames must match docs/contract/gold_schema.yml.

Checks names and their order, Spark types, and for non-nullable columns and primary keys the absence of
nulls and duplicates in a small synthetic extract. The parity test covers the values.
"""

from __future__ import annotations

from typing import Any

import pytest
import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from support import CONTRACT_PATH, raw_bed_row, raw_beds_frame, raw_population_frame

from healthcare_lakehouse.gold import build_gold_tables

SPARK_TYPES: dict[str, type[T.DataType]] = {
    "string": T.StringType,
    "int": T.IntegerType,
    "bigint": T.LongType,
    "boolean": T.BooleanType,
    "date": T.DateType,
}


@pytest.fixture(scope="module")
def contract() -> dict[str, Any]:
    return yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def synthetic_gold(spark: SparkSession) -> dict[str, DataFrame]:
    """Two years, two facilities (one multi-site), two disciplines, two regions. Enough for every table."""
    beds = raw_beds_frame(
        spark,
        [
            raw_bed_row(year="2021", structure_code="010001", discipline_code="08", beds_total="13"),
            raw_bed_row(year="2022", structure_code="010001", discipline_code="08", beds_total="13"),
            raw_bed_row(
                year="2022",
                structure_code="010002",
                site_subcode="01",
                discipline_code="28",
                discipline_name="RIABILITAZIONE",
                discipline_type="RIABILITAZIONE",
                beds_ordinary="0",
                beds_paying="0",
                beds_day_hospital="0",
                beds_day_surgery="0",
                beds_total="0",
                ward_count="0",
            ),
        ],
    )
    population = raw_population_frame(
        spark,
        [
            {"REF_AREA": "ITC1", "TIME_PERIOD": 2021, "OBS_VALUE": 4300000},
            {"REF_AREA": "ITC1", "TIME_PERIOD": 2022, "OBS_VALUE": 4290000},
            {"REF_AREA": "ITD1", "TIME_PERIOD": 2021, "OBS_VALUE": 533000},
            {"REF_AREA": "ITD1", "TIME_PERIOD": 2022, "OBS_VALUE": 534000},
        ],
    )
    return build_gold_tables(spark, beds, population, 2021, 2022)


def test_every_contract_table_is_built(contract: dict[str, Any], synthetic_gold: dict[str, DataFrame]) -> None:
    assert set(synthetic_gold) == set(contract["tables"])


@pytest.mark.parametrize(
    "table", ["dim_year", "dim_region", "dim_discipline", "dim_facility", "fact_hospital_beds", "fact_population"]
)
def test_columns_names_order_and_types_match_the_contract(
    contract: dict[str, Any], synthetic_gold: dict[str, DataFrame], table: str
) -> None:
    expected = contract["tables"][table]["columns"]
    schema = synthetic_gold[table].schema
    assert schema.fieldNames() == [c["name"] for c in expected]
    for column, field in zip(expected, schema.fields, strict=True):
        assert isinstance(field.dataType, SPARK_TYPES[column["type"]]), (
            f"{table}.{column['name']}: {field.dataType} is not {column['type']}"
        )


@pytest.mark.parametrize(
    "table", ["dim_year", "dim_region", "dim_discipline", "dim_facility", "fact_hospital_beds", "fact_population"]
)
def test_non_nullable_columns_have_no_nulls_and_keys_are_unique(
    contract: dict[str, Any], synthetic_gold: dict[str, DataFrame], table: str
) -> None:
    spec = contract["tables"][table]
    df = synthetic_gold[table]
    not_null = [c["name"] for c in spec["columns"] if not c["nullable"]]
    null_counts = df.select(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in not_null]).collect()[0]
    assert all(null_counts[c] in (0, None) for c in not_null), null_counts.asDict()

    key_columns = spec["primary_key"]
    assert df.groupBy(*key_columns).count().filter(F.col("count") > 1).count() == 0
    for unique in spec.get("unique", []):
        assert df.groupBy(*unique).count().filter(F.col("count") > 1).count() == 0


def test_region_dimension_has_the_21_reference_rows(synthetic_gold: dict[str, DataFrame]) -> None:
    assert synthetic_gold["dim_region"].count() == 21
