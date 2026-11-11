"""Static region reference (dim_region source), packaged with the code as ``reference/regions.csv``.

The packaged copy holds the same rows as ``docs/contract/reference/regions.csv``; a test enforces that.
"""

from __future__ import annotations

import csv
from functools import cache
from importlib import resources

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

REGION_SCHEMA = T.StructType(
    [
        T.StructField("region_code", T.StringType(), False),
        T.StructField("region_name", T.StringType(), False),
        T.StructField("istat_region_name", T.StringType(), False),
        T.StructField("istat_area_code", T.StringType(), False),
        T.StructField("geographic_area", T.StringType(), False),
        T.StructField("macro_area", T.StringType(), False),
        T.StructField("is_autonomous_province", T.BooleanType(), False),
        T.StructField("sort_order", T.IntegerType(), False),
    ]
)


@cache
def region_rows() -> tuple[tuple[object, ...], ...]:
    """Return the 21 region rows (20 ISTAT regions), typed as ``REGION_SCHEMA``."""
    text = resources.files("healthcare_lakehouse").joinpath("reference/regions.csv").read_text(encoding="utf-8")
    rows = []
    for rec in csv.DictReader(text.splitlines()):
        rows.append(
            (
                rec["region_code"],
                rec["region_name"],
                rec["istat_region_name"],
                rec["istat_area_code"],
                rec["geographic_area"],
                rec["macro_area"],
                rec["is_autonomous_province"] == "true",
                int(rec["sort_order"]),
            )
        )
    return tuple(rows)


def load_regions(spark: SparkSession) -> DataFrame:
    """Return ``dim_region`` (before the gold column order is applied)."""
    return spark.createDataFrame(list(region_rows()), schema=REGION_SCHEMA)
