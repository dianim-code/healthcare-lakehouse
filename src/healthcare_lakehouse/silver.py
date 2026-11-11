"""Silver: typed, cleaned source rows, restricted to the gold window."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from healthcare_lakehouse.cleaning import clean_text_columns
from healthcare_lakehouse.config import BEDS_COLUMNS, COUNT_COLUMNS, POPULATION_COLUMNS


def to_silver_beds(raw: DataFrame, from_year: int, to_year: int) -> DataFrame:
    """Rename, clean (C1), type and window the beds rows, then build ``facility_id`` (C2).

    Counts are cast to ``bigint``. Values that are not numbers become null, and the silver expectations
    fail the pipeline on any null count.
    """
    staged = raw.select([F.col(src).alias(dst) for src, dst in BEDS_COLUMNS.items()])
    text_columns = [c for c in staged.columns if c not in COUNT_COLUMNS and c != "year"]
    cleaned = clean_text_columns(staged, [*text_columns, *COUNT_COLUMNS])
    typed = cleaned.withColumn("year", F.col("year").cast("int")).withColumns(
        {c: F.col(c).cast("bigint") for c in COUNT_COLUMNS}
    )
    windowed = typed.filter(F.col("year").between(from_year, to_year))
    return windowed.withColumn("facility_id", facility_id(F.col("structure_code"), F.col("site_subcode")))


def facility_id(structure_code: Column, site_subcode: Column) -> Column:
    """Rule C2: ``structure_code`` alone, or ``structure_code-site_subcode`` when a subcode exists."""
    return F.when(site_subcode.isNull(), structure_code).otherwise(F.concat(structure_code, F.lit("-"), site_subcode))


def to_silver_population(raw: DataFrame, regions: DataFrame, from_year: int, to_year: int) -> DataFrame:
    """Select the ISTAT columns, clean the area code, type the values and map areas to ``region_code``.

    Areas without a region keep a null ``region_code``; the pipeline expectations fail on them.
    """
    staged = raw.select([F.col(src).alias(dst) for src, dst in POPULATION_COLUMNS.items()])
    cleaned = clean_text_columns(staged, ["istat_area_code"])
    typed = cleaned.withColumn("year", F.col("year").cast("int")).withColumn(
        "population", F.col("population").cast("bigint")
    )
    windowed = typed.filter(F.col("year").between(from_year, to_year))
    mapping = regions.select("istat_area_code", "region_code")
    return windowed.join(mapping, on="istat_area_code", how="left")
