"""Gold builders: one function per contract table, with the column order of gold_schema.yml.

Rules implemented here: C3 (SCD type 1 by whole row), C4 (facility_key), C5 (health_authority_id),
C6 (discipline_type in sentence case). C2 is in silver. C7 is enforced by the pipeline expectations
and by the tests.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from healthcare_lakehouse.config import AUTHORITY_TYPES, FACILITY_ATTRIBUTES
from healthcare_lakehouse.reference import load_regions
from healthcare_lakehouse.silver import to_silver_beds, to_silver_population


def latest_row_per(df: DataFrame, key: str, order: list[Column]) -> DataFrame:
    """Keep one whole row per ``key``: the first row under ``order`` (rule C3, SCD type 1).

    The row is taken as a whole, so every attribute comes from the same year. Taking the last non-null
    value per column would mix years.
    """
    w = Window.partitionBy(key).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def sentence_case(col: Column) -> Column:
    """Rule C6: first letter upper case, the rest lower case (``ACUTI`` becomes ``Acuti``)."""
    return F.concat(F.upper(F.substring(col, 1, 1)), F.lower(F.substring(col, 2, 100_000)))


def authority_type_label(code: Column) -> Column:
    """Map the Ministry authority type (1 or 2) to its verbatim label. Unknown codes become null."""
    expr: Column = F.lit(None).cast("string")
    for source_code, label in reversed(list(AUTHORITY_TYPES.items())):
        expr = F.when(code == source_code, F.lit(label)).otherwise(expr)
    return expr


def build_dim_year(spark: SparkSession, from_year: int, to_year: int) -> DataFrame:
    """One row per year of the gold window, with 1 January as reference date."""
    years = spark.range(from_year, to_year + 1).select(F.col("id").cast("int").alias("year"))
    return years.select(
        "year",
        F.make_date(F.col("year"), F.lit(1), F.lit(1)).alias("reference_date"),
        (F.col("year") == F.lit(to_year)).alias("is_latest_year"),
    )


def build_dim_region(regions: DataFrame) -> DataFrame:
    """Static region reference, already typed (see ``reference.load_regions``)."""
    return regions.select(
        "region_code",
        "region_name",
        "istat_region_name",
        "istat_area_code",
        "geographic_area",
        "macro_area",
        "is_autonomous_province",
        "sort_order",
    )


def build_dim_discipline(beds: DataFrame) -> DataFrame:
    """Discipline attributes from the latest year the discipline appears in (C3, C6)."""
    latest = latest_row_per(beds, "discipline_code", [F.col("year").desc(), F.col("facility_id").desc()])
    return latest.select(
        "discipline_code",
        "discipline_name",
        sentence_case(F.col("discipline_type")).alias("discipline_type"),
    )


def build_dim_facility(beds: DataFrame) -> DataFrame:
    """One row per hospital site (C3, C4, C5).

    Attributes come from the latest beds row of the site. ``first_year`` and ``last_year`` are the
    site's span in the window. ``facility_key`` is the rank of ``facility_id`` in ascending order.
    """
    latest = latest_row_per(beds, "facility_id", [F.col("year").desc(), F.col("discipline_code").desc()])
    attributes = latest.select("facility_id", *FACILITY_ATTRIBUTES)
    span = beds.groupBy("facility_id").agg(
        F.min("year").cast("int").alias("first_year"),
        F.max("year").cast("int").alias("last_year"),
    )
    site = attributes.join(span, on="facility_id", how="inner")
    keyed = site.withColumn("facility_key", F.row_number().over(Window.orderBy("facility_id")).cast("int"))
    return keyed.select(
        "facility_key",
        "facility_id",
        "structure_code",
        "site_subcode",
        "facility_name",
        "address",
        "municipality_code",
        "municipality_name",
        "province_code",
        "region_code",
        F.concat(F.col("region_code"), F.col("authority_code")).alias("health_authority_id"),
        authority_type_label(F.col("authority_type_code")).alias("health_authority_type"),
        "facility_type_code",
        "facility_type_name",
        "first_year",
        "last_year",
    )


def build_fact_hospital_beds(beds: DataFrame, dim_facility: DataFrame) -> DataFrame:
    """Beds by year, site and discipline. ``facility_key`` comes from joining the site dimension."""
    keys = dim_facility.select("facility_id", "facility_key")
    joined = beds.join(F.broadcast(keys), on="facility_id", how="inner")
    return joined.select(
        F.col("year").cast("int").alias("year"),
        F.col("facility_key").cast("int").alias("facility_key"),
        "discipline_code",
        "region_code",
        F.col("ward_count").cast("int").alias("ward_count"),
        F.col("beds_ordinary").cast("int").alias("beds_ordinary"),
        F.col("beds_paying").cast("int").alias("beds_paying"),
        F.col("beds_day_hospital").cast("int").alias("beds_day_hospital"),
        F.col("beds_day_surgery").cast("int").alias("beds_day_surgery"),
        F.col("beds_total").cast("int").alias("beds_total"),
    )


def build_fact_population(population: DataFrame) -> DataFrame:
    """Resident population on 1 January, by year and region."""
    return population.select(
        F.col("year").cast("int").alias("year"),
        "region_code",
        F.col("population").cast("bigint").alias("population"),
    )


def build_gold_tables(
    spark: SparkSession,
    beds_raw: DataFrame,
    population_raw: DataFrame,
    from_year: int,
    to_year: int,
) -> dict[str, DataFrame]:
    """Compose silver and gold: all six contract tables, keyed by table name.

    This is the whole transformation in one call. The pipeline materialises the same builders one by one
    so that each step gets its own expectations.
    """
    regions = load_regions(spark)
    beds = to_silver_beds(beds_raw, from_year, to_year)
    population = to_silver_population(population_raw, regions, from_year, to_year)
    dim_facility = build_dim_facility(beds)
    return {
        "dim_year": build_dim_year(spark, from_year, to_year),
        "dim_region": build_dim_region(regions),
        "dim_discipline": build_dim_discipline(beds),
        "dim_facility": dim_facility,
        "fact_hospital_beds": build_fact_hospital_beds(beds, dim_facility),
        "fact_population": build_fact_population(population),
    }
