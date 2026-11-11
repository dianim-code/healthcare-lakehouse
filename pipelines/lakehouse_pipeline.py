"""Lakeflow Declarative Pipeline: bronze, silver and gold as thin wrappers around ``healthcare_lakehouse``.

The transformations live in the package (pure DataFrame-to-DataFrame functions, tested locally and on
Databricks Connect). This file only names the datasets, reads the pipeline configuration and attaches the
data-quality expectations of rule C7.

API: ``from pyspark import pipelines as dp`` and ``@dp.materialized_view`` (Lakeflow Declarative Pipelines
Python reference, https://docs.databricks.com/aws/en/ldp/developer/python-dev). ``spark`` is provided by the
pipeline runtime. Pipeline settings are read with ``spark.conf.get`` (keys set in the pipeline configuration,
https://docs.databricks.com/aws/en/ldp/parameters).

Expectations cannot express uniqueness, cross-table foreign keys or "every year x region present". Those
checks are enforced by construction (window functions, inner joins) and by the test suite.
"""

from pyspark import pipelines as dp

from healthcare_lakehouse import bronze, gold, reference, silver
from healthcare_lakehouse.config import GOLD_WINDOW_DEFAULT

REGION_CODES = ", ".join(f"'{row[0]}'" for row in reference.region_rows())
DISCIPLINE_TYPES = "'Acuti', 'Riabilitazione', 'Lungodegenza'"


def raw_path() -> str:
    return spark.conf.get("lakehouse.raw_path")  # noqa: F821 - provided by the pipeline runtime


def gold_window() -> tuple[int, int]:
    from_year = int(spark.conf.get("lakehouse.from_year", str(GOLD_WINDOW_DEFAULT[0])))  # noqa: F821
    to_year = int(spark.conf.get("lakehouse.to_year", str(GOLD_WINDOW_DEFAULT[1])))  # noqa: F821
    return from_year, to_year


# --- bronze: raw files from the Unity Catalog volume ---------------------------------------------------------


@dp.materialized_view(
    name="bronze_beds",
    # Bronze keeps the Ministry headers as published ("Codice Regione", "N° Reparti", ...). Delta rejects
    # spaces and similar characters in column names unless column mapping is on; silver renames them.
    table_properties={"delta.columnMapping.mode": "name"},
)
def bronze_beds():
    return bronze.read_beds_csv(spark, f"{raw_path()}/beds_*.csv")  # noqa: F821


@dp.materialized_view(name="bronze_population")
def bronze_population():
    return bronze.read_population_csv(spark, f"{raw_path()}/population_istat.csv")  # noqa: F821


# --- silver: cleaned, typed, windowed ------------------------------------------------------------------------


@dp.materialized_view(name="silver_beds")
@dp.expect_all_or_fail(
    {
        "structure_code_present": "structure_code IS NOT NULL",
        "discipline_code_present": "discipline_code IS NOT NULL",
        "counts_present": (
            "ward_count IS NOT NULL AND beds_ordinary IS NOT NULL AND beds_paying IS NOT NULL "
            "AND beds_day_hospital IS NOT NULL AND beds_day_surgery IS NOT NULL AND beds_total IS NOT NULL"
        ),
        "counts_non_negative": (
            "ward_count >= 0 AND beds_ordinary >= 0 AND beds_paying >= 0 "
            "AND beds_day_hospital >= 0 AND beds_day_surgery >= 0 AND beds_total >= 0"
        ),
        "total_equals_sum_of_regimes": (
            "beds_total = beds_ordinary + beds_paying + beds_day_hospital + beds_day_surgery"
        ),
        "region_code_valid": f"region_code IN ({REGION_CODES})",
    }
)
def silver_beds():
    from_year, to_year = gold_window()
    return silver.to_silver_beds(spark.read.table("bronze_beds"), from_year, to_year)  # noqa: F821


@dp.materialized_view(name="silver_population")
@dp.expect_all_or_fail(
    {
        "region_code_present": "region_code IS NOT NULL",
        "population_at_least_one": "population >= 1",
    }
)
def silver_population():
    from_year, to_year = gold_window()
    return silver.to_silver_population(
        spark.read.table("bronze_population"),  # noqa: F821
        reference.load_regions(spark),  # noqa: F821
        from_year,
        to_year,
    )


# --- gold: the six contract tables ---------------------------------------------------------------------------


@dp.materialized_view(name="dim_year")
def dim_year():
    from_year, to_year = gold_window()
    return gold.build_dim_year(spark, from_year, to_year)  # noqa: F821


@dp.materialized_view(name="dim_region")
@dp.expect_all_or_fail({"region_code_present": "region_code IS NOT NULL"})
def dim_region():
    return gold.build_dim_region(reference.load_regions(spark))  # noqa: F821


@dp.materialized_view(name="dim_discipline")
@dp.expect_all_or_fail(
    {
        "discipline_code_present": "discipline_code IS NOT NULL",
        "discipline_type_known": f"discipline_type IN ({DISCIPLINE_TYPES})",
    }
)
def dim_discipline():
    return gold.build_dim_discipline(spark.read.table("silver_beds"))  # noqa: F821


@dp.materialized_view(name="dim_facility")
@dp.expect_all_or_fail(
    {
        "facility_key_positive": "facility_key >= 1",
        "facility_id_present": "facility_id IS NOT NULL",
        "health_authority_type_known": "health_authority_type IS NOT NULL",
        "span_ordered": "first_year <= last_year",
    }
)
def dim_facility():
    return gold.build_dim_facility(spark.read.table("silver_beds"))  # noqa: F821


@dp.materialized_view(name="fact_hospital_beds")
@dp.expect_all_or_fail(
    {
        "region_code_valid": f"region_code IN ({REGION_CODES})",
        "ward_count_non_negative": "ward_count >= 0",
        "beds_non_negative": (
            "beds_ordinary >= 0 AND beds_paying >= 0 AND beds_day_hospital >= 0 AND beds_day_surgery >= 0"
        ),
        "total_equals_sum_of_regimes": (
            "beds_total = beds_ordinary + beds_paying + beds_day_hospital + beds_day_surgery"
        ),
    }
)
def fact_hospital_beds():
    return gold.build_fact_hospital_beds(
        spark.read.table("silver_beds"),  # noqa: F821
        spark.read.table("dim_facility"),  # noqa: F821
    )


@dp.materialized_view(name="fact_population")
@dp.expect_all_or_fail(
    {
        "region_code_present": "region_code IS NOT NULL",
        "population_at_least_one": "population >= 1",
    }
)
def fact_population():
    return gold.build_fact_population(spark.read.table("silver_population"))  # noqa: F821
