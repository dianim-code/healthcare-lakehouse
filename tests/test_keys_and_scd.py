"""Unit tests for silver windowing, facility_id (C2), SCD type 1 (C3), facility_key (C4), C5 and C6."""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from support import raw_bed_row, raw_beds_frame

from healthcare_lakehouse.gold import (
    build_dim_discipline,
    build_dim_facility,
    build_dim_year,
)
from healthcare_lakehouse.silver import to_silver_beds


def silver(spark: SparkSession, rows: list[dict[str, str]], from_year: int = 2019, to_year: int = 2023) -> DataFrame:
    return to_silver_beds(raw_beds_frame(spark, rows), from_year, to_year)


def facility_rows(df: DataFrame) -> dict[str, dict]:
    return {r["facility_id"]: r.asDict() for r in df.collect()}


def test_facility_id_uses_structure_code_or_structure_and_subcode(spark: SparkSession) -> None:
    """C2: single-site structures keep the structure code; multi-site ones add '-' and the subcode."""
    beds = silver(
        spark,
        [
            raw_bed_row(structure_code="010030", site_subcode=""),
            raw_bed_row(structure_code="010030", site_subcode="01"),
        ],
    )
    ids = sorted(r["facility_id"] for r in beds.select("facility_id").collect())
    assert ids == ["010030", "010030-01"]


def test_silver_window_keeps_only_the_gold_years(spark: SparkSession) -> None:
    beds = silver(
        spark,
        [raw_bed_row(year="2018"), raw_bed_row(year="2019"), raw_bed_row(year="2023"), raw_bed_row(year="2024")],
        from_year=2019,
        to_year=2023,
    )
    assert sorted(r["year"] for r in beds.select("year").collect()) == [2019, 2023]


def test_facility_key_is_the_ascending_rank_of_facility_id(spark: SparkSession) -> None:
    """C4: keys 1..N in ascending facility_id order, whatever the input order."""
    beds = silver(
        spark,
        [
            raw_bed_row(structure_code="030001"),
            raw_bed_row(structure_code="010002"),
            raw_bed_row(structure_code="010001"),
        ],
    )
    dim = build_dim_facility(beds).collect()
    by_id = {r["facility_id"]: r["facility_key"] for r in dim}
    assert by_id == {"010001": 1, "010002": 2, "030001": 3}


def test_scd1_takes_the_whole_row_from_the_latest_year(spark: SparkSession) -> None:
    """C3: the 2022 row wins as a whole. Its null address must not be filled from the 2021 row."""
    beds = silver(
        spark,
        [
            raw_bed_row(year="2021", facility_name="NOME VECCHIO", address="VIA VECCHIA 9", discipline_code="08"),
            raw_bed_row(year="2022", facility_name="NOME NUOVO", address="", discipline_code="08"),
        ],
    )
    site = facility_rows(build_dim_facility(beds))["010001"]
    assert site["facility_name"] == "NOME NUOVO"
    assert site["address"] is None
    assert (site["first_year"], site["last_year"]) == (2021, 2022)


def test_scd1_tie_within_a_year_is_broken_by_discipline_code(spark: SparkSession) -> None:
    """Within the latest year, the row with the highest discipline code is the one kept."""
    beds = silver(
        spark,
        [
            raw_bed_row(year="2023", discipline_code="08", facility_name="DA SCARTARE"),
            raw_bed_row(year="2023", discipline_code="09", facility_name="DA TENERE"),
        ],
    )
    assert facility_rows(build_dim_facility(beds))["010001"]["facility_name"] == "DA TENERE"


def test_health_authority_id_and_type_label(spark: SparkSession) -> None:
    """C5: the authority code repeats across regions, so the id is region_code + authority code."""
    beds = silver(
        spark,
        [
            raw_bed_row(structure_code="010001", region_code="010", authority_code="003", authority_type_code="1"),
            raw_bed_row(structure_code="150001", region_code="150", authority_code="003", authority_type_code="2"),
        ],
    )
    rows = facility_rows(build_dim_facility(beds))
    assert rows["010001"]["health_authority_id"] == "010003"
    assert rows["010001"]["health_authority_type"] == "Azienda sanitaria locale (ASL)"
    assert rows["150001"]["health_authority_id"] == "150003"
    assert rows["150001"]["health_authority_type"] == "Struttura sanitaria aziendale di tipo ospedaliero"


def test_discipline_type_is_sentence_case_and_scd1(spark: SparkSession) -> None:
    """C6: ACUTI becomes Acuti. C3: the name and type come from the latest year."""
    beds = silver(
        spark,
        [
            raw_bed_row(year="2021", discipline_code="28", discipline_name="VECCHIA", discipline_type="RIABILITAZIONE"),
            raw_bed_row(
                year="2023", discipline_code="28", discipline_name="RIABILITAZIONE", discipline_type="RIABILITAZIONE"
            ),
            raw_bed_row(year="2023", discipline_code="08", discipline_name="CARDIOLOGIA", discipline_type="ACUTI"),
        ],
    )
    rows = {r["discipline_code"]: r.asDict() for r in build_dim_discipline(beds).collect()}
    assert rows["28"] == {
        "discipline_code": "28",
        "discipline_name": "RIABILITAZIONE",
        "discipline_type": "Riabilitazione",
    }
    assert rows["08"]["discipline_type"] == "Acuti"


def test_dim_year_marks_only_the_latest_year(spark: SparkSession) -> None:
    rows = build_dim_year(spark, 2021, 2023).orderBy("year").collect()
    assert [r["year"] for r in rows] == [2021, 2022, 2023]
    assert [r["is_latest_year"] for r in rows] == [False, False, True]
    assert rows[0]["reference_date"] == date(2021, 1, 1)
