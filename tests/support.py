"""Shared test helpers: repository paths, raw-row factories and the backend-aware raw readers.

Imported by the tests as ``from support import ...`` (pytest puts this directory on ``sys.path``).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from healthcare_lakehouse.config import BEDS_COLUMNS

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "data" / "raw"
GOLD_SAMPLE_DIR = REPO_ROOT / "sample-data" / "gold"
CONTRACT_PATH = REPO_ROOT / "docs" / "contract" / "gold_schema.yml"

# Staging name -> default raw value. An empty string means missing and becomes null after rule C1.
BED_DEFAULTS: dict[str, str] = {
    "year": "2021",
    "region_code": "010",
    "region_name_src": "Piemonte",
    "authority_code": "001",
    "authority_type_code": "1",
    "structure_code": "010001",
    "site_subcode": "",
    "facility_name": "OSPEDALE A",
    "address": "VIA ROMA 1",
    "municipality_code": "001272",
    "municipality_name": "TORINO",
    "province_code": "TO",
    "facility_type_code": "1",
    "facility_type_name": "Ospedale a gestione diretta",
    "discipline_code": "08",
    "discipline_name": "CARDIOLOGIA",
    "discipline_type": "ACUTI",
    "ward_count": "3",
    "beds_ordinary": "10",
    "beds_paying": "0",
    "beds_day_hospital": "2",
    "beds_day_surgery": "1",
    "beds_total": "13",
}


def raw_bed_row(**overrides: str) -> dict[str, str]:
    """One beds row keyed by the Ministry header names, as the CSV reader returns it."""
    staged = {**BED_DEFAULTS, **overrides}
    return {source: staged[target] for source, target in BEDS_COLUMNS.items()}


def raw_beds_frame(spark: SparkSession, rows: list[dict[str, str]]) -> DataFrame:
    """A raw beds DataFrame (all string columns, Ministry header names) built from ``raw_bed_row`` dicts."""
    headers = list(BEDS_COLUMNS)
    schema = T.StructType([T.StructField(h, T.StringType(), True) for h in headers])
    return spark.createDataFrame([tuple(r[h] for h in headers) for r in rows], schema=schema)


def raw_population_frame(spark: SparkSession, rows: list[dict[str, Any]]) -> DataFrame:
    """A raw ISTAT DataFrame with the SDMX columns the pipeline reads."""
    schema = T.StructType([T.StructField(c, T.StringType(), True) for c in ("REF_AREA", "TIME_PERIOD", "OBS_VALUE")])
    return spark.createDataFrame(
        [(str(r["REF_AREA"]), str(r["TIME_PERIOD"]), str(r["OBS_VALUE"])) for r in rows], schema=schema
    )


def beds_raw_frame(spark: SparkSession, paths: list[Path], backend: str) -> DataFrame:
    """Raw beds rows for the given Ministry files.

    On the local backend the production reader (``read_beds_csv``) runs, so CI tests the real CSV parsing.
    Databricks Connect runs readers on the server, which cannot see local paths, so the same files are
    parsed with pandas (the reference settings of scripts/build_sample.py) and sent as a DataFrame.
    """
    from healthcare_lakehouse.bronze import read_beds_csv

    if backend == "local":
        return read_beds_csv(spark, [str(p) for p in paths])
    frames = []
    for path in paths:
        pdf = pd.read_csv(path, sep=";", encoding="latin-1", dtype=str, keep_default_na=False)
        pdf.columns = [c.strip() for c in pdf.columns]
        frames.append(pdf)
    return spark.createDataFrame(pd.concat(frames, ignore_index=True))


def population_raw_frame_from_file(spark: SparkSession, path: Path, backend: str) -> DataFrame:
    """Raw ISTAT rows read from a file, with the same split as ``beds_raw_frame``."""
    from healthcare_lakehouse.bronze import read_population_csv

    if backend == "local":
        return read_population_csv(spark, str(path))
    return spark.createDataFrame(pd.read_csv(path, dtype=str, keep_default_na=False))
