"""Unit tests for rule C1 (text cleaning)."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from healthcare_lakehouse.cleaning import clean_text


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("  VALLE D`AOSTA  ", "VALLE D'AOSTA"),
        ("OSPEDALE\u00a0MARIA\u00a0\u00a0VITTORIA", "OSPEDALE MARIA VITTORIA"),
        ("\u00a0LEADING AND TRAILING\u00a0", "LEADING AND TRAILING"),
        ("A\t\r\n  B", "A B"),
        ("A \t \r\n B", "A B"),
        ("Mixed Case Kept", "Mixed Case Kept"),
        ("   ", None),
        ("\u00a0\u00a0", None),
        ("", None),
        (None, None),
    ],
)
def test_clean_text_applies_rule_c1(spark: SparkSession, raw: str | None, expected: str | None) -> None:
    df = spark.createDataFrame([(raw,)], "value string")
    result = df.select(clean_text(F.col("value")).alias("value")).collect()[0]["value"]
    assert result == expected


def test_no_break_space_inside_a_name_becomes_a_plain_space(spark: SparkSession) -> None:
    """The 2021-2023 files contain U+00A0 in one hospital name. Spark trim() would leave it in place."""
    df = spark.createDataFrame([("OSPEDALE\u00a0DI\u00a0ZONA",)], "value string")
    result = df.select(clean_text(F.col("value")).alias("value")).collect()[0]["value"]
    assert result == "OSPEDALE DI ZONA"
    assert "\u00a0" not in result
