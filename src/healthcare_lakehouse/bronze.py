"""Bronze: raw files from the Unity Catalog volume, read as strings with no inference.

The readers are the only I/O of the package. Everything downstream is a DataFrame-to-DataFrame function.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession


def read_beds_csv(spark: SparkSession, paths: str | Sequence[str]) -> DataFrame:
    """Read Ministry beds CSV files (``;``-separated, ISO-8859-1, padded text) as strings.

    ``paths`` is one path, a glob such as ``/Volumes/.../raw/beds_*.csv``, or a list of paths. Header names
    are stripped, as in the pandas reference. ``escape`` is ``"`` so that doubled quotes inside a value are
    read as one quote.
    """
    reader = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("encoding", "ISO-8859-1")
        .option("quote", '"')
        .option("escape", '"')
    )
    raw = reader.csv([paths] if isinstance(paths, str) else list(paths))
    return raw.toDF(*[name.strip() for name in raw.columns])


def read_population_csv(spark: SparkSession, path: str) -> DataFrame:
    """Read the ISTAT SDMX-CSV population file as strings."""
    return spark.read.option("header", True).csv(path)
