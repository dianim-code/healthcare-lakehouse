"""Test fixtures. The Spark backend is chosen with the SPARK_BACKEND environment variable.

- ``local`` (default, used in CI): a local Spark session. Needs Java and the ``local`` dependency group.
- ``databricks``: Databricks Connect on serverless compute. The profile comes from DATABRICKS_CONFIG_PROFILE
  and is never written into this repository.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


def backend_name() -> str:
    """Return the selected backend, or fail with a clear message."""
    name = os.environ.get("SPARK_BACKEND", "local")
    if name not in {"local", "databricks"}:
        raise pytest.UsageError(f"SPARK_BACKEND must be 'local' or 'databricks', got {name!r}")
    return name


@pytest.fixture(scope="session")
def backend() -> str:
    return backend_name()


@pytest.fixture(scope="session")
def spark(backend: str) -> Iterator[SparkSession]:
    session: SparkSession
    if backend == "local":
        session = SparkSession.builder.master("local[2]").appName("healthcare-lakehouse-tests").getOrCreate()
    else:
        from databricks.connect import DatabricksSession

        profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
        if not profile:
            raise pytest.UsageError("SPARK_BACKEND=databricks needs DATABRICKS_CONFIG_PROFILE to be set")
        session = DatabricksSession.builder.profile(profile).serverless(True).getOrCreate()
    yield session
    if backend == "local":
        session.stop()
