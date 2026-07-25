"""Shared pytest fixtures — a local SparkSession for the transform tests."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """A single local-mode SparkSession for the whole test session."""
    session = (
        SparkSession.builder.appName("dfs-conform-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
