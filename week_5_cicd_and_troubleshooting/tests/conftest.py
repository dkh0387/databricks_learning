"""Shared pytest fixtures: a local SparkSession and the bundle root on sys.path
(so `from src.transform import ...` works when running `pytest tests/` from the bundle root)."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
        .master("local[2]")
        .appName("bundle-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate())
    yield session
    session.stop()