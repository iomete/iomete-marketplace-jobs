"""Shared test fixtures for spark-based detection."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .appName("orphaned-asset-detection-tests")
        .getOrCreate()
    )
    yield session
    session.stop()
