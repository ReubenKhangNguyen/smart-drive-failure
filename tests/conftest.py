from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession  # imported here: tests that need no Spark also run where pyspark is absent (Airflow venv)

    session = (
        SparkSession.builder.master("local[2]")
        .appName("smart-drive-failure-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()
