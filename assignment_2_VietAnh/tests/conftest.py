import os

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder 
        .appName("pytest") \
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield spark

    spark.stop()

@pytest.fixture(scope="session")
def dag_folder():
    conftest_dir  = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(conftest_dir, "..", "dags")
