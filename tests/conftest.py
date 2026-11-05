import os

import pytest


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    os.environ.setdefault("PYSPARK_PYTHON", "python3")
    s = (SparkSession.builder.master("local[2]").appName("citibike-tests")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .config("spark.sql.session.timeZone", "America/New_York")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
