"""SparkSession built from the environment config."""
from pyspark.sql import SparkSession


def get_spark(cfg: dict, app: str) -> SparkSession:
    sc = cfg.get("spark", {})
    return (
        SparkSession.builder.appName(f"citibike-{cfg['env']}-{app}")
        .master(sc.get("master", "local[*]"))
        .config("spark.driver.memory", sc.get("driver_memory", "4g"))
        .config("spark.sql.shuffle.partitions", str(sc.get("shuffle_partitions", 16)))
        # rewrite only the year/month partitions a run touches
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", sc.get("timezone", "America/New_York"))
        .config("spark.sql.parquet.compression.codec", "zstd")
        .getOrCreate()
    )
