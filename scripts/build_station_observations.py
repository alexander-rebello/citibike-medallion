#!/usr/bin/env python3
"""Collect every station occurrence in Bronze into <lake>/silver/station_observations (small Parquet).

  CITIBIKE_ENV=prod CITIBIKE_SPARK_MASTER=spark://192.168.26.92:7077 python scripts/build_station_observations.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from citibike import bronze, config, silver, stations  # noqa: E402
from citibike.spark import get_spark  # noqa: E402


def main():
    cfg = config.load()
    lake = cfg["paths"]["lake_root"]
    rules = silver.load_rules()
    spark = get_spark(cfg, "station-observations")
    spark.sparkContext.setLogLevel("WARN")
    parts = [stations.observations(spark.read.parquet(bronze.bronze_path(lake, s)), s, rules) for s in stations.SIDES]
    out = parts[0].unionByName(parts[1])
    path = os.path.join(lake, "silver", "station_observations")
    out.coalesce(1).write.mode("overwrite").parquet(path)
    print(f"wrote {spark.read.parquet(path).count()} station observations to {path}")
    spark.stop()


if __name__ == "__main__":
    main()
