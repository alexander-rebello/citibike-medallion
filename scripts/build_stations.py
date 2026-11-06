#!/usr/bin/env python3
"""Build the station dimension from <lake>/silver/station_observations.

  CITIBIKE_ENV=prod CITIBIKE_SPARK_MASTER=local[8] python scripts/build_stations.py

Writes <lake>/silver/{stations,station_map,station_conflicts,station_coord_ambiguous} and prints a summary.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyspark.sql import functions as F  # noqa: E402

from citibike import config, silver, stations  # noqa: E402
from citibike.spark import get_spark  # noqa: E402


def main():
    cfg = config.load()
    root = os.path.join(cfg["paths"]["lake_root"], "silver")
    spark = get_spark(cfg, "stations")
    spark.sparkContext.setLogLevel("WARN")
    obs = spark.read.parquet(os.path.join(root, "station_observations"))
    out = stations.build(obs, silver.load_rules())
    for name, df in {"stations": out["stations"], "station_map": out["station_map"], "station_conflicts": out["conflicts"],
                     "station_coord_ambiguous": out["coord_ambiguous"]}.items():
        df.coalesce(1).write.mode("overwrite").parquet(os.path.join(root, name))
    s = spark.read.parquet(os.path.join(root, "stations"))
    print("stations:", s.count())
    s.groupBy("era").agg(F.count("*").alias("stations"), F.sum("n_trips").alias("occurrences")).orderBy("era").show(truncate=False)
    print("matched by position:", s.where("matched_by_position").count(), "| with id/name conflict:", s.where("has_conflict").count(),
          "| valet:", s.where("is_valet").count(), "| outside NYC:", s.where("outside_nyc").count())
    print("conflict pairs:", spark.read.parquet(os.path.join(root, "station_conflicts")).count(),
          "| ambiguous position candidates:", spark.read.parquet(os.path.join(root, "station_coord_ambiguous")).count())
    spark.stop()


if __name__ == "__main__":
    main()
