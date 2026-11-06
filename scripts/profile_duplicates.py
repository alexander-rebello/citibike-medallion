#!/usr/bin/env python3
"""Measure duplicates in Bronze before writing the dedupe rule.

  modern_v2: rows sharing a ride id. legacy_v1: rows sharing (bike id, start time).
For each, how many are exact copies, how many conflict (same key, different content) and how many cross a month boundary.
Writes <lake>/_meta/profile/duplicates.json.
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyspark.sql import functions as F  # noqa: E402

from citibike import bronze, config  # noqa: E402
from citibike.spark import get_spark  # noqa: E402


def summarise(df, key, content):
    ym = F.col("year") * 100 + F.col("month")
    g = (df.where(F.concat_ws("", *key) != "")
         .groupBy(*key).agg(F.count(F.lit(1)).alias("n"), F.countDistinct(ym).alias("months"),
                            F.countDistinct(*content).alias("variants")).where("n > 1").cache())
    r = g.agg(F.count(F.lit(1)).alias("keys_with_duplicates"), F.sum(F.col("n") - 1).alias("extra_rows"),
              F.sum(F.when(F.col("variants") == 1, F.col("n") - 1).otherwise(0)).alias("extra_rows_exact_copies"),
              F.sum(F.when(F.col("variants") > 1, F.col("n") - 1).otherwise(0)).alias("extra_rows_conflicting"),
              F.sum(F.when(F.col("months") > 1, F.col("n") - 1).otherwise(0)).alias("extra_rows_across_months"),
              F.max("n").alias("max_copies")).first().asDict()
    by_year = {str(x["year"]): x["extra"] for x in
               df.where(F.concat_ws("", *key) != "").join(g.select(*key), list(key))
               .groupBy("year").agg(F.count(F.lit(1)).alias("rows_in_dup_keys")).withColumnRenamed("rows_in_dup_keys", "extra")
               .collect()}
    r["rows_in_duplicated_keys_by_year"] = by_year
    return {k: (int(v) if isinstance(v, (int,)) or hasattr(v, "__int__") and not isinstance(v, dict) else v) for k, v in r.items()}


def main():
    cfg = config.load()
    lake = cfg["paths"]["lake_root"]
    spark = get_spark(cfg, "duplicates")
    spark.sparkContext.setLogLevel("WARN")
    res = {}
    modern = spark.read.parquet(bronze.bronze_path(lake, "modern_v2")).select(
        "rideid", "startedat", "endedat", "startstationid", "endstationid", "membercasual", "rideabletype", "year", "month")
    res["modern_by_ride_id"] = summarise(modern, ["rideid"], ["startedat", "endedat", "startstationid", "endstationid",
                                                              "membercasual", "rideabletype"])
    legacy = spark.read.parquet(bronze.bronze_path(lake, "legacy_v1")).select(
        "bikeid", "starttime", "stoptime", "startstationid", "endstationid", "tripduration", "usertype", "year", "month")
    res["legacy_by_bike_and_start"] = summarise(legacy, ["bikeid", "starttime"], ["stoptime", "startstationid", "endstationid",
                                                                                    "tripduration", "usertype"])
    out = os.path.join(lake, "_meta", "profile", "duplicates.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(res, f, indent=1)
    print(json.dumps(res, indent=1))
    spark.stop()


if __name__ == "__main__":
    main()
