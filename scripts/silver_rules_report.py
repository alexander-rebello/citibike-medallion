#!/usr/bin/env python3
"""Run the Silver step-1 rules over Bronze and report their impact (nothing is written to Silver).

  CITIBIKE_ENV=dev  python scripts/silver_rules_report.py
  CITIBIKE_ENV=prod CITIBIKE_SPARK_MASTER=spark://192.168.26.92:7077 python scripts/silver_rules_report.py

Writes <lake>/_meta/silver_rules/{impact,service_matches}.jsonl:
  impact:          per schema/year/month: rows, accepted, rows hit by each rule (independent) and by first reason, flags
  service_matches: which station id/name pairs the service_station rule hit (to check for false positives)
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyspark.sql import functions as F  # noqa: E402

from citibike import bronze, config, silver  # noqa: E402
from citibike.spark import get_spark  # noqa: E402

FLAGS = ["duration_mismatch", "birth_year_nulled", "is_incomplete", "is_valet", "outside_nyc"]


def dump(path, rows):
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r, default=str) + "\n")


def main():
    cfg = config.load()
    lake = cfg["paths"]["lake_root"]
    out = os.path.join(lake, "_meta", "silver_rules")
    os.makedirs(out, exist_ok=True)
    rules = silver.load_rules()
    spark = get_spark(cfg, "silver-rules")
    spark.sparkContext.setLogLevel("WARN")
    months = cfg["scope"]["months"]

    impact, service = [], []
    for schema in ("legacy_v1", "modern_v2"):
        df = spark.read.parquet(bronze.bronze_path(lake, schema))
        if months != "all":
            keep = [(int(m[:4]), int(m[5:])) for m in months]
            df = df.where(" or ".join(f"(year={y} and month={m})" for y, m in keep))
        s = silver.standardise(df, schema, rules).cache()
        aggs = [F.count(F.lit(1)).alias("rows"), F.sum((F.size("reject_reasons") == 0).cast("int")).alias("accepted")]
        aggs += [F.sum(F.array_contains("reject_reasons", r).cast("int")).alias(f"hit_{r}") for r in silver.REASONS]
        aggs += [F.sum(F.coalesce(F.col("reject_reason") == r, F.lit(False)).cast("int")).alias(f"first_{r}") for r in silver.REASONS]
        aggs += [F.sum(F.col(f).cast("int")).alias(f"flag_{f}") for f in FLAGS]
        impact += [r.asDict() for r in s.groupBy("schema", "year", "month").agg(*aggs).orderBy("year", "month").collect()]
        svc = (s.where(F.array_contains("reject_reasons", "service_station"))
               .groupBy("start_station_id", "start_station_name", "end_station_id", "end_station_name").count())
        service += [r.asDict() for r in svc.collect()]
        s.unpersist()
    dump(os.path.join(out, "impact.jsonl"), impact)
    dump(os.path.join(out, "service_matches.jsonl"), service)
    print(f"wrote {len(impact)} impact rows and {len(service)} service-station pairs to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
