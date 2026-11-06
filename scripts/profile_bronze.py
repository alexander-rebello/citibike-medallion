#!/usr/bin/env python3
"""Profile the Bronze lake and write the results to <lake>/_meta/profile/ (JSON lines).

  CITIBIKE_ENV=prod python scripts/profile_bronze.py
  CITIBIKE_ENV=prod CITIBIKE_SPARK_MASTER=spark://192.168.26.92:7077 python scripts/profile_bronze.py   # on the cluster

values.jsonl:   schema, year, month, column, value, n   (shape/class counts per column and month)
distinct.jsonl: schema, year, rows, approx distinct station ids/names/bikes
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from citibike import bronze, config, profiling  # noqa: E402
from citibike.spark import get_spark  # noqa: E402


def write_jsonl(path, df, order):
    rows = [r.asDict() for r in df.orderBy(*order).collect()]
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r, default=str) + "\n")
    return len(rows)


def main():
    cfg = config.load()
    lake = cfg["paths"]["lake_root"]
    out = os.path.join(lake, "_meta", "profile")
    os.makedirs(out, exist_ok=True)
    spark = get_spark(cfg, "profile")
    spark.sparkContext.setLogLevel("WARN")

    values, distinct = None, None
    for schema in profiling.SPECS:
        df = spark.read.parquet(bronze.bronze_path(lake, schema))
        v, d = profiling.profile_values(df, schema), profiling.profile_distinct(df, schema)
        values = v if values is None else values.unionByName(v)
        distinct = d if distinct is None else distinct.unionByName(d, allowMissingColumns=True)

    n = write_jsonl(os.path.join(out, "values.jsonl"), values, ["schema", "column", "year", "month", "value"])
    m = write_jsonl(os.path.join(out, "distinct.jsonl"), distinct, ["schema", "year"])
    print(f"wrote {n} value rows and {m} distinct rows to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
