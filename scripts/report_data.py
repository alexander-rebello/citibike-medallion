#!/usr/bin/env python3
"""Export the aggregates the report's figures use, from published Gold only (same query as the dashboard).

  CITIBIKE_ENV=prod CITIBIKE_SPARK_MASTER=local[4] python scripts/report_data.py   # -> docs/report/data/gold_summary.json
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from build_dashboard import aggregate  # noqa: E402
from citibike import config  # noqa: E402
from citibike.spark import get_spark  # noqa: E402

OUT = os.path.join(os.path.dirname(__file__), "..", "docs", "report", "data", "gold_summary.json")


def main():
    cfg = config.load()
    gold = os.path.join(cfg["paths"]["lake_root"], "gold")
    if not os.path.exists(os.path.join(gold, "_READY")):
        sys.exit("gold/_READY is missing")
    spark = get_spark(cfg, "report-data")
    spark.sparkContext.setLogLevel("WARN")
    data = aggregate(spark, gold)
    spark.stop()
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(data, f, indent=1)
    print(f"wrote {OUT} from gold run {data['provenance']['run_id']}")


if __name__ == "__main__":
    main()
