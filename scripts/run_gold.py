#!/usr/bin/env python3
"""Build and publish Gold from accepted Silver trips. Publishes only if Silver's checks hold and the totals match.

  CITIBIKE_ENV=dev  python scripts/run_gold.py
  CITIBIKE_ENV=prod CITIBIKE_SPARK_MASTER=spark://192.168.26.92:7077 python scripts/run_gold.py

Exit code 1 (and nothing published; the previous Gold and its _READY stay) if a check fails.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from citibike import bronze, config, gold, silver_run  # noqa: E402
from citibike.spark import get_spark  # noqa: E402


def silver_checks(lake: str, scope) -> tuple[list[dict], int]:
    """Checks on the Silver ledger for the months in scope, and the number of accepted trips Gold must add up to."""
    ledger = silver_run.read_ledger(lake)
    months = sorted(bronze.read_ledger(lake)) if scope == "all" else sorted(scope)
    missing = [m for m in months if m not in ledger]
    unbalanced = [m for m in months if m in ledger
                  and ledger[m]["bronze_rows"] != ledger[m]["accepted"] + ledger[m]["rejected"] + ledger[m]["duplicates"]]
    checks = [{"name": "silver_published_for_every_month", "ok": not missing, "detail": f"missing: {missing[:5]}"},
              {"name": "silver_row_accounting", "ok": not unbalanced, "detail": f"unbalanced: {unbalanced[:5]}"}]
    return checks, sum(ledger[m]["accepted"] for m in months if m in ledger)


def main():
    cfg = config.load()
    lake = cfg["paths"]["lake_root"]
    checks, expected = silver_checks(lake, cfg["scope"]["months"])
    spark = get_spark(cfg, "gold")
    spark.sparkContext.setLogLevel("WARN")
    trips = spark.read.parquet(os.path.join(silver_run.root(lake), "trips"))
    stations = spark.read.parquet(os.path.join(silver_run.root(lake), "stations"))
    by_station = gold.station_year(trips, stations).cache()
    tables = {"gold_riders_daily": gold.riders_daily(trips), "gold_demand_hourly": gold.demand_hourly(trips),
              "gold_station_year": by_station, "gold_station_bands": gold.station_bands(by_station)}
    try:
        marker = gold.publish(spark, lake, bronze.new_run_id(), tables, expected, checks)
    except gold.GoldCheckFailed as e:
        print(f"NOT PUBLISHED: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)
    print(f"published gold run {marker['run_id']}: {marker['trips']:,} trips in {marker['tables']}")
    spark.stop()


if __name__ == "__main__":
    main()
