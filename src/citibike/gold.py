"""Gold: small tables that answer the question, built only from accepted Silver trips, published behind a gate.

  gold_riders_daily  (date, rider type, bike type)  -> trips, avg duration, avg distance
  gold_demand_hourly (station, date, hour)          -> trips, avg duration

Publishing: build into gold/_staging/<run_id>/, check the totals against the accepted Silver rows, and only then swap
the tables into gold/ and write gold/_READY. A failed check leaves the previously published tables and _READY untouched.
Consumers (the dashboard) read Gold only when _READY exists.
"""
import json
import os
import shutil
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F


class GoldCheckFailed(Exception):
    """Silver checks or Gold totals did not hold; nothing was published."""


def _with_partitions(df: DataFrame) -> DataFrame:
    return df.withColumn("year", F.year("start_date")).withColumn("month", F.month("start_date"))


def riders_daily(trips: DataFrame) -> DataFrame:
    return _with_partitions(
        trips.groupBy("start_date", "rider_type", "bike_type")
        .agg(F.count(F.lit(1)).alias("trips"),
             F.round(F.avg("duration_s"), 1).alias("avg_duration_s"),
             F.round(F.avg("distance_m"), 1).alias("avg_distance_m")))


def demand_hourly(trips: DataFrame, station_col: str = "start_station_key") -> DataFrame:
    return _with_partitions(
        trips.groupBy(station_col, "start_date", "start_hour")
        .agg(F.count(F.lit(1)).alias("trips"), F.round(F.avg("duration_s"), 1).alias("avg_duration_s")))


def total_trips(gold: DataFrame) -> int:
    return gold.agg(F.sum("trips")).first()[0] or 0


def publish(spark, lake_root: str, run_id: str, tables: dict[str, DataFrame], expected_trips: int,
            silver_checks: list[dict]) -> dict:
    """Stage, verify, swap, mark ready. `silver_checks`: [{"name": ..., "ok": bool}]. Raises GoldCheckFailed."""
    failed = [c["name"] for c in silver_checks if not c["ok"]]
    if failed:
        raise GoldCheckFailed(f"Silver checks failed, nothing published: {failed}")

    gold = os.path.join(lake_root, "gold")
    staging = os.path.join(gold, "_staging", run_id)
    shutil.rmtree(staging, ignore_errors=True)
    rows = {}
    for name, df in tables.items():
        df.write.mode("overwrite").partitionBy("year", "month").parquet(os.path.join(staging, name))
        rows[name] = total_trips(spark.read.parquet(os.path.join(staging, name)))
    bad = {n: (r, expected_trips) for n, r in rows.items() if r != expected_trips}
    if bad:
        raise GoldCheckFailed(f"trip totals differ from accepted Silver rows (gold, silver): {bad}; staging kept at {staging}")

    for name in tables:  # swap: the old table is renamed away first, then the staged one moved in
        final, old = os.path.join(gold, name), os.path.join(gold, f"_old_{run_id}_{name}")
        if os.path.exists(final):
            os.rename(final, old)
        os.rename(os.path.join(staging, name), final)
        shutil.rmtree(old, ignore_errors=True)
    shutil.rmtree(staging, ignore_errors=True)

    marker = {"run_id": run_id, "published_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
              "tables": sorted(tables), "trips": expected_trips, "silver_checks": silver_checks}
    tmp = os.path.join(gold, "_READY.tmp")
    with open(tmp, "w") as f:
        json.dump(marker, f, indent=1)
    os.replace(tmp, os.path.join(gold, "_READY"))
    return marker
