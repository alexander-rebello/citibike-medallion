"""Gold: small tables that answer the question, built only from accepted Silver trips, published behind a gate.

  gold_riders_daily  (date, rider type, bike type)  -> trips, avg duration, avg distance
  gold_demand_hourly (station, date, hour)          -> trips, avg duration
  gold_station_year  (year, station)                -> trips started, rank, distance from Midtown, band
  gold_station_bands (year, band)                   -> trips and stations per distance band from Midtown

Publishing: build into gold/_staging/<run_id>/, check the totals against the accepted Silver rows, and only then swap
the tables into gold/ and write gold/_READY. A failed check leaves the previously published tables and _READY untouched.
Consumers (the dashboard) read Gold only when _READY exists.
"""
import json
import os
import shutil
from datetime import datetime, timezone

from pyspark.sql import DataFrame, Window, functions as F

from citibike import silver


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


MIDTOWN = (40.7580, -73.9855)           # Times Square: the reference point for "central Manhattan"
BANDS = [(2000, "0-2 km"), (5000, "2-5 km"), (10000, "5-10 km")]


def distance_band(dist_m) -> "Column":
    expr = F.when(dist_m.isNull(), F.lit("unknown"))
    for limit, name in BANDS:
        expr = expr.when(dist_m < limit, F.lit(name))
    return expr.otherwise(F.lit("10+ km"))


def station_year(trips: DataFrame, stations: DataFrame) -> DataFrame:
    """Trips started per station and year. Rank (1 = busiest) only among regular NYC stations: valet stations and
    Jersey City / Hoboken stations keep their row (so totals add up) but get no rank and the band 'valet' / 'outside_nyc'."""
    st = F.broadcast(stations.select("station_key", "station_name", "lat", "lng", "is_valet", "outside_nyc"))
    per = (trips.groupBy(F.year("start_date").alias("year"), F.col("start_station_key").alias("station_key"))
           .agg(F.count(F.lit(1)).alias("trips_started")).join(st, "station_key", "left"))
    dist = silver.haversine_m(F.col("lat"), F.col("lng"), F.lit(MIDTOWN[0]), F.lit(MIDTOWN[1]))
    eligible = ~F.coalesce(F.col("is_valet"), F.lit(False)) & ~F.coalesce(F.col("outside_nyc"), F.lit(False)) & F.col("lat").isNotNull()
    by_year = Window.partitionBy("year")
    ranked = Window.partitionBy("year").orderBy(eligible.cast("int").desc(), F.desc("trips_started"), "station_key")
    return (per.withColumn("dist_midtown_m", dist).withColumn("_eligible", eligible)
            .withColumn("band", F.when(F.col("is_valet"), F.lit("valet")).when(F.col("outside_nyc"), F.lit("outside_nyc"))
                        .otherwise(distance_band(F.col("dist_midtown_m"))))
            .withColumn("rank", F.when(F.col("_eligible"), F.row_number().over(ranked)))
            .withColumn("share_of_year_pct", F.round(100 * F.col("trips_started") / F.sum("trips_started").over(by_year), 3))
            .drop("_eligible"))


def station_bands(station_year_df: DataFrame) -> DataFrame:
    """Per year and band: trips, stations and share of the year's trips."""
    out = (station_year_df.groupBy("year", "band")
           .agg(F.sum("trips_started").alias("trips"), F.count(F.lit(1)).alias("stations")))
    return out.withColumn("share_of_year_pct", F.round(100 * F.col("trips") / F.sum("trips").over(Window.partitionBy("year")), 2))


def total_trips(gold: DataFrame) -> int:
    return gold.agg(F.sum("trips")).first()[0] or 0   # station_year uses "trips_started": see total_started


def total_started(df: DataFrame) -> int:
    return df.agg(F.sum("trips_started")).first()[0] or 0


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
        parts = [c for c in ("year", "month") if c in df.columns]
        if parts:   # one file per partition instead of one per (shuffle task, partition): docs/performance.md §5
            df = df.repartition(*parts)
        df.write.mode("overwrite").partitionBy(*parts).parquet(os.path.join(staging, name))
        staged = spark.read.parquet(os.path.join(staging, name))
        rows[name] = total_started(staged) if "trips_started" in staged.columns else total_trips(staged)
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
