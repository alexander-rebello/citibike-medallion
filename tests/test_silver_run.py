"""Silver run end to end on a tiny lake: duplicates across months, a bike starting twice, station checks, re-runs."""
import json

import pytest
from pyspark.sql import functions as F

from citibike import bronze, silver_run

LEG = ("tripduration starttime stoptime startstationid startstationname startstationlatitude startstationlongitude "
       "endstationid endstationname endstationlatitude endstationlongitude bikeid usertype birthyear gender").split()
MOD = ("rideid rideabletype startedat endedat startstationname startstationid endstationname endstationid startlat startlng "
       "endlat endlng membercasual").split()
META = ["_corrupt_record", "_source_file", "_header_fingerprint", "_schema_version", "_run_id", "_ingested_at"]

LEG_ROW = dict(tripduration="600", starttime="1/15/2015 8:00", stoptime="1/15/2015 8:10", startstationid="100",
               startstationname="S1", startstationlatitude="40.70", startstationlongitude="-74.00", endstationid="200",
               endstationname="S2", endstationlatitude="40.71", endstationlongitude="-74.00", bikeid="1", usertype="Subscriber",
               birthyear="1980", gender="1")
MOD_ROW = dict(rideid="R0", rideabletype="classic_bike", startedat="2024-06-10 08:00:00.000", endedat="2024-06-10 08:10:00.000",
               startstationname="Alpha", startstationid="6000.01", endstationname="Beta", endstationid="6000.02",
               startlat="40.70", startlng="-74.00", endlat="40.71", endlng="-74.00", membercasual="member")
SMAP = [("legacy_v1", "100", "S1", 1), ("legacy_v1", "200", "S2", 2), ("modern_v2", "6000.01", "Alpha", 3), ("modern_v2", "6000.02", "Beta", 4)]


def write_bronze(spark, lake, schema, month_rows):
    """month_rows: {"2024-06": [dict, ...]}. Writes Bronze Parquet and returns ledger entries."""
    cols, ledger, frames = (LEG if schema == "legacy_v1" else MOD), {}, []
    for month, rows in month_rows.items():
        y, m = int(month[:4]), int(month[5:])
        data = [tuple({**(LEG_ROW if schema == "legacy_v1" else MOD_ROW), **r}[c] for c in cols)
                + (None, f"{month}.csv.gz", "fp", schema, "r", "t", y, m) for r in rows]
        frames.append(spark.createDataFrame(data, ", ".join(f"{c} string" for c in cols + META) + ", year int, month int"))
        ledger[month] = {"digest": f"d-{month}-{len(rows)}", "schema": schema, "rows": len(rows), "corrupt_rows": 0, "run_id": "r"}
    df = frames[0]
    for f in frames[1:]:
        df = df.unionByName(f)
    df.write.mode("append").partitionBy("year", "month").parquet(bronze.bronze_path(lake, schema))
    return ledger


def setup(spark, tmp_path, smap=SMAP, **schemas):
    lake = str(tmp_path)
    ledger = {}
    for schema, month_rows in schemas.items():
        ledger.update(write_bronze(spark, lake, schema, month_rows))
    bronze.write_ledger(lake, {**bronze.read_ledger(lake), **ledger})
    spark.createDataFrame([(s, i, n, k, 1, False) for s, i, n, k in smap],
                          "schema string, station_id string, station_name string, station_key long, n long, by_position boolean") \
        .write.mode("overwrite").parquet(f"{lake}/silver/station_map")
    return {"env": "test", "paths": {"lake_root": lake}, "scope": {"months": "all"}}


def table(spark, cfg, name):
    t = silver_run.read_table(spark, f"{cfg['paths']['lake_root']}/silver/{name}")
    return t if t is not None else spark.createDataFrame([], "trip_id string")


def test_modern_trip_in_two_month_files_is_kept_once_and_the_accounting_holds(spark, tmp_path):
    boundary = dict(rideid="RB", startedat="2024-06-30 23:58:00.000", endedat="2024-07-01 00:10:00.000")
    cfg = setup(spark, tmp_path, modern_v2={
        "2024-06": [dict(rideid="R1"), boundary, dict(rideid="R3", endedat="2024-06-10 08:00:30.000")],   # R3 is 30 s long
        "2024-07": [dict(boundary, endlat="40.7100001"), dict(rideid="R4", startedat="2024-07-02 09:00:00.000", endedat="2024-07-02 09:30:00.000")]})
    res = silver_run.run(spark, cfg, run_id="run1")
    assert res["failed"] == [] and res["processed"] == ["2024-06", "2024-07"]
    trips, rej, dup = table(spark, cfg, "trips"), table(spark, cfg, "rejected"), table(spark, cfg, "duplicates")
    assert sorted(r.trip_id for r in trips.collect()) == ["R1", "R4", "RB"]          # one copy of the boundary trip
    assert [(r.trip_id, r.reject_reason) for r in rej.collect()] == [("R3", "duration_lt_60s")]
    (d,) = dup.collect()
    assert (d.trip_id, d.kind, d.year, d.month) == ("RB", "cross_month_trip_id", 2024, 7)  # the later file's copy is removed
    assert trips.where("trip_id = 'RB'").first().month == 6
    q = {(r.year, r.month): r for r in table(spark, cfg, "quality").collect()}
    assert q[(2024, 6)].input_rows == 3 and q[(2024, 7)].input_rows == 2 and q[(2024, 7)].duplicates_removed == 1
    assert all(trips.where(F.col("start_station_key").isNull()).count() == 0 for _ in [0])
    ledger = json.load(open(f"{tmp_path}/_meta/silver_ledger.json"))
    assert ledger["2024-07"]["accepted"] == 1 and ledger["2024-07"]["duplicates"] == 1


def test_same_bike_starting_twice_keeps_the_shorter_trip(spark, tmp_path):
    cfg = setup(spark, tmp_path, legacy_v1={"2015-01": [
        dict(tripduration="533", stoptime="1/15/2015 8:08"), dict(tripduration="540", stoptime="1/15/2015 8:09"),
        dict(bikeid="2")]})
    silver_run.run(spark, cfg)
    assert sorted(r.duration_s for r in table(spark, cfg, "trips").collect()) == [533, 600]
    (d,) = table(spark, cfg, "duplicates").collect()
    assert d.kind == "same_bike_start" and d.duration_s == 540


def test_exact_copies_inside_one_month_are_collapsed_to_one(spark, tmp_path):
    cfg = setup(spark, tmp_path, legacy_v1={"2015-01": [dict(), dict(), dict()]})
    silver_run.run(spark, cfg)
    assert table(spark, cfg, "trips").count() == 1
    assert [r.kind for r in table(spark, cfg, "duplicates").collect()] == ["same_month_trip_id"] * 2


def test_next_month_is_checked_against_the_published_previous_month(spark, tmp_path):
    boundary = dict(rideid="RB", startedat="2024-06-30 23:58:00.000", endedat="2024-07-01 00:10:00.000")
    cfg = setup(spark, tmp_path, modern_v2={"2024-06": [dict(rideid="R1"), boundary]})
    silver_run.run(spark, cfg, months=["2024-06"])
    cfg = setup(spark, tmp_path, modern_v2={"2024-07": [boundary, dict(rideid="R4", startedat="2024-07-02 09:00:00.000", endedat="2024-07-02 09:30:00.000")]})
    res = silver_run.run(spark, cfg, months=["2024-07"])
    assert res["processed"] == ["2024-07"] and res["failed"] == []
    assert [r.kind for r in table(spark, cfg, "duplicates").collect()] == ["cross_month_prior"]
    assert sorted(r.trip_id for r in table(spark, cfg, "trips").collect()) == ["R1", "R4", "RB"]


def test_month_whose_start_station_is_missing_from_the_table_fails_and_is_not_published(spark, tmp_path):
    cfg = setup(spark, tmp_path, smap=[s for s in SMAP if s[2] != "Alpha"], modern_v2={"2024-06": [dict(rideid="R1")]})
    res = silver_run.run(spark, cfg)
    assert res["failed"] == ["2024-06"] and res["processed"] == []
    assert any(c["name"] == "station_keys_complete" and not c["ok"] for c in res["checks"]["2024-06"])
    assert not (tmp_path / "silver" / "trips").exists()
    assert "2024-06" not in json.load(open(f"{tmp_path}/_meta/silver_ledger.json"))


def test_unchanged_months_are_skipped_and_force_reprocesses(spark, tmp_path):
    cfg = setup(spark, tmp_path, modern_v2={"2024-06": [dict(rideid="R1")]})
    assert silver_run.run(spark, cfg)["processed"] == ["2024-06"]
    again = silver_run.run(spark, cfg)
    assert again["processed"] == [] and again["skipped"] == ["2024-06"]
    assert silver_run.run(spark, cfg, force=True)["processed"] == ["2024-06"]
    assert table(spark, cfg, "trips").count() == 1                 # re-processing replaced the month, did not add to it


def test_months_missing_from_the_bronze_ledger_are_not_processed(spark, tmp_path):
    cfg = setup(spark, tmp_path, modern_v2={"2024-06": [dict(rideid="R1")]})
    res = silver_run.run(spark, cfg, months=["2024-06", "2024-07"])
    assert res["not_ready"] == ["2024-07"] and res["processed"] == ["2024-06"]


def test_a_new_station_elsewhere_does_not_invalidate_months_that_do_not_use_it(spark, tmp_path):
    from citibike import stations
    cfg = setup(spark, tmp_path, modern_v2={"2024-06": [dict(rideid="R1")], "2024-07": [dict(rideid="R2", startedat="2024-07-02 09:00:00.000", endedat="2024-07-02 09:30:00.000")]})
    lake = cfg["paths"]["lake_root"]

    def monthly(rows):   # what stations.update would have stored: which stations occur in which month
        return spark.createDataFrame(rows, "schema string, station_id string, station_name string, year int, month int")
    base = [("modern_v2", "6000.01", "Alpha", 2024, 6), ("modern_v2", "6000.02", "Beta", 2024, 6),
            ("modern_v2", "6000.01", "Alpha", 2024, 7), ("modern_v2", "6000.02", "Beta", 2024, 7)]
    monthly(base).write.mode("overwrite").parquet(f"{lake}/silver/station_observations_monthly")
    assert sorted(silver_run.run(spark, cfg)["processed"]) == ["2024-06", "2024-07"]

    # July gets a brand-new station (7777.01); the map grows by one row. June uses neither it nor anything that changed.
    monthly(base + [("modern_v2", "7777.01", "Gamma", 2024, 7)]).write.mode("overwrite").parquet(f"{lake}/silver/station_observations_monthly")
    spark.createDataFrame([(s, i, n, k, 1, False) for s, i, n, k in SMAP + [("modern_v2", "7777.01", "Gamma", 5)]],
                          "schema string, station_id string, station_name string, station_key long, n long, by_position boolean") \
        .write.mode("overwrite").parquet(f"{lake}/silver/station_map")
    again = silver_run.run(spark, cfg)
    assert again["processed"] == ["2024-07"] and again["skipped"] == ["2024-06"]
