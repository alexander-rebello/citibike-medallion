import datetime as dt
import json
import os

import pytest

from citibike import gold

D1, D2 = dt.date(2024, 6, 1), dt.date(2024, 6, 2)
COLS = "start_date date, start_hour int, rider_type string, bike_type string, duration_s long, distance_m double, start_station_key string"
ROWS = [(D1, 8, "member", "electric", 600, 2000.0, "A"), (D1, 8, "member", "electric", 1200, 4000.0, "A"),
        (D1, 9, "casual", "classic", 900, None, "B"), (D2, 8, "member", "classic", 300, 1000.0, "A")]


@pytest.fixture
def trips(spark):
    return spark.createDataFrame(ROWS, COLS)


def test_riders_daily_aggregates_per_date_rider_and_bike(spark, trips):
    out = {(r.start_date, r.rider_type, r.bike_type): r for r in gold.riders_daily(trips).collect()}
    r = out[(D1, "member", "electric")]
    assert (r.trips, r.avg_duration_s, r.avg_distance_m, r.year, r.month) == (2, 900.0, 3000.0, 2024, 6)
    assert out[(D1, "casual", "classic")].avg_distance_m is None  # no distance known, not zero
    assert gold.total_trips(gold.riders_daily(trips)) == 4


def test_demand_hourly_aggregates_per_station_date_and_hour(trips):
    out = {(r.start_station_key, r.start_date, r.start_hour): r.trips for r in gold.demand_hourly(trips).collect()}
    assert out == {("A", D1, 8): 2, ("B", D1, 9): 1, ("A", D2, 8): 1}


def tables(trips):
    return {"gold_riders_daily": gold.riders_daily(trips), "gold_demand_hourly": gold.demand_hourly(trips)}


OK = [{"name": "silver_row_accounting", "ok": True}]


def test_publish_swaps_tables_in_and_writes_ready_marker(spark, trips, tmp_path):
    marker = gold.publish(spark, str(tmp_path), "run1", tables(trips), 4, OK)
    ready = json.loads((tmp_path / "gold" / "_READY").read_text())
    assert ready["run_id"] == "run1" and ready["trips"] == 4 and marker == ready
    assert gold.total_trips(spark.read.parquet(str(tmp_path / "gold" / "gold_riders_daily"))) == 4
    assert not (tmp_path / "gold" / "_staging" / "run1").exists()


def test_failed_silver_check_publishes_nothing(spark, trips, tmp_path):
    with pytest.raises(gold.GoldCheckFailed, match="silver_row_accounting"):
        gold.publish(spark, str(tmp_path), "run1", tables(trips), 4, [{"name": "silver_row_accounting", "ok": False}])
    assert not (tmp_path / "gold").exists() or not (tmp_path / "gold" / "_READY").exists()


def test_total_mismatch_keeps_the_previous_publication(spark, trips, tmp_path):
    gold.publish(spark, str(tmp_path), "run1", tables(trips), 4, OK)
    with pytest.raises(gold.GoldCheckFailed, match="totals differ"):
        gold.publish(spark, str(tmp_path), "run2", tables(trips), 5, OK)  # Silver says 5 accepted rows, Gold has 4
    assert json.loads((tmp_path / "gold" / "_READY").read_text())["run_id"] == "run1"
    assert gold.total_trips(spark.read.parquet(str(tmp_path / "gold" / "gold_riders_daily"))) == 4
    assert os.path.exists(tmp_path / "gold" / "_staging" / "run2")  # kept for inspection


# ---------------------------------------------------------------- stations and distance bands

ST_COLS = "station_key long, station_name string, lat double, lng double, is_valet boolean, outside_nyc boolean"
STATIONS = [(1, "Times Sq", 40.7580, -73.9855, False, False),     # at the reference point: 0-2 km
            (2, "Far Brooklyn", 40.6500, -73.9500, False, False),   # about 12 km away: 10+ km
            (3, "Penn Valet", 40.7500, -73.9900, True, False),
            (4, "Hoboken", 40.7440, -74.0300, False, True)]


def trips_for_stations(spark):
    rows = [(D1, 8, "member", "classic", 600, 1.0, k) for k in [1] * 5 + [2] * 3 + [3] * 7 + [4] * 2]
    rows += [(dt.date(2025, 1, 5), 9, "member", "classic", 600, 1.0, 2)]
    return spark.createDataFrame(rows, "start_date date, start_hour int, rider_type string, bike_type string, duration_s long, "
                                       "distance_m double, start_station_key long")


def test_station_year_ranks_only_regular_nyc_stations_and_keeps_every_trip(spark):
    out = {(r.year, r.station_key): r for r in gold.station_year(trips_for_stations(spark), spark.createDataFrame(STATIONS, ST_COLS)).collect()}
    assert out[(2024, 1)].rank == 1 and out[(2024, 2)].rank == 2          # the valet station (7 trips) is not ranked
    assert out[(2024, 3)].rank is None and out[(2024, 3)].band == "valet" and out[(2024, 3)].trips_started == 7
    assert out[(2024, 4)].rank is None and out[(2024, 4)].band == "outside_nyc"
    assert out[(2024, 1)].band == "0-2 km" and out[(2024, 2)].band == "10+ km" and out[(2025, 2)].rank == 1
    assert sum(r.trips_started for r in out.values()) == 18               # nothing lost


def test_station_bands_add_up_per_year(spark):
    sy = gold.station_year(trips_for_stations(spark), spark.createDataFrame(STATIONS, ST_COLS))
    bands = {(r.year, r.band): r for r in gold.station_bands(sy).collect()}
    assert bands[(2024, "0-2 km")].trips == 5 and bands[(2024, "valet")].trips == 7
    assert round(sum(r.share_of_year_pct for r in bands.values() if r.year == 2024)) == 100


def test_publish_checks_station_tables_against_the_trip_total(spark, tmp_path):
    trips, st = trips_for_stations(spark), spark.createDataFrame(STATIONS, ST_COLS)
    sy = gold.station_year(trips, st)
    gold.publish(spark, str(tmp_path), "run1", {"gold_station_year": sy, "gold_station_bands": gold.station_bands(sy)}, 18, OK)
    assert gold.total_started(spark.read.parquet(str(tmp_path / "gold" / "gold_station_year"))) == 18
