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
