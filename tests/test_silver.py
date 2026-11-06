"""Silver step 1 on rows shaped like each era's real Bronze rows (plus the edge cases from the profile)."""
from pyspark.sql import functions as F

from citibike import silver

RULES = silver.load_rules()

LEGACY = dict(tripduration="600", starttime="2018-01-01 13:50:57.4340", stoptime="2018-01-01 14:00:57.4340",
              startstationid="3160.0", startstationname="Central Park West & W 76 St",
              startstationlatitude="40.77896784", startstationlongitude="-73.97374737",
              endstationid="519.0", endstationname="Pershing Square North",
              endstationlatitude="40.75188406", endstationlongitude="-73.97770164",
              bikeid="14529", usertype="Subscriber", birthyear="1984.0", gender="1",
              _source_file="f.csv.gz", year=2018, month=1)
MODERN = dict(rideid="20BB0D08A5124FA3", rideabletype="electric_bike", startedat="2024-06-27 10:43:06.955",
              endedat="2024-06-27 10:46:50.783", startstationname="Berkeley Pl & 6 Ave", startstationid="4134.06",
              endstationname="Plaza St West & Flatbush Ave", endstationid="4010.13",
              startlat="40.6765304", startlng="-73.97846879", endlat="40.6750207", endlng="-73.97111473",
              membercasual="member", _source_file="m.csv.gz", year=2024, month=6)


def run(spark, schema, base, *rows):
    data = [{**base, **r} for r in rows]
    cols = list(base)
    df = spark.createDataFrame([tuple(d[c] for c in cols) for d in data],
                               ", ".join(f"{c} {'int' if c in ('year', 'month') else 'string'}" for c in cols))
    out = silver.standardise(df, schema, RULES)
    # read the New York wall-clock time from Spark: collect() would show the instant in the VM's timezone (UTC)
    return out.withColumn("start_local", F.date_format("start_time", "yyyy-MM-dd'T'HH:mm:ss")).collect()


def test_rule_runs_cover_every_month_exactly_once():
    def months(a, b):
        y, m = map(int, a.split("-")); out = []
        while f"{y}-{m:02d}" <= b:
            out.append(f"{y}-{m:02d}"); y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        return out
    seen = [m for r in RULES["legacy_v1"]["runs"] for m in months(r["from"], r["to"])]
    assert seen == months("2013-06", "2019-12")  # contiguous, no gap, no overlap, in order


def test_legacy_row_is_mapped_and_accepted(spark):
    (r,) = run(spark, "legacy_v1", LEGACY, {})
    assert r.reject_reasons == [] and r.reject_reason is None
    assert (r.start_station_id, r.end_station_id) == ("3160", "519")  # '.0' stripped
    assert (r.rider_type, r.bike_type, r.gender, r.duration_s, r.birth_year) == ("member", "classic", "male", 600, 1984)
    assert r.start_local == "2018-01-01T13:50:57" and r.start_hour == 13
    assert len(r.trip_id) == 32 and r.distance_m > 3000 and not r.duration_mismatch


def test_legacy_trip_id_is_stable_and_depends_on_the_trip(spark):
    a, b, c = run(spark, "legacy_v1", LEGACY, {}, {}, {"bikeid": "999"})
    assert a.trip_id == b.trip_id != c.trip_id


def test_2015_timestamps_without_seconds_use_tripduration(spark):
    (r,) = run(spark, "legacy_v1", LEGACY, dict(starttime="1/1/2015 0:01", stoptime="1/1/2015 0:24", tripduration="1346",
                                                 year=2015, month=1))
    assert r.duration_s == 1346 and not r.duration_mismatch and r.reject_reason is None
    assert r.start_local == "2015-01-01T00:01:00"


def test_same_year_month_after_a_format_change_uses_the_new_pattern(spark):
    r14, r16 = run(spark, "legacy_v1", LEGACY,
                   dict(starttime="9/1/2014 8:05:03", stoptime="9/1/2014 8:15:03", year=2014, month=9),
                   dict(starttime="2016-10-01 08:05:03", stoptime="2016-10-01 08:15:03", year=2016, month=10))
    assert r14.start_local == "2014-09-01T08:05:03" and r14.reject_reason is None
    assert r16.start_local == "2016-10-01T08:05:03" and r16.reject_reason is None


def test_wrong_pattern_for_the_month_is_a_counted_reject_not_a_crash(spark):
    (r,) = run(spark, "legacy_v1", LEGACY, dict(starttime="garbage"))
    assert r.reject_reason == "time_missing" and r.start_time is None


def test_birth_year_missing_marker_and_implausible_values(spark):
    marker, old, young, ok = run(spark, "legacy_v1", LEGACY, dict(birthyear="\\N"), dict(birthyear="1890.0"),
                                 dict(birthyear="2010.0"), dict(birthyear="1960.0"))
    assert (marker.birth_year, marker.birth_year_nulled) == (None, False)  # missing, not wrong
    assert (old.birth_year, old.birth_year_nulled) == (None, True)
    assert (young.birth_year, young.birth_year_nulled) == (None, True)     # younger than 16 in 2018
    assert (ok.birth_year, ok.birth_year_nulled) == (1960, False)


def test_user_type_and_gender_mapping(spark):
    c, blank, unk = run(spark, "legacy_v1", LEGACY, dict(usertype="Customer", gender="2"), dict(usertype=""),
                        dict(gender="0"))
    assert (c.rider_type, c.gender) == ("casual", "female")
    assert blank.rider_type == "unknown" and unk.gender == "unknown"


def test_duration_rules(spark):
    long_, short, zero = run(spark, "legacy_v1", LEGACY, dict(tripduration="90000"), dict(tripduration="30"),
                             dict(tripduration="0"))
    assert long_.reject_reason == "duration_gt_24h"
    assert short.reject_reason == "duration_lt_60s"
    assert zero.reject_reason == "duration_non_positive"


def test_service_stations_and_montreal_are_rejected_with_all_reasons(spark):
    depot, montreal, valet = run(
        spark, "legacy_v1", LEGACY,
        dict(startstationname="NYCBS Depot - GOW", startstationid="3432"),
        dict(startstationname="8D Mobile 01", startstationid="3650", startstationlatitude="45.50626",
             startstationlongitude="-73.5689"),
        dict(startstationname="Penn Station Valet", startstationid="3230"))
    assert depot.reject_reasons == ["service_station"]
    assert montreal.reject_reasons == ["service_station", "outside_coordinates"] and montreal.reject_reason == "service_station"
    assert valet.reject_reasons == [] and valet.is_valet  # kept and flagged


def test_zero_coordinates_are_rejected(spark):
    (r,) = run(spark, "legacy_v1", LEGACY, dict(endstationlatitude="0.0", endstationlongitude="0.0"))
    assert r.reject_reasons == ["outside_coordinates"]


def test_modern_row_is_mapped_and_accepted(spark):
    (r,) = run(spark, "modern_v2", MODERN, {})
    assert r.reject_reasons == [] and r.trip_id == "20BB0D08A5124FA3"
    assert (r.bike_type, r.rider_type, r.duration_s, r.gender, r.birth_year) == ("electric", "member", 224, None, None)
    assert r.start_station_id == "4134.06" and r.start_date.isoformat() == "2024-06-27" and not r.is_incomplete


def test_modern_service_station_trailing_underscore_and_missing_stations(spark):
    sys_, under, no_start, no_end = run(
        spark, "modern_v2", MODERN,
        dict(startstationid="SYS016", startstationname="Morgan Bike Mechanics"),
        dict(startstationid="5303.06_"),
        dict(startstationid="", startstationname=""),
        dict(endstationid="", endstationname="", endlat="", endlng=""))
    assert sys_.reject_reasons == ["service_station"]
    assert under.start_station_id == "5303.06" and under.reject_reason is None
    assert no_start.reject_reasons == ["missing_start_station"]
    assert no_end.reject_reason is None and no_end.is_incomplete and no_end.distance_m is None  # flagged, kept


def test_jersey_city_station_is_kept_and_flagged(spark):
    (r,) = run(spark, "modern_v2", MODERN, dict(endstationid="JC072", endstationname="Morris Canal"))
    assert r.reject_reason is None and r.outside_nyc


def test_fall_back_hour_gives_non_positive_duration_not_a_crash(spark):
    # 01:50 then 01:10 on 2024-11-03 is a real 20 min trip across the repeated hour; wall clock reads backwards
    (r,) = run(spark, "modern_v2", MODERN, dict(startedat="2024-11-03 01:50:00.000", endedat="2024-11-03 01:10:00.000",
                                                year=2024, month=11))
    assert r.reject_reason == "duration_non_positive"
