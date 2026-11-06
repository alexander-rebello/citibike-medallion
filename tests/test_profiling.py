from citibike import profiling as P


def shapes(spark, values, rules):
    df = spark.createDataFrame([(v,) for v in values], "v string")
    return [r[0] for r in df.select(P.shape("v", rules)).collect()]


def test_timestamp_shapes_cover_every_legacy_and_modern_format(spark):
    got = shapes(spark, ["2013-07-01 00:00:01", "2018-01-01 00:00:03.4770", "2024-06-27 10:43:06.955",
                         "1/1/2015 0:01", "10/12/2016 13:05:59", "", None, "2015-01-01"], P.TS_RULES)
    assert got == ["iso_s", "iso_frac4", "iso_frac3", "us_m", "us_s", "empty", "empty", "other"]


def test_station_id_shapes_keep_legacy_float_and_modern_dotted_ids_apart(spark):
    got = shapes(spark, ["455", "3160.0", "6584.12", "4134.06", "SYS.X", " "], P.ID_RULES)
    assert got == ["int", "dot0", "dotted", "dotted", "other", "empty"]


def test_duration_birth_year_and_coordinate_classes(spark):
    df = spark.createDataFrame(
        [("30", "1890.0", "40.75", "-73.99"), ("120", "1985.0", "0", "0"), ("90000", "2020", "45.1", "-80"),
         ("-5", "x", "", "NaN")],
        "d string, b string, lat string, lng string")
    r = df.select(P.duration_class("d").alias("d"), P.birth_year_class("b").alias("b"),
                  P.coord_class("lat", *P.NYC_LAT).alias("lat"), P.coord_class("lng", *P.NYC_LNG).alias("lng")).collect()
    assert [x.d for x in r] == ["<60s", "ok", ">24h", "<=0"]
    assert [x.b for x in r] == ["<1900", "1920-2010", ">2010", "non_numeric"]
    assert [x.lat for x in r] == ["in_nyc", "zero", "other", "empty"]
    assert [x.lng for x in r] == ["in_nyc", "zero", "other", "non_numeric"]


def test_profile_values_counts_per_month_and_column(spark):
    rows = [("2015", "1", "1/1/2015 0:01", "1/1/2015 0:24", "455", "265", "A", "B", "Subscriber", "1", "40.7", "40.7",
             "-73.9", "-73.9", "300", "1960.0", "7", None),
            ("2015", "1", "2015-01-01 00:02:03", "bad", "3160.0", "", "A", "B", "Customer", "0", "40.7", "40.7",
             "-73.9", "-73.9", "5", "", "8", "raw text")]
    cols = ("year month starttime stoptime startstationid endstationid startstationname endstationname usertype gender "
            "startstationlatitude endstationlatitude startstationlongitude endstationlongitude tripduration birthyear "
            "bikeid _corrupt_record").split()
    df = spark.createDataFrame(rows, ", ".join(f"{c} string" for c in cols))
    out = {(r.column, r.value): r.n for r in P.profile_values(df, "legacy_v1").collect()}
    assert out[("starttime", "us_m")] == 1 and out[("starttime", "iso_s")] == 1
    assert out[("stoptime", "other")] == 1
    assert out[("startstationid", "int")] == 1 and out[("startstationid", "dot0")] == 1
    assert out[("endstationid", "empty")] == 1
    assert out[("tripduration", "<60s")] == 1 and out[("birthyear", "empty")] == 1
    assert out[("_corrupt_record", "present")] == 1 and out[("_corrupt_record", "empty")] == 1
