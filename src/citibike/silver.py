"""Silver, step 1: map each Bronze schema to the common trip columns and decide accept / reject.

`standardise(df, schema, rules)` returns every input row exactly once, with
  - the common trip columns (typed, cleaned),
  - `reject_reasons`: all rules the row breaks (empty array = accepted) and `reject_reason`, the first of them,
  - flags that keep the row (is_incomplete, is_valet, outside_nyc, birth_year_nulled, duration_mismatch, dst_repaired).
Rows are never dropped here, so bronze rows = accepted + rejected holds by construction; duplicates come later.
DataFrame functions only, no UDFs. All thresholds and patterns live in config/silver_rules.toml.
"""
import tomllib

from pyspark.sql import Column, DataFrame, functions as F

from citibike.config import CONFIG_DIR

# Priority order of the reject reasons (reject_reason is the first that applies).
REASONS = ["time_missing", "duration_non_positive", "duration_lt_60s", "duration_gt_24h",
           "missing_start_station", "service_station", "outside_coordinates"]


def load_rules(path=None) -> dict:
    with open(path or CONFIG_DIR / "silver_rules.toml", "rb") as f:
        return tomllib.load(f)


# ---------------------------------------------------------------- small column helpers

def _blank_to_null(c: Column) -> Column:
    t = F.trim(c)
    return F.when(t == "", F.lit(None)).otherwise(t)


def station_id(col: str) -> Column:
    """'3160.0' -> '3160'; '5303.06_' -> '5303.06'; '' -> null. Ids stay strings."""
    c = F.regexp_replace(F.regexp_replace(F.trim(F.col(col)), r"\.0+$", ""), r"_+$", "")
    return F.when(c == "", F.lit(None)).otherwise(c)


def _ym() -> Column:
    return F.col("year") * 100 + F.col("month")


def by_month_run(runs: list[dict], make) -> Column:
    """Pick a value per row from the run of months it belongs to (null if its month is in no run)."""
    expr = None
    for r in runs:
        lo, hi = int(r["from"].replace("-", "")), int(r["to"].replace("-", ""))
        cond, val = _ym().between(lo, hi), make(r)
        expr = F.when(cond, val) if expr is None else expr.when(cond, val)
    return expr


def legacy_trip_id() -> Column:
    """No id in legacy data: hash of start, stop, bike and start station (the same trip always gets the same id)."""
    return F.substring(F.sha2(F.concat_ws("|", F.col("starttime"), F.col("stoptime"), F.col("bikeid"),
                                          station_id("startstationid")), 256), 1, 32)


def trip_id_expr(schema: str) -> Column:
    return legacy_trip_id() if schema == "legacy_v1" else _blank_to_null(F.col("rideid"))


def bike_start_key(schema: str) -> Column:
    """Raw (bike, start time) text: one bike cannot start two trips at the same moment. Modern data has no bike id."""
    if schema == "legacy_v1":
        return F.concat_ws("|", F.col("bikeid"), F.col("starttime"))
    return F.lit(None).cast("string")


def haversine_m(lat1: Column, lng1: Column, lat2: Column, lng2: Column) -> Column:
    a = (F.sin(F.radians(lat2 - lat1) / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(F.radians(lng2 - lng1) / 2) ** 2)
    return F.round(2 * 6371000 * F.asin(F.sqrt(a)))


def _num(col: str) -> Column:
    """Text -> double; anything that is not a number ('', '\\N', 'NaN' text) becomes null."""
    d = F.trim(F.col(col)).cast("double")
    return F.when(F.isnan(d), F.lit(None)).otherwise(d)


# ---------------------------------------------------------------- per-schema mapping to the common columns

def _legacy(df: DataFrame, rules: dict) -> DataFrame:
    runs = rules["legacy_v1"]["runs"]
    start = by_month_run(runs, lambda r: F.to_timestamp(F.col("starttime"), r["pattern"]))
    end = by_month_run(runs, lambda r: F.to_timestamp(F.col("stoptime"), r["pattern"]))
    ts_diff = F.round(end.cast("double") - start.cast("double")).cast("long")
    tripduration = _num("tripduration").cast("long")
    sid = station_id("startstationid")
    user = F.lower(F.trim(F.col("usertype")))
    return df.select(
        legacy_trip_id().alias("trip_id"), _blank_to_null(F.col("bikeid")).alias("bike_id"),
        start.alias("start_time"), end.alias("end_time"),
        F.coalesce(tripduration, ts_diff).alias("duration_s"),
        # legacy gives the true duration; compare it with the timestamps (DST, rounding to a minute)
        (F.abs(tripduration - ts_diff) > by_month_run(runs, lambda r: F.lit(2 if r["has_seconds"] else 60)))
        .alias("duration_mismatch"),
        sid.alias("start_station_id"), _blank_to_null(F.col("startstationname")).alias("start_station_name"),
        _num("startstationlatitude").alias("start_lat"), _num("startstationlongitude").alias("start_lng"),
        station_id("endstationid").alias("end_station_id"), _blank_to_null(F.col("endstationname")).alias("end_station_name"),
        _num("endstationlatitude").alias("end_lat"), _num("endstationlongitude").alias("end_lng"),
        F.when(user == "subscriber", "member").when(user == "customer", "casual").otherwise("unknown").alias("rider_type"),
        F.lit("classic").alias("bike_type"),  # legacy data has no bike type; assumed classic (documented)
        _num("birthyear").alias("_birth_raw"),
        F.when(F.trim(F.col("gender")) == "1", "male").when(F.trim(F.col("gender")) == "2", "female")
        .otherwise("unknown").alias("gender"),
        F.lit("legacy_v1").alias("schema"), F.col("_source_file").alias("source_file"), "year", "month")


def _modern(df: DataFrame, rules: dict) -> DataFrame:
    runs = rules["modern_v2"]["runs"]
    start = by_month_run(runs, lambda r: F.to_timestamp(F.col("startedat"), r["pattern"]))
    end = by_month_run(runs, lambda r: F.to_timestamp(F.col("endedat"), r["pattern"]))
    member = F.lower(F.trim(F.col("membercasual")))
    bike = F.lower(F.trim(F.col("rideabletype")))
    return df.select(
        _blank_to_null(F.col("rideid")).alias("trip_id"), F.lit(None).cast("string").alias("bike_id"),
        start.alias("start_time"), end.alias("end_time"),
        F.round(end.cast("double") - start.cast("double")).cast("long").alias("duration_s"),
        F.lit(False).alias("duration_mismatch"),
        station_id("startstationid").alias("start_station_id"), _blank_to_null(F.col("startstationname")).alias("start_station_name"),
        _num("startlat").alias("start_lat"), _num("startlng").alias("start_lng"),
        station_id("endstationid").alias("end_station_id"), _blank_to_null(F.col("endstationname")).alias("end_station_name"),
        _num("endlat").alias("end_lat"), _num("endlng").alias("end_lng"),
        F.when(member.isin("member", "casual"), member).otherwise("unknown").alias("rider_type"),
        F.when(bike == "classic_bike", "classic").when(bike == "electric_bike", "electric").otherwise("unknown")
        .alias("bike_type"),
        F.lit(None).cast("double").alias("_birth_raw"), F.lit(None).cast("string").alias("gender"),
        F.lit("modern_v2").alias("schema"), F.col("_source_file").alias("source_file"), "year", "month")


MAPPERS = {"legacy_v1": _legacy, "modern_v2": _modern}


# ---------------------------------------------------------------- rules: reject reasons and flags

def _is_service(id_col: str, name_col: str, rules: dict) -> Column:
    s = rules["stations"]
    by_name = F.col(name_col).rlike("(?i)" + "|".join(s["service_name_patterns"]))
    return F.coalesce(F.col(id_col).rlike(s["service_id_pattern"]) | by_name, F.lit(False))


def _bad_coord(lat: str, lng: str, rules: dict) -> Column:
    c = rules["coordinates"]
    la, ln = F.col(lat), F.col(lng)
    present = la.isNotNull() | ln.isNotNull()
    inside = la.between(c["lat_min"], c["lat_max"]) & ln.between(c["lng_min"], c["lng_max"])
    return F.coalesce(present & ~inside, F.lit(False))


def apply_rules(df: DataFrame, rules: dict) -> DataFrame:
    t, b, s = rules["trip"], rules["birth_year"], rules["stations"]
    d = F.col("duration_s")
    cond = {
        "time_missing": F.col("start_time").isNull() | F.col("end_time").isNull(),
        "duration_non_positive": d <= 0,
        "duration_lt_60s": (d > 0) & (d < t["min_duration_s"]),
        "duration_gt_24h": d > t["max_duration_s"],
        "missing_start_station": F.col("start_station_id").isNull(),
        "service_station": (_is_service("start_station_id", "start_station_name", rules)
                            | _is_service("end_station_id", "end_station_name", rules)),
        "outside_coordinates": (_bad_coord("start_lat", "start_lng", rules)
                                | _bad_coord("end_lat", "end_lng", rules)),
    }
    assert list(cond) == REASONS
    reasons = F.filter(F.array(*[F.when(F.coalesce(c, F.lit(False)), F.lit(n)) for n, c in cond.items()]),
                       lambda x: x.isNotNull())

    plausible = F.col("_birth_raw").between(b["min_year"], F.col("year") - b["min_age"])
    out = (df
           .withColumn("reject_reasons", reasons)
           .withColumn("reject_reason", F.col("reject_reasons").getItem(0))
           .withColumn("birth_year", F.when(plausible, F.col("_birth_raw").cast("int")))
           .withColumn("birth_year_nulled", F.col("_birth_raw").isNotNull() & ~F.coalesce(plausible, F.lit(False)))
           .withColumn("is_incomplete", F.col("end_station_id").isNull())
           .withColumn("is_valet", F.coalesce(F.col("start_station_name").rlike(s["valet_name_pattern"])
                                              | F.col("end_station_name").rlike(s["valet_name_pattern"]), F.lit(False)))
           .withColumn("outside_nyc", F.coalesce(F.col("start_station_id").rlike(s["outside_nyc_id_pattern"])
                                                 | F.col("end_station_id").rlike(s["outside_nyc_id_pattern"]), F.lit(False)))
           .withColumn("distance_m", haversine_m(F.col("start_lat"), F.col("start_lng"), F.col("end_lat"), F.col("end_lng")))
           .withColumn("start_date", F.to_date("start_time"))
           .withColumn("start_hour", F.hour("start_time"))
           .withColumn("start_dow", F.dayofweek("start_time"))  # 1 = Sunday
           .drop("_birth_raw"))
    return out


# ---------------------------------------------------------------- daylight-saving repair

_HOUR = "INTERVAL 1 HOUR"


def _wall(c: Column) -> Column:
    return F.date_format(c, "yyyy-MM-dd HH:mm:ss")


def repair_dst(df: DataFrame) -> DataFrame:
    """Fix end times made wrong by the repeated hour when daylight saving ends (local 01:00-01:59 happens twice).

    Local timestamps carry no offset, so Spark reads a repeated time as its first occurrence. A trip that ends in the
    second pass then appears to end before it started (or, in legacy data, disagrees with `tripduration` by 3600 s).
      modern: end <= start and the end time is a repeated one          -> end + 1 h
      legacy: |tripduration - timestamp difference| is 3600 s (+-60)   -> end = start + tripduration
    Not detectable in modern data (no true duration): a trip whose end lands in the second pass but still reads as after
    its start (e.g. 00:50 -> 01:10). Those keep a duration that is 1 h too short; this is a stated limitation.
    """
    st, en, dur = F.col("start_time"), F.col("end_time"), F.col("duration_s")
    diff = F.round(en.cast("double") - st.cast("double"))
    first_pass_of_repeated = _wall(en + F.expr(_HOUR)) == _wall(en)  # +1 h shows the same wall-clock time
    legacy_fix = (F.col("schema") == "legacy_v1") & (F.abs(F.abs(dur - diff) - 3600) <= 60)
    modern_fix = (F.col("schema") == "modern_v2") & (en <= st) & first_pass_of_repeated
    # decide once and keep the decision in columns: the steps below change the columns the conditions read
    marked = (df
              .withColumn("_fix_legacy", F.coalesce(legacy_fix, F.lit(False)))
              .withColumn("_fix_modern", F.coalesce(modern_fix, F.lit(False))))
    fl, fm = F.col("_fix_legacy"), F.col("_fix_modern")
    return (marked
            .withColumn("dst_repaired", fl | fm)
            .withColumn("end_time", F.when(fl, (st.cast("double") + dur).cast("timestamp"))
                        .when(fm, en + F.expr(_HOUR)).otherwise(en))
            .withColumn("duration_s", F.when(fm, dur + 3600).otherwise(dur))
            .withColumn("duration_mismatch", F.col("duration_mismatch") & ~fl)
            .drop("_fix_legacy", "_fix_modern"))


def standardise(df: DataFrame, schema: str, rules: dict | None = None) -> DataFrame:
    rules = rules or load_rules()
    return apply_rules(repair_dst(MAPPERS[schema](df, rules)), rules)
