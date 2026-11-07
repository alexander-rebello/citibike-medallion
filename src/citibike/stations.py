"""Station dimension.

`monthly_observations` collapses every start/end station occurrence of one month into one small row per
(schema, id, name): how often it occurs and where it is. `merge_months` combines the months, `build` links the records
into stations (same id or name AND close together), and `update` does all of it incrementally: only months that are new or
changed in Bronze are scanned, everything after that works on a few thousand rows.
Service points are left out (same rule as Silver). Stations that appear only in rejected trips (too long, etc.) still
count: a bad trip does not make the station unreal.
"""
import json
import os
import time

from pyspark.sql import Column, DataFrame, Window, functions as F

from citibike import silver

# (id, name, lat, lng) column names of the start and end side in each Bronze schema
SIDES = {
    "legacy_v1": [("startstationid", "startstationname", "startstationlatitude", "startstationlongitude"),
                  ("endstationid", "endstationname", "endstationlatitude", "endstationlongitude")],
    "modern_v2": [("startstationid", "startstationname", "startlat", "startlng"),
                  ("endstationid", "endstationname", "endlat", "endlng")],
}


CELL_DECIMALS = 4   # positions are first grouped into cells of about 10 m, then medians are taken over the cells


def weighted_median(df: DataFrame, keys: list[str], value: str, weight: str, out: str) -> DataFrame:
    """Per key, the smallest `value` at which the cumulative weight reaches half of the total (null values are ignored)."""
    x = df.where(F.col(value).isNotNull() & (F.col(weight) > 0))
    upto = Window.partitionBy(*keys).orderBy(value).rowsBetween(Window.unboundedPreceding, 0)
    total = Window.partitionBy(*keys)
    return (x.withColumn("_cum", F.sum(weight).over(upto)).withColumn("_tot", F.sum(weight).over(total))
            .where(F.col("_cum") * 2 >= F.col("_tot")).groupBy(*keys).agg(F.min(value).alias(out)))


def monthly_observations(bronze: DataFrame, schema: str, rules: dict) -> DataFrame:
    """One row per (schema, id, name, year, month): occurrences, how many have a valid position, and the median position.

    The scan over Bronze only trims, casts and groups raw values into position cells. Cleaning ids, the service-point
    rule and the medians then run on the much smaller grouped table, not on every trip.
    Service points are left out (same rule as Silver). Stations that only appear in rejected trips still count.
    """
    c = rules["coordinates"]
    sides = []
    for i, n, la, ln in SIDES[schema]:
        lat, lng = silver._num(la), silver._num(ln)
        valid = lat.between(c["lat_min"], c["lat_max"]) & lng.between(c["lng_min"], c["lng_max"])
        sides.append(bronze.select(F.trim(F.col(i)).alias("raw_id"), F.trim(F.col(n)).alias("raw_name"),
                                   F.when(valid, F.round(lat, CELL_DECIMALS)).alias("lat"),
                                   F.when(valid, F.round(lng, CELL_DECIMALS)).alias("lng"), "year", "month"))
    cells = (sides[0].unionByName(sides[1]).where(F.col("raw_id").isNotNull() & (F.col("raw_id") != ""))
             .groupBy("raw_id", "raw_name", "lat", "lng", "year", "month").agg(F.count(F.lit(1)).alias("cnt"))
             # small, and used three times below (counts, latitude median, longitude median): compute the Bronze scan once.
             # Without this Spark scanned Bronze three times (event log: three stages with identical input and task count).
             .localCheckpoint())

    small = (cells.withColumn("station_id", silver.station_id("raw_id"))
             .withColumn("station_name", F.coalesce(silver._blank_to_null(F.col("raw_name")), F.lit("")))   # "" = no name
             .where(F.col("station_id").isNotNull()))
    small = small.where(~_service(rules))
    keys = ["station_id", "station_name", "year", "month"]
    totals = small.groupBy(*keys).agg(F.sum("cnt").alias("n"),
                                      F.sum(F.when(F.col("lat").isNotNull(), F.col("cnt")).otherwise(0)).alias("n_coords"))
    lat = weighted_median(small, keys, "lat", "cnt", "lat")
    lng = weighted_median(small, keys, "lng", "cnt", "lng")
    return (totals.join(lat, keys, "left").join(lng, keys, "left")
            .withColumn("station_name", F.when(F.col("station_name") == "", F.lit(None)).otherwise(F.col("station_name")))
            .withColumn("ym", F.col("year") * 100 + F.col("month")).withColumn("schema", F.lit(schema)))


def _service(rules: dict) -> Column:
    name = F.when(F.col("station_name") == "", F.lit(None)).otherwise(F.col("station_name"))
    s = rules["stations"]
    return F.coalesce(F.col("station_id").rlike(s["service_id_pattern"])
                      | name.rlike("(?i)" + "|".join(s["service_name_patterns"])), F.lit(False))


def merge_months(monthly: DataFrame) -> DataFrame:
    """Across months: total occurrences, first and last month, and the median of the monthly median positions."""
    keys = ["schema", "station_id", "station_name"]
    m = monthly.withColumn("station_name", F.coalesce("station_name", F.lit("")))
    totals = m.groupBy(*keys).agg(F.sum("n").alias("n"), F.sum("n_coords").alias("n_coords"),
                                  F.min("ym").alias("first_ym"), F.max("ym").alias("last_ym"))
    lat = weighted_median(m, keys, "lat", "n_coords", "lat")
    lng = weighted_median(m, keys, "lng", "n_coords", "lng")
    return (totals.join(lat, keys, "left").join(lng, keys, "left")
            .withColumn("station_name", F.when(F.col("station_name") == "", F.lit(None)).otherwise(F.col("station_name")))
            .select("station_id", "station_name", "n", "n_coords", "lat", "lng", "first_ym", "last_ym", "schema"))


# ---------------------------------------------------------------- station identity (the dimension)

def prepare(obs: DataFrame, valet_pattern: str) -> DataFrame:
    """One node per observation; `anchor` is a stable text label (earliest month first) used to name the station.
    `is_valet_record`: valet points are a different kind of place and are never merged with regular stations."""
    return (obs.withColumn("node", F.xxhash64("schema", "station_id", "station_name"))
            .withColumn("is_valet_record", F.coalesce(F.col("station_name").rlike(valet_pattern), F.lit(False)))
            .withColumn("anchor", F.concat_ws("|", F.lpad(F.col("first_ym").cast("string"), 6, "0"), "schema",
                                              "station_id", F.coalesce("station_name", F.lit("")))))


def link_candidates(nodes: DataFrame) -> DataFrame:
    """Pairs of nodes that share an id (same schema) or an exact name, with their distance (null: a position is missing)."""
    a, b = nodes.alias("a"), nodes.alias("b")
    before = (F.col("a.node") < F.col("b.node")) & (F.col("a.is_valet_record") == F.col("b.is_valet_record"))
    dist = silver.haversine_m(F.col("a.lat"), F.col("a.lng"), F.col("b.lat"), F.col("b.lng"))

    def pairs(cond, rule):
        return a.join(b, cond & before).select(F.col("a.node").alias("src"), F.col("b.node").alias("dst"),
                                               dist.alias("dist_m"), F.lit(rule).alias("rule"))
    same_id = (F.col("a.schema") == F.col("b.schema")) & (F.col("a.station_id") == F.col("b.station_id"))
    return pairs(same_id, "id").unionByName(pairs(F.col("a.station_name") == F.col("b.station_name"), "name"))


def components(labels: DataFrame, edges: DataFrame, max_iter: int = 30) -> DataFrame:
    """Connected components by min-label propagation. labels: (node, label); edges: (src, dst), either direction."""
    both = edges.select("src", "dst").unionByName(edges.select(F.col("dst").alias("src"), F.col("src").alias("dst")))
    for _ in range(max_iter):
        nbr = (both.join(labels.select(F.col("node").alias("dst"), F.col("label").alias("nl")), "dst")
               .groupBy("src").agg(F.min("nl").alias("nl")))
        new = (labels.join(nbr, labels.node == nbr.src, "left")
               .select("node", F.least("label", F.coalesce("nl", "label")).alias("label")).localCheckpoint())
        changed = new.alias("n").join(labels.alias("o"), "node").where(F.col("n.label") != F.col("o.label")).count()
        labels = new
        if changed == 0:
            return labels
    raise RuntimeError(f"station components did not converge in {max_iter} iterations")


def build(obs: DataFrame, rules: dict) -> dict[str, DataFrame]:
    """Returns {stations, station_map, conflicts, coord_ambiguous}: see docs. Rules in order: id, name, then position."""
    radius = rules["stations"]["match_radius_m"]
    nodes = prepare(obs, rules["stations"]["valet_name_pattern"]).cache()
    cand = link_candidates(nodes).cache()
    ok = cand.where(F.col("dist_m").isNull() | (F.col("dist_m") <= radius))
    conflicts = cand.where(F.col("dist_m") > radius)

    # stage A: same id or same name, and not contradicted by position
    a = components(nodes.select("node", F.col("anchor").alias("label")), ok.select("src", "dst"))

    # stage B: components that exist in only one era, matched by position if the match is mutual and unique
    part = nodes.join(a.withColumnRenamed("label", "comp"), "node")
    w = F.col("n_coords")
    comps = (part.groupBy("comp")
             .agg(F.max((F.col("schema") == "legacy_v1").cast("int")).alias("has_legacy"),
                  F.max((F.col("schema") == "modern_v2").cast("int")).alias("has_modern"),
                  F.max(F.col("is_valet_record").cast("int")).alias("valet"),
                  (F.sum(F.col("lat") * w) / F.sum(F.when(F.col("lat").isNotNull(), w))).alias("lat"),
                  (F.sum(F.col("lng") * w) / F.sum(F.when(F.col("lng").isNotNull(), w))).alias("lng")))

    def side(df, p):
        return df.select(F.col("comp").alias(f"{p}_comp"), F.col("lat").alias(f"{p}_lat"), F.col("lng").alias(f"{p}_lng"),
                         F.col("valet").alias(f"{p}_valet"))
    legacy_only = side(comps.where((F.col("has_legacy") == 1) & (F.col("has_modern") == 0)), "l")
    modern_only = side(comps.where((F.col("has_modern") == 1) & (F.col("has_legacy") == 0)), "m")
    near = (legacy_only.crossJoin(modern_only)
            .withColumn("dist_m", silver.haversine_m(F.col("l_lat"), F.col("l_lng"), F.col("m_lat"), F.col("m_lng")))
            .where((F.col("dist_m") <= radius) & (F.col("l_valet") == F.col("m_valet")))
            .withColumn("k_l", F.count(F.lit(1)).over(Window.partitionBy("l_comp")))
            .withColumn("k_m", F.count(F.lit(1)).over(Window.partitionBy("m_comp"))).cache())
    unique = near.where((F.col("k_l") == 1) & (F.col("k_m") == 1))
    ambiguous = near.where((F.col("k_l") > 1) | (F.col("k_m") > 1))
    bridge = (unique.select(F.col("l_comp").alias("comp"), F.least("l_comp", "m_comp").alias("final"))
              .unionByName(unique.select(F.col("m_comp").alias("comp"), F.least("l_comp", "m_comp").alias("final"))))

    node_final = (part.join(bridge, "comp", "left")
                  .withColumn("anchor_final", F.coalesce("final", "comp"))
                  .withColumn("by_position", F.col("final").isNotNull()))
    conflict_nodes = (conflicts.select(F.col("src").alias("node")).unionByName(conflicts.select(F.col("dst").alias("node")))
                      .distinct().withColumn("has_conflict", F.lit(True)))
    node_final = (node_final.join(conflict_nodes, "node", "left")
                  .withColumn("station_key", F.xxhash64("anchor_final"))
                  .withColumn("has_conflict", F.coalesce("has_conflict", F.lit(False))))

    station_map = node_final.select("schema", "station_id", "station_name", "station_key", "n", "by_position")

    best = Window.partitionBy("station_key").orderBy(F.desc("last_ym"), F.desc("n"))
    latest = (node_final.withColumn("rk", F.row_number().over(best)).where("rk = 1")
              .select("station_key", F.col("station_name").alias("name"), F.col("lat").alias("best_lat"),
                      F.col("lng").alias("best_lng")))
    stations = (node_final.groupBy("station_key")
                .agg(F.sum("n").alias("n_trips"), F.min("first_ym").alias("valid_from_ym"), F.max("last_ym").alias("valid_to_ym"),
                     F.collect_set(F.when(F.col("schema") == "legacy_v1", F.col("station_id"))).alias("legacy_ids"),
                     F.collect_set(F.when(F.col("schema") == "modern_v2", F.col("station_id"))).alias("modern_ids"),
                     F.collect_set("station_name").alias("names"),
                     F.max("by_position").alias("matched_by_position"), F.max("has_conflict").alias("has_conflict"),
                     F.count(F.lit(1)).alias("records"))
                .join(latest, "station_key")
                .withColumn("era", F.when((F.size("legacy_ids") > 0) & (F.size("modern_ids") > 0), "both")
                            .when(F.size("legacy_ids") > 0, "legacy_only").otherwise("modern_only"))
                .withColumn("is_valet", F.expr("exists(names, x -> x rlike '" + rules["stations"]["valet_name_pattern"] + "')"))
                .withColumn("outside_nyc", F.expr("exists(array_union(legacy_ids, modern_ids), x -> x rlike '"
                                                  + rules["stations"]["outside_nyc_id_pattern"] + "')"))
                .select("station_key", F.col("name").alias("station_name"), F.col("best_lat").alias("lat"),
                        F.col("best_lng").alias("lng"), "legacy_ids", "modern_ids", "names", "valid_from_ym", "valid_to_ym",
                        "n_trips", "era", "matched_by_position", "has_conflict", "is_valet", "outside_nyc"))
    return {"stations": stations, "station_map": station_map, "conflicts": conflicts, "coord_ambiguous": ambiguous}


# ---------------------------------------------------------------- incremental update

def _obs_ledger_path(lake: str) -> str:
    return os.path.join(lake, "_meta", "station_observations_ledger.json")


def update(spark, cfg: dict, rules: dict, force: bool = False) -> dict:
    """Scan new or changed Bronze months, merge all months, rebuild stations / station_map / conflicts.

    Returns what changed: scanned months, records that are new, and records whose station key changed (a changed key makes
    Silver re-process the affected months, because the station map digest changes).
    """
    from citibike import bronze
    lake = cfg["paths"]["lake_root"]
    out = os.path.join(lake, "silver")
    ledger = bronze.read_ledger(lake)
    path = _obs_ledger_path(lake)
    done = json.load(open(path)) if os.path.exists(path) else {}
    todo = sorted(m for m in ledger if force or done.get(m) != ledger[m]["digest"])

    t0 = time.time()
    by_schema: dict[str, list[str]] = {}
    for m in todo:
        by_schema.setdefault(ledger[m]["schema"], []).append(m)
    for schema, months in by_schema.items():
        yms = [int(m[:4]) * 100 + int(m[5:]) for m in months]
        raw = spark.read.parquet(bronze.bronze_path(lake, schema)).where((F.col("year") * 100 + F.col("month")).isin(yms))
        (monthly_observations(raw, schema, rules).write.mode("overwrite").partitionBy("schema", "year", "month")
         .parquet(os.path.join(out, "station_observations_monthly")))
    scan_seconds = round(time.time() - t0, 1)
    if todo:
        done.update({m: ledger[m]["digest"] for m in todo})
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path + ".tmp", "w") as f:
            json.dump(done, f, indent=1, sort_keys=True)
        os.replace(path + ".tmp", path)

    merged = merge_months(spark.read.parquet(os.path.join(out, "station_observations_monthly"))).cache()
    built = build(merged, rules)

    old_map_path = os.path.join(out, "station_map")
    old = spark.read.parquet(old_map_path).select("schema", "station_id", "station_name", F.col("station_key").alias("old_key")).collect() \
        if os.path.exists(old_map_path) else []
    new_stations, new_map = built["stations"].cache(), built["station_map"].cache()
    new = {(r.schema, r.station_id, r.station_name): r.station_key for r in new_map.collect()}
    oldk = {(r.schema, r.station_id, r.station_name): r.old_key for r in old}
    summary = {"scanned_months": todo, "scan_seconds": scan_seconds, "records": len(new), "stations": new_stations.count(),
               "new_records": len(set(new) - set(oldk)), "key_changes": sum(1 for k in set(new) & set(oldk) if new[k] != oldk[k]),
               "records_no_longer_present": len(set(oldk) - set(new))}

    merged.coalesce(1).write.mode("overwrite").parquet(os.path.join(out, "station_observations"))
    for name, df in {"stations": new_stations, "station_map": new_map, "station_conflicts": built["conflicts"],
                     "station_coord_ambiguous": built["coord_ambiguous"]}.items():
        df.coalesce(1).write.mode("overwrite").parquet(os.path.join(out, name))
    return summary

