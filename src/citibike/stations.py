"""Station dimension, step 1: what stations does the data contain?

`observations` collapses every start/end station occurrence into one small row per (schema, id, name):
how often it occurs, in which months, and where it is (median of the valid coordinates).
Service points are left out (same rule as Silver). Stations that appear only in rejected trips (too long, etc.) still
count: a bad trip does not make the station unreal.
"""
from pyspark.sql import DataFrame, Window, functions as F

from citibike import silver

# (id, name, lat, lng) column names of the start and end side in each Bronze schema
SIDES = {
    "legacy_v1": [("startstationid", "startstationname", "startstationlatitude", "startstationlongitude"),
                  ("endstationid", "endstationname", "endstationlatitude", "endstationlongitude")],
    "modern_v2": [("startstationid", "startstationname", "startlat", "startlng"),
                  ("endstationid", "endstationname", "endlat", "endlng")],
}


def observations(bronze: DataFrame, schema: str, rules: dict) -> DataFrame:
    sides = [bronze.select(silver.station_id(i).alias("station_id"),
                           silver._blank_to_null(F.col(n)).alias("station_name"),
                           silver._num(la).alias("lat"), silver._num(ln).alias("lng"), "year", "month")
             for i, n, la, ln in SIDES[schema]]
    obs = sides[0].unionByName(sides[1]).where(F.col("station_id").isNotNull())
    obs = obs.where(~silver._is_service("station_id", "station_name", rules))

    c = rules["coordinates"]
    valid = (F.col("lat").between(c["lat_min"], c["lat_max"]) & F.col("lng").between(c["lng_min"], c["lng_max"]))
    ym = F.col("year") * 100 + F.col("month")
    return (obs.groupBy("station_id", "station_name")
            .agg(F.count(F.lit(1)).alias("n"),
                 F.count(F.when(valid, 1)).alias("n_coords"),
                 F.percentile_approx(F.when(valid, F.col("lat")), 0.5).alias("lat"),
                 F.percentile_approx(F.when(valid, F.col("lng")), 0.5).alias("lng"),
                 F.min(ym).alias("first_ym"), F.max(ym).alias("last_ym"))
            .withColumn("schema", F.lit(schema)))


# ---------------------------------------------------------------- station identity (the dimension)

def prepare(obs: DataFrame) -> DataFrame:
    """One node per observation; `anchor` is a stable text label (earliest month first) used to name the station."""
    return (obs.withColumn("node", F.xxhash64("schema", "station_id", "station_name"))
            .withColumn("anchor", F.concat_ws("|", F.lpad(F.col("first_ym").cast("string"), 6, "0"), "schema",
                                              "station_id", F.coalesce("station_name", F.lit("")))))


def link_candidates(nodes: DataFrame) -> DataFrame:
    """Pairs of nodes that share an id (same schema) or an exact name, with their distance (null: a position is missing)."""
    a, b = nodes.alias("a"), nodes.alias("b")
    before = F.col("a.node") < F.col("b.node")
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
    nodes = prepare(obs).cache()
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
                  (F.sum(F.col("lat") * w) / F.sum(F.when(F.col("lat").isNotNull(), w))).alias("lat"),
                  (F.sum(F.col("lng") * w) / F.sum(F.when(F.col("lng").isNotNull(), w))).alias("lng")))

    def side(df, p):
        return df.select(F.col("comp").alias(f"{p}_comp"), F.col("lat").alias(f"{p}_lat"), F.col("lng").alias(f"{p}_lng"))
    legacy_only = side(comps.where((F.col("has_legacy") == 1) & (F.col("has_modern") == 0)), "l")
    modern_only = side(comps.where((F.col("has_modern") == 1) & (F.col("has_legacy") == 0)), "m")
    near = (legacy_only.crossJoin(modern_only)
            .withColumn("dist_m", silver.haversine_m(F.col("l_lat"), F.col("l_lng"), F.col("m_lat"), F.col("m_lng")))
            .where(F.col("dist_m") <= radius)
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
