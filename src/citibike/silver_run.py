"""Silver run: reconciled Bronze months -> silver/{trips,rejected,duplicates,quality}, behind checks.

For every month in the batch:
  1. read Bronze (only months in the Bronze ledger), map to the common columns and apply the rules (silver.py),
  2. attach station keys (broadcast join with silver/station_map),
  3. remove duplicates: the same trip id in two files (keep the earlier file), and one bike starting twice at the same
     moment (keep the shorter trip). Duplicate keys are found from key columns only; just the few affected rows are ranked,
  4. write everything once to a staging area, check it, and publish only the months whose checks pass.

Checks per month: Bronze rows = accepted + rejected + duplicates removed; every accepted trip has its station keys.
A month is re-processed when its Bronze data (or the previous month's, for cross-month duplicates), the rules or the
station table changed; otherwise it is skipped.
"""
import hashlib
import json
import os
import shutil
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.utils import AnalysisException

from citibike import bronze, silver

OUTPUTS = ("trips", "rejected", "duplicates", "quality")
DUP_COLS = ["trip_id", "kind", "schema", "year", "month", "source_file", "start_time", "bike_id", "duration_s"]


# ---------------------------------------------------------------- paths, ledger, digests

def root(lake: str) -> str:
    return os.path.join(lake, "silver")


def _meta(lake: str) -> str:
    return os.path.join(lake, "_meta")


def read_ledger(lake: str) -> dict:
    path = os.path.join(_meta(lake), "silver_ledger.json")
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


def write_ledger(lake: str, ledger: dict) -> None:
    os.makedirs(_meta(lake), exist_ok=True)
    path = os.path.join(_meta(lake), "silver_ledger.json")
    with open(path + ".tmp", "w") as f:
        json.dump(ledger, f, indent=1, sort_keys=True)
    os.replace(path + ".tmp", path)


def append_log(lake: str, records: list[dict]) -> None:
    os.makedirs(_meta(lake), exist_ok=True)
    with open(os.path.join(_meta(lake), "silver_runs.jsonl"), "a") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def _digest(*parts) -> str:
    return hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()[:16]


def rules_digest(rules_path=None) -> str:
    with open(rules_path or (silver.CONFIG_DIR / "silver_rules.toml"), "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()[:16]


def stations_digest(smap: DataFrame) -> str:
    row = smap.agg(F.sha2(F.concat_ws(",", F.sort_array(F.collect_list(
        F.concat_ws("|", "schema", "station_id", F.coalesce("station_name", F.lit("")), F.col("station_key").cast("string"))))), 256)).first()
    return row[0][:16]


def station_digests(spark: SparkSession, lake: str, smap: DataFrame) -> dict[str, str] | None:
    """Per month: fingerprint of the station-map rows for the stations that occur in that month.

    A month's Silver output only depends on its own stations, so a new station elsewhere must not invalidate it.
    None if the monthly observations are not available (then the whole-table digest is used)."""
    path = os.path.join(root(lake), "station_observations_monthly")
    if not os.path.exists(path):
        return None
    occ = spark.read.parquet(path).select("schema", "station_id", "station_name", "year", "month")
    m = smap.select(F.col("schema").alias("m_schema"), F.col("station_id").alias("m_id"), F.col("station_name").alias("m_name"), "station_key")
    j = occ.join(F.broadcast(m), (occ["schema"] == m["m_schema"]) & occ["station_id"].eqNullSafe(m["m_id"])
                 & occ["station_name"].eqNullSafe(m["m_name"]), "left")
    rows = (j.groupBy("year", "month")
            .agg(F.sha2(F.concat_ws(",", F.sort_array(F.collect_list(F.concat_ws(
                "|", "schema", "station_id", F.coalesce("station_name", F.lit("")), F.col("station_key").cast("string"))))), 256).alias("d"))
            .collect())
    return {f"{r['year']}-{r['month']:02d}": r["d"][:16] for r in rows}


def prev_month(m: str) -> str:
    y, mo = int(m[:4]), int(m[5:])
    return f"{y - 1}-12" if mo == 1 else f"{y}-{mo - 1:02d}"


def _ym(m: str) -> int:
    return int(m[:4]) * 100 + int(m[5:])


def plan(bronze_ledger: dict, silver_ledger: dict, scope, rules_dig: str, st_dig, force: bool):
    """-> (batch months with their digest, skipped months, months not ready in Bronze)."""
    months = sorted(bronze_ledger) if scope == "all" else sorted(scope)
    batch, skipped, not_ready = {}, [], []
    for m in months:
        if m not in bronze_ledger:
            not_ready.append(m)
            continue
        prev = bronze_ledger.get(prev_month(m), {}).get("digest", "")
        d = _digest(prev, bronze_ledger[m]["digest"], rules_dig, st_dig.get(m, "") if isinstance(st_dig, dict) else st_dig)
        if not force and silver_ledger.get(m, {}).get("digest") == d:
            skipped.append(m)
        else:
            batch[m] = d
    return batch, skipped, not_ready


# ---------------------------------------------------------------- building the batch

def read_table(spark: SparkSession, path: str) -> DataFrame | None:
    """A Silver table, or None if it does not exist or is empty (Spark writes no files for zero rows)."""
    if not os.path.exists(path):
        return None
    try:
        return spark.read.parquet(path)
    except AnalysisException:
        return None


def with_station_keys(df: DataFrame, smap: DataFrame) -> DataFrame:
    """Add start_station_key / end_station_key from the (small, broadcast) station map; null-safe on id and name."""
    for side in ("start", "end"):
        m = F.broadcast(smap.select(F.col("schema").alias(f"_{side}_schema"), F.col("station_id").alias(f"_{side}_id"),
                                    F.col("station_name").alias(f"_{side}_name"),
                                    F.col("station_key").alias(f"{side}_station_key")))
        cond = ((F.col("schema") == F.col(f"_{side}_schema")) & F.col(f"{side}_station_id").eqNullSafe(F.col(f"_{side}_id"))
                & F.col(f"{side}_station_name").eqNullSafe(F.col(f"_{side}_name")))
        df = df.join(m, cond, "left").drop(f"_{side}_schema", f"_{side}_id", f"_{side}_name")
    return df


def resolve_duplicates(aff: DataFrame, prior_ids: DataFrame | None) -> tuple[DataFrame, DataFrame]:
    """Rank the few rows that share a trip id or a (bike, start). Returns (rows to keep, removed rows with a reason)."""
    cols = aff.columns
    a = (aff.withColumn("file_ym", F.col("year") * 100 + F.col("month"))
         .withColumn("_h", F.md5(F.to_json(F.struct(*cols)))))
    if prior_ids is not None:
        a = a.join(prior_ids.select("trip_id").distinct().withColumn("_prior", F.lit(True)), "trip_id", "left")
    else:
        a = a.withColumn("_prior", F.lit(False))
    a = a.withColumn("_prior", F.coalesce("_prior", F.lit(False)))

    by_id = Window.partitionBy("trip_id")
    a = (a.withColumn("_rn1", F.when(F.col("trip_id").isNotNull(),
                                     F.row_number().over(by_id.orderBy("file_ym", "source_file", "_h"))).otherwise(F.lit(1)))
         .withColumn("_ym1", F.min("file_ym").over(by_id)))
    lost1 = F.col("_prior") | (F.col("_rn1") > 1)
    kind1 = (F.when(F.col("_prior"), "cross_month_prior").when(F.col("file_ym") == F.col("_ym1"), "same_month_trip_id")
             .otherwise("cross_month_trip_id"))
    losers1 = a.where(lost1).withColumn("kind", kind1)

    by_bike = Window.partitionBy("bike_id", "start_time").orderBy("duration_s", "trip_id")
    s = a.where(~lost1).withColumn(
        "_rn2", F.when(F.col("bike_id").isNotNull() & F.col("start_time").isNotNull(), F.row_number().over(by_bike)).otherwise(F.lit(1)))
    losers2 = s.where(F.col("_rn2") > 1).withColumn("kind", F.lit("same_bike_start"))
    helpers = ["file_ym", "_h", "_prior", "_rn1", "_ym1", "_rn2"]
    keep = s.where(F.col("_rn2") == 1).drop(*helpers)
    removed = losers1.select(*DUP_COLS).unionByName(losers2.select(*DUP_COLS))
    return keep, removed


def build_batch(spark: SparkSession, lake: str, rules: dict, by_schema: dict[str, list[str]], smap: DataFrame,
                prior: list[str]) -> tuple[DataFrame, DataFrame]:
    """-> (all standardised rows after duplicate removal, removed duplicate rows)."""
    prior_ids = None
    if prior:
        yms = [_ym(m) for m in prior]
        tables = [read_table(spark, os.path.join(root(lake), d)) for d in ("trips", "rejected")]
        parts = [t.where((F.col("year") * 100 + F.col("month")).isin(yms)).select("trip_id") for t in tables if t is not None]
        if parts:
            prior_ids = parts[0] if len(parts) == 1 else parts[0].unionByName(parts[1])

    normal_parts, aff_parts = [], []
    for schema, months in sorted(by_schema.items()):
        yms = [_ym(m) for m in months]
        raw = spark.read.parquet(bronze.bronze_path(lake, schema)).where((F.col("year") * 100 + F.col("month")).isin(yms))
        keys = raw.select(silver.trip_id_expr(schema).alias("_tid"), silver.bike_start_key(schema).alias("_bs"))
        dup_tid = keys.where(F.col("_tid").isNotNull()).groupBy("_tid").count().where("count > 1").select("_tid")
        if prior_ids is not None:   # ids that already exist in the previous, already published month
            dup_tid = dup_tid.unionByName(keys.select("_tid").where(F.col("_tid").isNotNull())
                                          .join(prior_ids.withColumnRenamed("trip_id", "_tid"), "_tid", "left_semi").distinct())
        dup_tid = dup_tid.distinct().withColumn("_m1", F.lit(True)).localCheckpoint()    # small; compute once
        dup_bs = (keys.where(F.col("_bs").isNotNull()).groupBy("_bs").count().where("count > 1")
                  .select("_bs", F.lit(True).alias("_m2")).localCheckpoint())

        marked = (raw.withColumn("_tid", silver.trip_id_expr(schema)).withColumn("_bs", silver.bike_start_key(schema))
                  .join(F.broadcast(dup_tid), "_tid", "left").join(F.broadcast(dup_bs), "_bs", "left"))
        affected = F.coalesce(F.col("_m1"), F.lit(False)) | F.coalesce(F.col("_m2"), F.lit(False))
        helpers = ["_tid", "_bs", "_m1", "_m2"]
        normal_parts.append(with_station_keys(silver.standardise(marked.where(~affected).drop(*helpers), schema, rules), smap))
        aff_parts.append(with_station_keys(silver.standardise(marked.where(affected).drop(*helpers), schema, rules), smap))

    def union(parts):
        out = parts[0]
        for p in parts[1:]:
            out = out.unionByName(p)
        return out
    aff = union(aff_parts).localCheckpoint()     # a few hundred rows
    keep, removed = resolve_duplicates(aff, prior_ids)
    return union(normal_parts).unionByName(keep), removed


# ---------------------------------------------------------------- checks and quality

def monthly_counts(staged: DataFrame, removed: DataFrame) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for r in staged.groupBy("year", "month", "status").count().collect():
        out.setdefault(f"{r['year']}-{r['month']:02d}", {})[r["status"]] = r["count"]
    for r in removed.groupBy("year", "month").count().collect():
        out.setdefault(f"{r['year']}-{r['month']:02d}", {})["duplicates"] = r["count"]
    unmatched = (staged.where((F.col("status") == "accepted") & F.col("start_station_id").isNotNull() & F.col("start_station_key").isNull())
                 .groupBy("year", "month").count().collect())
    for r in unmatched:
        out.setdefault(f"{r['year']}-{r['month']:02d}", {})["unmatched_start_station"] = r["count"]
    return out


def check_months(counts: dict[str, dict], bronze_ledger: dict, months: list[str]) -> dict[str, list[dict]]:
    res = {}
    for m in months:
        c = counts.get(m, {})
        total = c.get("accepted", 0) + c.get("rejected", 0) + c.get("duplicates", 0)
        res[m] = [{"name": "row_accounting", "ok": total == bronze_ledger[m]["rows"],
                   "detail": f"bronze {bronze_ledger[m]['rows']} = accepted {c.get('accepted', 0)} + rejected {c.get('rejected', 0)} + duplicates {c.get('duplicates', 0)}"},
                  {"name": "station_keys_complete", "ok": c.get("unmatched_start_station", 0) == 0,
                   "detail": f"{c.get('unmatched_start_station', 0)} accepted trips without a start station key"}]
    return res


def quality_table(staged: DataFrame, removed: DataFrame, run_id: str) -> DataFrame:
    flags = ["is_incomplete", "is_valet", "outside_nyc", "birth_year_nulled", "dst_repaired", "duration_mismatch"]
    acc = F.col("status") == "accepted"
    aggs = [F.sum(acc.cast("int")).alias("accepted"), F.sum((~acc).cast("int")).alias("rejected")]
    aggs += [F.sum(F.array_contains("reject_reasons", r).cast("int")).alias(f"hit_{r}") for r in silver.REASONS]
    aggs += [F.sum(F.coalesce(F.col("reject_reason") == r, F.lit(False)).cast("int")).alias(f"first_{r}") for r in silver.REASONS]
    aggs += [F.sum(F.col(f).cast("int")).alias(f"flag_{f}") for f in flags]
    aggs += [F.sum((acc & F.col("end_station_key").isNull()).cast("int")).alias("accepted_without_end_station_key"),
             F.sum((acc & F.col("start_station_key").isNull()).cast("int")).alias("accepted_without_start_station_key"),
             F.sum((acc & F.col("birth_year").isNull()).cast("int")).alias("accepted_without_birth_year"),
             F.sum((acc & F.col("distance_m").isNull()).cast("int")).alias("accepted_without_distance")]
    q = staged.groupBy("schema", "year", "month").agg(*aggs)
    d = (removed.groupBy("schema", "year", "month")
         .agg(F.count(F.lit(1)).alias("duplicates_removed"),
              *[F.sum((F.col("kind") == k).cast("int")).alias(f"dup_{k}")
                for k in ("same_month_trip_id", "cross_month_trip_id", "cross_month_prior", "same_bike_start")]))
    out = q.join(d, ["schema", "year", "month"], "left")
    for c in [c for c in out.columns if c.startswith("dup")]:
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit(0)))
    return (out.withColumn("input_rows", F.col("accepted") + F.col("rejected") + F.col("duplicates_removed"))
            .withColumn("run_id", F.lit(run_id)))


# ---------------------------------------------------------------- the run

def run(spark: SparkSession, cfg: dict, months=None, force: bool = False, run_id: str | None = None) -> dict:
    lake = cfg["paths"]["lake_root"]
    run_id = run_id or bronze.new_run_id()
    rules = silver.load_rules()
    smap_path = os.path.join(root(lake), "station_map")
    if not os.path.exists(smap_path):
        raise FileNotFoundError(f"{smap_path} missing: build the station table first (scripts/build_stations.py)")
    smap = spark.read.parquet(smap_path)

    b_ledger, s_ledger = bronze.read_ledger(lake), read_ledger(lake)
    batch, skipped, not_ready = plan(b_ledger, s_ledger, months or cfg["scope"]["months"], rules_digest(),
                                     station_digests(spark, lake, smap) or stations_digest(smap), force)
    result = {"run_id": run_id, "processed": [], "skipped": skipped, "not_ready": not_ready, "failed": [], "checks": {}}
    if not batch:
        append_log(lake, [{"run_id": run_id, "env": cfg["env"], "status": "nothing_to_do", "skipped": len(skipped),
                           "not_ready": not_ready, "finished_at": datetime.now(timezone.utc).isoformat()}])
        return result

    t0 = time.time()
    by_schema: dict[str, list[str]] = {}
    for m in batch:
        by_schema.setdefault(b_ledger[m]["schema"], []).append(m)
    prior = sorted({prev_month(m) for m in batch if prev_month(m) not in batch and prev_month(m) in s_ledger})

    final, removed = build_batch(spark, lake, rules, by_schema, smap, prior)
    final = final.withColumn("status", F.when(F.size("reject_reasons") == 0, "accepted").otherwise("rejected"))
    staging = os.path.join(root(lake), "_staging", run_id)
    shutil.rmtree(staging, ignore_errors=True)
    final.write.mode("overwrite").partitionBy("year", "month", "status").parquet(os.path.join(staging, "all"))
    removed.coalesce(1).write.mode("overwrite").parquet(os.path.join(staging, "duplicates"))
    staged, removed_s = spark.read.parquet(os.path.join(staging, "all")), spark.read.parquet(os.path.join(staging, "duplicates"))

    counts = monthly_counts(staged, removed_s)
    checks = check_months(counts, b_ledger, sorted(batch))
    ok_months = [m for m, cs in checks.items() if all(c["ok"] for c in cs)]
    result["failed"] = sorted(set(batch) - set(ok_months))
    result["checks"] = checks

    if ok_months:
        yms = [_ym(m) for m in ok_months]
        in_ok = (F.col("year") * 100 + F.col("month")).isin(yms)
        out = root(lake)
        (staged.where(in_ok & (F.col("status") == "accepted")).drop("status", "reject_reasons", "reject_reason")
         .write.mode("overwrite").partitionBy("year", "month").parquet(os.path.join(out, "trips")))
        (staged.where(in_ok & (F.col("status") == "rejected")).drop("status")
         .write.mode("overwrite").partitionBy("year", "month").parquet(os.path.join(out, "rejected")))
        removed_s.where(in_ok).write.mode("overwrite").partitionBy("year", "month").parquet(os.path.join(out, "duplicates"))
        quality_table(staged.where(in_ok), removed_s.where(in_ok), run_id).write.mode("overwrite") \
            .partitionBy("year", "month").parquet(os.path.join(out, "quality"))
    seconds = round(time.time() - t0, 1)

    log = []
    for m in sorted(batch):
        c = counts.get(m, {})
        rec = {"run_id": run_id, "env": cfg["env"], "month": m, "schema": b_ledger[m]["schema"],
               "bronze_rows": b_ledger[m]["rows"], "accepted": c.get("accepted", 0), "rejected": c.get("rejected", 0),
               "duplicates": c.get("duplicates", 0), "status": "ok" if m in ok_months else "check_failed",
               "checks": checks[m], "batch_seconds": seconds, "finished_at": datetime.now(timezone.utc).isoformat()}
        log.append(rec)
        if m in ok_months:
            s_ledger[m] = {"digest": batch[m], "schema": rec["schema"], "bronze_rows": rec["bronze_rows"], "accepted": rec["accepted"],
                           "rejected": rec["rejected"], "duplicates": rec["duplicates"], "run_id": run_id}
        else:
            s_ledger.pop(m, None)
    write_ledger(lake, s_ledger)
    append_log(lake, log)
    if not result["failed"]:
        shutil.rmtree(staging, ignore_errors=True)   # kept for inspection when a check failed
    result.update(processed=ok_months, seconds=seconds, log=log)
    return result
