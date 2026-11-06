"""Bronze: load the selected raw CSV files unchanged into Parquet, one dataset per logical schema.

* every source column is kept as a string, renamed only to its normalized registry name
* rows with the wrong number of fields are kept, with the raw line in _corrupt_record
* metadata per row: _source_file, _header_fingerprint, _schema_version, _run_id, _ingested_at
* output: <lake>/bronze/trips_<schema>/year=YYYY/month=M  (dynamic overwrite: only months in this run)
* after writing, row counts are reconciled per month against the raw manifest;
  only reconciled months enter the ledger, and Silver reads only ledger months
* the ledger also prevents double ingestion: unchanged months are skipped unless force=True
"""
import datetime as dt
import hashlib
import json
import os
import time
import uuid
from collections import defaultdict

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from citibike import registry

READY = ("single_set", "duplicate_sets_dropped")


def new_run_id() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


def bronze_path(lake_root: str, schema: str) -> str:
    return os.path.join(lake_root, "bronze", f"trips_{schema}")


def meta_dir(lake_root: str) -> str:
    return os.path.join(lake_root, "_meta")


def read_ledger(lake_root: str) -> dict:
    path = os.path.join(meta_dir(lake_root), "bronze_ledger.json")
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


def write_ledger(lake_root: str, ledger: dict) -> None:
    os.makedirs(meta_dir(lake_root), exist_ok=True)
    path = os.path.join(meta_dir(lake_root), "bronze_ledger.json")
    with open(path + ".tmp", "w") as f:
        json.dump(ledger, f, indent=1, sort_keys=True)
    os.replace(path + ".tmp", path)


def append_run_log(lake_root: str, records: list[dict]) -> None:
    os.makedirs(meta_dir(lake_root), exist_ok=True)
    with open(os.path.join(meta_dir(lake_root), "bronze_runs.jsonl"), "a") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def selection_digest(sel: dict) -> str:
    """Identity of a month's input: which files, with which content."""
    key = "\n".join(f"{f['path']}|{f['content_sha256']}" for f in sel["selected_files"])
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def csv_schema(columns: list[str]) -> T.StructType:
    return T.StructType([T.StructField(c, T.StringType()) for c in columns]
                        + [T.StructField("_corrupt_record", T.StringType())])


def plan(selection: dict, months: list[str] | str, ledger: dict, force: bool):
    """Split requested months into work items, skips and refusals (no Spark needed)."""
    wanted = sorted(selection) if months == "all" else sorted(months)
    work, skipped, refused = [], [], []
    for m in wanted:
        sel = selection.get(m)
        if sel is None:
            refused.append({"month": m, "reason": "month not in raw manifest"})
            continue
        if sel["status"] not in READY:
            refused.append({"month": m, "reason": f"source selection status {sel['status']}"})
            continue
        try:
            infos = {f["header_sha256"]: registry.lookup(f["header_sha256"]) for f in sel["selected_files"]}
        except registry.UnknownHeaderError as e:
            refused.append({"month": m, "reason": str(e)})
            continue
        schemas = {i["schema"] for i in infos.values()}
        if len(schemas) != 1:
            refused.append({"month": m, "reason": f"files of one month span schemas {sorted(schemas)}"})
            continue
        digest = selection_digest(sel)
        if not force and ledger.get(m, {}).get("digest") == digest:
            skipped.append(m)
            continue
        groups = defaultdict(list)
        for f in sel["selected_files"]:
            groups[f["header_sha256"]].append(f["path"])
        work.append({"month": m, "schema": schemas.pop(), "digest": digest,
                     "expected_rows": sel["rows"], "files": len(sel["selected_files"]),
                     "groups": [(fp, infos[fp], paths) for fp, paths in sorted(groups.items())]})
    return work, skipped, refused


def read_month(spark: SparkSession, item: dict, run_id: str) -> DataFrame:
    year, month = map(int, item["month"].split("-"))
    parts = []
    for fp, info, paths in item["groups"]:
        df = (spark.read.schema(csv_schema(info["columns"]))
              .option("header", True)          # header already verified via its fingerprint
              .option("mode", "PERMISSIVE")
              .option("columnNameOfCorruptRecord", "_corrupt_record")
              .csv(paths))
        parts.append(df.select(
            *info["columns"], "_corrupt_record",
            F.regexp_replace(F.col("_metadata.file_path"), r"^file:", "").alias("_source_file"),
            F.lit(fp).alias("_header_fingerprint"),
            F.lit(info["schema"]).alias("_schema_version"),
            F.lit(run_id).alias("_run_id"),
            F.current_timestamp().alias("_ingested_at"),
            F.lit(year).alias("year"),
            F.lit(month).alias("month"),
        ))
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


def reconcile(spark: SparkSession, path: str, items: list[dict]) -> dict[str, dict]:
    keys = [tuple(map(int, i["month"].split("-"))) for i in items]
    cond = F.lit(False)
    for y, m in keys:
        cond = cond | ((F.col("year") == y) & (F.col("month") == m))
    rows = (spark.read.parquet(path).where(cond)
            .groupBy("year", "month")
            .agg(F.count(F.lit(1)).alias("rows"),
                 F.count("_corrupt_record").alias("corrupt"))
            .collect())
    return {f"{r['year']}-{r['month']:02d}": {"rows": r["rows"], "corrupt": r["corrupt"]} for r in rows}


def run(spark: SparkSession, cfg: dict, selection: dict, months=None, force=False, run_id=None) -> dict:
    lake = cfg["paths"]["lake_root"]
    run_id = run_id or new_run_id()
    months = months or cfg["scope"]["months"]
    ledger = read_ledger(lake)
    work, skipped, refused = plan(selection, months, ledger, force)

    log, failed = [], []
    by_schema = defaultdict(list)
    for item in work:
        by_schema[item["schema"]].append(item)

    for schema, items in sorted(by_schema.items()):
        path = bronze_path(lake, schema)
        t0 = time.time()
        frames = [read_month(spark, i, run_id) for i in items]
        df = frames[0]
        for f in frames[1:]:
            df = df.unionByName(f)
        # one task per source file (gzip is not splittable): files of ~1M rows -> Parquet files of tens of MB
        df.write.mode("overwrite").partitionBy("year", "month").parquet(path)
        seconds = round(time.time() - t0, 1)
        counts = reconcile(spark, path, items)

        for i in items:
            got = counts.get(i["month"], {"rows": 0, "corrupt": 0})
            ok = got["rows"] == i["expected_rows"]
            rec = {"run_id": run_id, "env": cfg["env"], "month": i["month"], "schema": schema,
                   "files": i["files"], "expected_rows": i["expected_rows"], "bronze_rows": got["rows"],
                   "corrupt_rows": got["corrupt"], "status": "ok" if ok else "reconcile_failed",
                   "batch_seconds": seconds, "finished_at": dt.datetime.now(dt.timezone.utc).isoformat()}
            log.append(rec)
            if ok:
                ledger[i["month"]] = {"digest": i["digest"], "schema": schema, "rows": got["rows"],
                                      "corrupt_rows": got["corrupt"], "run_id": run_id}
            else:
                ledger.pop(i["month"], None)   # never let Silver read an unreconciled month
                failed.append(i["month"])

    write_ledger(lake, ledger)
    for r in refused:
        log.append({"run_id": run_id, "env": cfg["env"], "month": r["month"], "status": "refused",
                    "reason": r["reason"], "finished_at": dt.datetime.now(dt.timezone.utc).isoformat()})
    append_run_log(lake, log)
    return {"run_id": run_id, "processed": [i["month"] for i in work if i["month"] not in failed],
            "skipped": skipped, "refused": refused, "failed": failed, "log": log}
