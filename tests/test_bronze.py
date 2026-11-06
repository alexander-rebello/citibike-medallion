"""Bronze on tiny raw files built from real sample rows of each era."""
import gzip
import json
import os

import pytest

from citibike import bronze, sources

LEGACY_QUOTED = (
    '"tripduration","starttime","stoptime","start station id","start station name","start station latitude",'
    '"start station longitude","end station id","end station name","end station latitude","end station longitude",'
    '"bikeid","usertype","birth year","gender"')
LEGACY_ROWS = [
    '695,"2013-06-01 00:00:01","2013-06-01 00:11:36",444,"Broadway & W 24 St",40.7423543,-73.98915076,'
    '434,"9 Ave & W 18 St",40.74317449,-74.00366443,19678,"Subscriber",1983,1',
    '1346,"2013-06-01 00:01:00","2013-06-01 00:24:00",455,"1 Ave & E 44 St",40.75001986,-73.96905301,'
    '265,"Stanton St & Chrystie St",40.72229346,-73.99147535,18660,"Subscriber",1960,2',
]
MODERN = ("ride_id,rideable_type,started_at,ended_at,start_station_name,start_station_id,end_station_name,"
          "end_station_id,start_lat,start_lng,end_lat,end_lng,member_casual")
MODERN_ROWS = [
    "8E865410DBDE0CA9,electric_bike,2024-06-01 13:00:04.563,2024-06-01 13:04:04.652,3 St & 3 Ave,4028.03,"
    "Carroll St & Smith St,4225.14,40.6750705,-73.98775226,40.680611,-73.99475825,casual",
    "1C6A640ACEFB0795,classic_bike,2024-06-14 08:41:33.060,2024-06-14 08:51:11.113,E 85 St & York Ave,7146.04,"
    "E 63 St & 3 Ave,6830.02,40.77536905,-73.94803392,40.763954,-73.9646,member",
]
FP = {"legacy": "388de383f8b42cf0", "modern": "aa38470066175dea"}


def write_raw(root, ym, name, header, rows):
    d = os.path.join(root, "raw", f"year={ym[0]}", f"month={ym[1]:02d}")
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, name + ".gz")
    with gzip.open(path, "wt") as f:
        f.write(header + "\n" + "".join(r + "\n" for r in rows))
    return path


def manifest_rec(path, ym, n_rows, fp, inner):
    return {"record": "file", "source_key": "src.zip", "inner_path": inner, "line_count": n_rows + 1,
            "status": "ok", "year": ym[0], "month": ym[1], "output_path": path,
            "header_sha256": fp, "content_sha256": "c-" + inner}


@pytest.fixture
def lake(tmp_path):
    recs = [
        manifest_rec(write_raw(tmp_path, (2013, 6), "a_1.csv", LEGACY_QUOTED, LEGACY_ROWS),
                     (2013, 6), 2, FP["legacy"], "6_June/a_1.csv"),
        manifest_rec(write_raw(tmp_path, (2024, 6), "b_1.csv", MODERN, MODERN_ROWS[:1]),
                     (2024, 6), 1, FP["modern"], "b_1.csv"),
        manifest_rec(write_raw(tmp_path, (2024, 6), "b_2.csv", MODERN, MODERN_ROWS[1:] + ["broken,row"]),
                     (2024, 6), 2, FP["modern"], "b_2.csv"),
    ]
    cfg = {"env": "test", "paths": {"lake_root": str(tmp_path / "lake")},
           "scope": {"months": ["2013-06", "2024-06"]}}
    return cfg, recs


def test_ingests_both_schemas_and_reconciles(spark, lake):
    cfg, recs = lake
    res = bronze.run(spark, cfg, sources.select(recs))
    assert res["failed"] == [] and res["refused"] == []
    assert sorted(res["processed"]) == ["2013-06", "2024-06"]

    legacy = spark.read.parquet(bronze.bronze_path(cfg["paths"]["lake_root"], "legacy_v1"))
    modern = spark.read.parquet(bronze.bronze_path(cfg["paths"]["lake_root"], "modern_v2"))
    assert legacy.count() == 2 and modern.count() == 3

    # values unchanged and still strings, quotes removed by the CSV parser only
    row = legacy.where("bikeid = '19678'").first()
    assert row["starttime"] == "2013-06-01 00:00:01" and row["startstationname"] == "Broadway & W 24 St"
    for name, dtype in legacy.dtypes:
        if not name.startswith("_") and name not in ("year", "month"):
            assert dtype == "string", name
    assert modern.where("startstationid = '4028.03'").count() == 1   # ids not mangled into numbers

    meta = modern.select("_source_file", "_schema_version", "_header_fingerprint", "_run_id").distinct().collect()
    assert {os.path.basename(m["_source_file"]) for m in meta} == {"b_1.csv.gz", "b_2.csv.gz"}
    assert {m["_schema_version"] for m in meta} == {"modern_v2"}
    assert {m["_run_id"] for m in meta} == {res["run_id"]}


def test_malformed_row_is_kept_and_counted(spark, lake):
    cfg, recs = lake
    res = bronze.run(spark, cfg, sources.select(recs))
    rec = next(r for r in res["log"] if r["month"] == "2024-06")
    assert rec["corrupt_rows"] == 1 and rec["status"] == "ok"
    modern = spark.read.parquet(bronze.bronze_path(cfg["paths"]["lake_root"], "modern_v2"))
    assert modern.where("_corrupt_record = 'broken,row'").count() == 1


def test_ledger_skips_unchanged_months_and_force_reprocesses(spark, lake):
    cfg, recs = lake
    sel = sources.select(recs)
    bronze.run(spark, cfg, sel)
    again = bronze.run(spark, cfg, sel)
    assert again["processed"] == [] and sorted(again["skipped"]) == ["2013-06", "2024-06"]
    forced = bronze.run(spark, cfg, sel, months=["2024-06"], force=True)
    assert forced["processed"] == ["2024-06"]


def test_rerun_of_one_month_leaves_other_months_untouched(spark, lake):
    cfg, recs = lake
    sel = sources.select(recs)
    first = bronze.run(spark, cfg, sel)
    bronze.run(spark, cfg, sel, months=["2024-06"], force=True)
    legacy = spark.read.parquet(bronze.bronze_path(cfg["paths"]["lake_root"], "legacy_v1"))
    modern = spark.read.parquet(bronze.bronze_path(cfg["paths"]["lake_root"], "modern_v2"))
    assert legacy.select("_run_id").distinct().first()[0] == first["run_id"]
    assert modern.count() == 3                       # replaced, not appended


def test_row_count_mismatch_fails_and_keeps_month_out_of_ledger(spark, lake):
    cfg, recs = lake
    recs[0]["line_count"] += 5                       # manifest promises rows the file does not have
    res = bronze.run(spark, cfg, sources.select(recs))
    assert res["failed"] == ["2013-06"]
    ledger = bronze.read_ledger(cfg["paths"]["lake_root"])
    assert "2013-06" not in ledger and "2024-06" in ledger


def test_unknown_header_and_conflicts_are_refused(spark, lake):
    cfg, recs = lake
    recs[0]["header_sha256"] = "0000000000000000"
    res = bronze.run(spark, cfg, sources.select(recs))
    assert [r["month"] for r in res["refused"]] == ["2013-06"]
    assert res["processed"] == ["2024-06"]


def test_run_log_records_every_month(spark, lake):
    cfg, recs = lake
    res = bronze.run(spark, cfg, sources.select(recs))
    path = os.path.join(bronze.meta_dir(cfg["paths"]["lake_root"]), "bronze_runs.jsonl")
    lines = [json.loads(line) for line in open(path)]
    assert {line["month"] for line in lines if line["run_id"] == res["run_id"]} == {"2013-06", "2024-06"}
