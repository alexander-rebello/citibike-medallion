#!/usr/bin/env python3
"""Bronze ingestion for the configured environment.

  CITIBIKE_ENV=dev  python scripts/run_bronze.py                 # the dev months
  CITIBIKE_ENV=prod python scripts/run_bronze.py                 # every month, unchanged ones skipped
  CITIBIKE_ENV=prod python scripts/run_bronze.py --months 2024-06 --force

Exit code 1 if any month was refused or failed reconciliation.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from citibike import bronze, config, sources  # noqa: E402
from citibike.spark import get_spark  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", nargs="+", help="YYYY-MM ... (default: config scope)")
    ap.add_argument("--force", action="store_true", help="re-ingest months already in the ledger")
    args = ap.parse_args()

    cfg = config.load()
    selection = sources.select(sources.read_manifest(cfg["paths"]["manifest"]))
    spark = get_spark(cfg, "bronze")
    spark.sparkContext.setLogLevel("WARN")
    # headers are verified by fingerprint and renamed on purpose; Spark's name check would warn per file
    jvm = spark.sparkContext._jvm
    jvm.org.apache.logging.log4j.core.config.Configurator.setLevel(
        "org.apache.spark.sql.catalyst.csv.CSVHeaderChecker", jvm.org.apache.logging.log4j.Level.ERROR)
    res = bronze.run(spark, cfg, selection, months=args.months, force=args.force)

    for r in res["log"]:
        if r["status"] == "refused":
            print(f"{r['month']}  REFUSED  {r['reason']}")
        else:
            print(f"{r['month']}  {r['status']:16} {r['bronze_rows']:>11,} / {r['expected_rows']:>11,} rows"
                  f"  corrupt={r['corrupt_rows']}  files={r['files']}  [{r['schema']}]")
    print(f"\nrun {res['run_id']} ({cfg['env']}): {len(res['processed'])} ingested, "
          f"{len(res['skipped'])} unchanged/skipped, {len(res['refused'])} refused, {len(res['failed'])} failed")
    spark.stop()
    sys.exit(1 if res["refused"] or res["failed"] else 0)


if __name__ == "__main__":
    main()
