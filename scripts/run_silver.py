#!/usr/bin/env python3
"""Silver for the configured environment: trips, rejected, duplicates, quality (needs Bronze and the station table).

  CITIBIKE_ENV=dev  python scripts/run_silver.py
  CITIBIKE_ENV=prod CITIBIKE_SPARK_MASTER=spark://192.168.26.92:7077 python scripts/run_silver.py
  CITIBIKE_ENV=prod python scripts/run_silver.py --months 2026-10 --force

Exit code 1 if a month failed its checks (it is then not published and not in the Silver ledger).
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from citibike import config, silver_run  # noqa: E402
from citibike.spark import get_spark  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", nargs="+", help="YYYY-MM ... (default: config scope)")
    ap.add_argument("--force", action="store_true", help="re-process months that are already up to date")
    args = ap.parse_args()

    cfg = config.load()
    spark = get_spark(cfg, "silver")
    spark.sparkContext.setLogLevel("WARN")
    res = silver_run.run(spark, cfg, months=args.months, force=args.force)
    for r in res.get("log", []):
        print(f"{r['month']}  {r['status']:13} bronze {r['bronze_rows']:>11,} = accepted {r['accepted']:>11,} + rejected {r['rejected']:>8,}"
              f" + duplicates {r['duplicates']:>4,}")
        for c in r["checks"]:
            if not c["ok"]:
                print(f"           CHECK FAILED {c['name']}: {c['detail']}")
    print(f"\nrun {res['run_id']} ({cfg['env']}): {len(res['processed'])} published, {len(res['skipped'])} up to date, "
          f"{len(res['not_ready'])} not in the Bronze ledger, {len(res['failed'])} failed checks")
    spark.stop()
    sys.exit(1 if res["failed"] else 0)


if __name__ == "__main__":
    main()
