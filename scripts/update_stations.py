#!/usr/bin/env python3
"""Update the station dimension incrementally: scan only new or changed Bronze months, then rebuild the (small) station table.

  CITIBIKE_ENV=prod CITIBIKE_SPARK_MASTER=spark://192.168.26.92:7077 python scripts/update_stations.py
  CITIBIKE_ENV=prod python scripts/update_stations.py --force        # rescan every month

Writes <lake>/silver/{station_observations_monthly,station_observations,stations,station_map,station_conflicts,
station_coord_ambiguous}. A changed station key is reported: Silver will re-process the months that use it.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from citibike import config, silver, stations  # noqa: E402
from citibike.spark import get_spark  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="rescan all months")
    args = ap.parse_args()
    cfg = config.load()
    spark = get_spark(cfg, "stations")
    spark.sparkContext.setLogLevel("WARN")
    s = stations.update(spark, cfg, silver.load_rules(), force=args.force)
    print(f"scanned {len(s['scanned_months'])} months in {s['scan_seconds']} s; {s['records']} records -> {s['stations']} stations; "
          f"{s['new_records']} new records, {s['key_changes']} station keys changed, {s['records_no_longer_present']} records gone")
    spark.stop()


if __name__ == "__main__":
    main()
