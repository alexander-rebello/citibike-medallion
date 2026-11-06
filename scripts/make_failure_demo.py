#!/usr/bin/env python3
"""Write the manifest for the failure demo: the real 2024-06 records, one with an unregistered header.

  python scripts/make_failure_demo.py
  CITIBIKE_ENV=failure_demo python scripts/run_pipeline.py --skip download     # must fail at select_sources
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from citibike import config  # noqa: E402

BAD = "deadbeefdeadbeef"


def main():
    src = os.path.expanduser("~/citibike/data/manifest/raw_manifest.jsonl")
    cfg = config.load("failure_demo")
    dst = cfg["paths"]["manifest"]
    recs = [r for r in map(json.loads, open(src)) if r.get("year") == 2024 and r.get("month") == 6]
    files = [r for r in recs if r.get("record") == "file"]
    assert files, "no 2024-06 file records in the real manifest"
    files[0]["header_sha256"] = BAD
    files[0]["header"] = "ride_id,started_at,ended_at,SOMETHING_NEW"
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    with open(dst, "w") as f:
        for r in recs:
            f.write(json.dumps(r) + "\n")
    print(f"wrote {len(recs)} records to {dst}; file 1 now has header fingerprint {BAD}")


if __name__ == "__main__":
    main()
