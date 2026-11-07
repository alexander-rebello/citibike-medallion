#!/usr/bin/env python3
"""Run the monthly pipeline for the configured environment: download -> check sources -> Bronze (-> Silver -> Gold).

  CITIBIKE_ENV=prod python scripts/run_pipeline.py                  # newest data, unchanged months are skipped
  CITIBIKE_ENV=prod python scripts/run_pipeline.py --months 2026-10 --force
  CITIBIKE_ENV=prod python scripts/run_pipeline.py --summary        # also print a markdown summary (CI job summary)

Exit code 1 if any step failed; later steps are then not run. Run log: <lake>/_meta/pipeline_runs.jsonl.
"""
import argparse
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "src"))

from citibike import config, pipeline  # noqa: E402


def build_steps(py: str, years: list[int], months: list[str] | None, force: bool, skip: tuple[str, ...] = ()):
    bronze = [py, os.path.join(HERE, "run_bronze.py")]
    if months:
        bronze += ["--months", *months]
    if force:
        bronze.append("--force")
    steps = [
        ("download", [py, os.path.join(HERE, "download_raw.py"), "--years", *map(str, years)]),
        ("select_sources", [py, os.path.join(HERE, "select_sources.py")]),
        ("bronze", bronze),
        ("silver", [py, os.path.join(HERE, "run_silver.py")] + (["--months", *months] if months else []) + (["--force"] if force else [])),
        ("gold", [py, os.path.join(HERE, "run_gold.py")]),
    ]
    return [(name, cmd) for name, cmd in steps if name not in skip]


def markdown(rec: dict) -> str:
    rows = "\n".join(f"| {s['name']} | {s['status']} | {s['seconds']} s |" for s in rec["steps"])
    skipped = f"\nNot run: {', '.join(rec['skipped'])}" if rec.get("skipped") else ""
    return (f"### Pipeline run {rec['run_id']} ({rec['env']}): **{rec['status']}**\n\n| step | status | time |\n|---|---|---|\n"
            f"{rows}\n{skipped}\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", nargs="+", help="YYYY-MM ... (default: everything new)")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--years", type=int, nargs="+", help="years to download (default: this year, plus last year in January)")
    ap.add_argument("--trigger", default=os.environ.get("GITHUB_EVENT_NAME", "manual"))
    ap.add_argument("--skip", nargs="+", default=[], help="step names to leave out, e.g. download")
    ap.add_argument("--summary", action="store_true")
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    years = args.years or ([now.year - 1, now.year] if now.month == 1 else [now.year])
    cfg = config.load()
    meta = os.path.join(cfg["paths"]["lake_root"], "_meta")
    rec = pipeline.run(build_steps(sys.executable, years, args.months, args.force, tuple(args.skip)), meta, cfg["env"], args.trigger)

    text = markdown(rec)
    print(text)
    if args.summary and os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(text)
    if rec["status"] != "success":
        failed = rec["steps"][-1]
        print(f"FAILED at step {failed['name']}; see {failed['log']}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
