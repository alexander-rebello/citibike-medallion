#!/usr/bin/env python3
"""Pick one file set per month from the raw manifest and check every header is registered.

Writes <manifest dir>/source_selection.json and prints a summary.
Exit code 1 if any month has conflicting file sets or an unregistered header.

  CITIBIKE_ENV=prod python scripts/select_sources.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from citibike import config, registry, sources  # noqa: E402


def main():
    cfg = config.load()
    selection = sources.select(sources.read_manifest(cfg["paths"]["manifest"]))
    problems = 0
    for month, s in selection.items():
        for fp in s["schemas"]:
            try:
                s.setdefault("schema_names", []).append(registry.lookup(fp)["schema"])
            except registry.UnknownHeaderError as e:
                print(f"{month}: {e}")
                problems += 1
        if s["status"] == "conflict":
            problems += 1
        if s["status"] != "single_set" or len(s["schemas"]) > 1:
            dropped = ", ".join(str(d["rows"]) for d in s["dropped"])
            print(f"{month}: {s['status']:24} keep {len(s['selected'])} file(s) / {s['rows']:,} rows"
                  f"  dropped sets: [{dropped}]  schemas: {s.get('schema_names')}")

    out = os.path.join(os.path.dirname(cfg["paths"]["manifest"]), "source_selection.json")
    with open(out, "w") as f:
        json.dump(selection, f, indent=1)

    by_status = {}
    for s in selection.values():
        by_status[s["status"]] = by_status.get(s["status"], 0) + 1
    total = sum(s["rows"] for s in selection.values())
    print(f"\n{len(selection)} months, {total:,} rows selected, {by_status}; wrote {out}")
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
