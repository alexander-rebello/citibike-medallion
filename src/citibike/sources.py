"""Choose exactly one set of raw files per month.

The yearly source archives sometimes contain the same month twice in different layouts,
e.g. 2013-citibike-tripdata/201309-citibike-tripdata.csv (one file) and
2013-citibike-tripdata/9_September/201309-citibike-tripdata_{1,2}.csv (split at 1M rows).
Raw keeps every file unchanged; this module decides which set Bronze reads, and proves
that the dropped sets hold the same number of rows. Sets that disagree are never dropped
silently: the month is marked 'conflict' and Bronze refuses it.
"""
import json
import posixpath
import re
from collections import defaultdict

MONTH_DIR = re.compile(r"^\d{1,2}_[A-Za-z]+$")   # e.g. 9_September
SPLIT_FILE = re.compile(r"_\d+\.csv$", re.IGNORECASE)


def read_manifest(path: str) -> list[dict]:
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def data_rows(rec: dict) -> int:
    return rec["line_count"] - 1  # minus header


def set_key(rec: dict) -> tuple[str, str, bool]:
    inner = rec["inner_path"]
    return rec["source_key"], posixpath.dirname(inner), bool(SPLIT_FILE.search(inner))


def priority(key: tuple[str, str, bool]) -> tuple:
    _, directory, is_split = key
    in_month_dir = bool(MONTH_DIR.match(posixpath.basename(directory)))
    # month folder first (the layout every year uses), then split files, then single files
    return (not in_month_dir, not is_split, key)


def select(manifest_records: list[dict]) -> dict[str, dict]:
    files = [r for r in manifest_records
             if r.get("record") == "file" and r.get("status") in ("ok", "name_collision_kept")]
    months = defaultdict(lambda: defaultdict(list))
    for r in files:
        months[f"{r['year']}-{r['month']:02d}"][set_key(r)].append(r)

    out = {}
    for month, sets in sorted(months.items()):
        ranked = sorted(sets, key=priority)
        rows = {k: sum(data_rows(r) for r in sets[k]) for k in ranked}
        chosen = ranked[0]
        if len(ranked) == 1:
            status = "single_set"
        elif len(set(rows.values())) == 1:
            status = "duplicate_sets_dropped"
        else:
            status = "conflict"
        out[month] = {
            "status": status,
            "rows": rows[chosen],
            "selected": sorted(r["output_path"] for r in sets[chosen]),
            "dropped": [
                {"files": sorted(r["output_path"] for r in sets[k]), "rows": rows[k]}
                for k in ranked[1:]
            ],
            "schemas": sorted({r["header_sha256"] for r in sets[chosen]}),
        }
    return out
