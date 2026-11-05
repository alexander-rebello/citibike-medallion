#!/usr/bin/env python3
"""
Citi Bike raw downloader (Bronze landing step).

For each configured year:
  1. List the public S3 bucket and pick that year's archives
     (a yearly archive if one exists, otherwise the monthly ones; JC files skipped).
  2. Download one archive at a time (resumable, retried).
  3. Stream every inner CSV (including CSVs inside nested zips) into gzip under
     raw/citibike/year=YYYY/month=MM/ -- the CSV bytes are unchanged, only stored compressed.
  4. Record checksums, sizes, line counts and the header fingerprint in a JSONL manifest.
  5. Delete the archive and move on.

Re-running is safe: finished archives are skipped, partial downloads resume.
Only the Python standard library is used.

Usage:
  nohup python3 download_raw.py > download.log 2>&1 &
  python3 download_raw.py --years 2015 2019 --data-dir ~/citibike/data
"""
import argparse
import datetime as dt
import gzip
import hashlib
import json
import os
import re
import shutil
import sys
import time
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET
import zipfile

BUCKET_URL = "https://s3.amazonaws.com/tripdata"
DEFAULT_YEARS = [2015, 2019, 2020, 2024]
CHUNK = 1024 * 1024
MIN_FREE_BYTES = 3 * 1024**3  # always keep this much free on top of what an archive needs
MONTH_NAMES = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}

RUN_ID = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


def header_fingerprint(header: bytes) -> str:
    """Exact-header identity used by the schema registry (src/citibike/registry.py must match)."""
    return hashlib.sha256(header.strip().lower()).hexdigest()[:16]


def log(msg):
    print(f"{dt.datetime.now():%Y-%m-%d %H:%M:%S} {msg}", flush=True)


def now_iso():
    return dt.datetime.now(dt.timezone.utc).isoformat()


# ---------------------------------------------------------------- listing

def list_bucket():
    """Return [{'key':..., 'size':..., 'etag':...}] for every object in the bucket."""
    objects, token = [], None
    while True:
        params = {"list-type": "2"}
        if token:
            params["continuation-token"] = token
        url = BUCKET_URL + "?" + urllib.parse.urlencode(params)
        with urllib.request.urlopen(url, timeout=60) as r:
            root = ET.fromstring(r.read())
        ns = {"s3": root.tag.split("}")[0].strip("{")} if root.tag.startswith("{") else {}
        p = "s3:" if ns else ""
        for c in root.findall(f"{p}Contents", ns):
            objects.append({
                "key": c.find(f"{p}Key", ns).text,
                "size": int(c.find(f"{p}Size", ns).text),
                "etag": (c.find(f"{p}ETag", ns).text or "").strip('"'),
            })
        truncated = root.find(f"{p}IsTruncated", ns)
        if truncated is not None and truncated.text == "true":
            token = root.find(f"{p}NextContinuationToken", ns).text
        else:
            return objects


def select_archives(objects, year):
    """Yearly archive if it exists, otherwise the monthly archives for that year."""
    zips = [o for o in objects
            if o["key"].lower().endswith(".zip") and not o["key"].upper().startswith("JC")]
    yearly = [o for o in zips if re.fullmatch(rf"{year}-citibike-tripdata(\.csv)?\.zip", o["key"])]
    if yearly:
        return yearly
    return sorted((o for o in zips if re.match(rf"{year}(0[1-9]|1[0-2])-citibike-tripdata", o["key"])),
                  key=lambda o: o["key"])


# ---------------------------------------------------------------- download

def download(obj, dest, retries=20):
    """Resumable download into dest; returns sha256 of the full file."""
    url = f"{BUCKET_URL}/{urllib.parse.quote(obj['key'])}"
    part = dest + ".part"
    for attempt in range(1, retries + 1):
        have = os.path.getsize(part) if os.path.exists(part) else 0
        if have == obj["size"]:
            break
        if have > obj["size"]:
            os.remove(part)
            have = 0
        req = urllib.request.Request(url, headers={"Range": f"bytes={have}-"} if have else {})
        try:
            with urllib.request.urlopen(req, timeout=120) as r, open(part, "ab" if have else "wb") as f:
                if have and r.status != 206:  # server ignored Range: start over
                    f.seek(0)
                    f.truncate()
                    have = 0
                last = time.time()
                while True:
                    chunk = r.read(CHUNK)
                    if not chunk:
                        break
                    f.write(chunk)
                    have += len(chunk)
                    if time.time() - last > 60:
                        log(f"  {obj['key']}: {have / 1e6:,.0f} / {obj['size'] / 1e6:,.0f} MB")
                        last = time.time()
        except Exception as e:  # network hiccup: wait and resume
            wait = min(300, 10 * attempt)
            log(f"  download error ({e}); attempt {attempt}/{retries}, retrying in {wait}s")
            time.sleep(wait)
    if os.path.getsize(part) != obj["size"]:
        raise RuntimeError(f"size mismatch for {obj['key']}")
    sha = hashlib.sha256()
    with open(part, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK), b""):
            sha.update(chunk)
    os.replace(part, dest)
    return sha.hexdigest()


# ---------------------------------------------------------------- extraction

def year_month_from_path(path):
    """Find YYYY/MM in an inner file path; None if it can't be determined."""
    base = os.path.basename(path)
    m = re.search(r"(20\d{2})[-_ ]?(0[1-9]|1[0-2])(?!\d)", base)
    if m:
        return int(m.group(1)), int(m.group(2))
    y = re.search(r"(20\d{2})", path)
    for part in reversed(path.lower().replace("\\", "/").split("/")):
        for name, num in MONTH_NAMES.items():
            if name in part and y:
                return int(y.group(1)), num
    return None


def stream_csv(zf, info, out_path):
    """Copy one CSV member into a gzip file, hashing and counting as it streams."""
    sha, lines, header = hashlib.sha256(), 0, b""
    tmp = out_path + ".tmp"
    with zf.open(info) as src, gzip.open(tmp, "wb", compresslevel=6) as dst:
        first = True
        for chunk in iter(lambda: src.read(CHUNK), b""):  # zipfile verifies CRC32 at EOF
            if first:
                header = chunk.split(b"\n", 1)[0].rstrip(b"\r")
                first = False
            sha.update(chunk)
            lines += chunk.count(b"\n")
            dst.write(chunk)
    os.replace(tmp, out_path)
    return sha.hexdigest(), lines, header


def process_zip(zip_path, source, raw_dir, tmp_dir, manifest, inner_prefix=""):
    """Handle every CSV in a zip, recursing into nested zips."""
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            name = info.filename
            inner = inner_prefix + name
            if info.is_dir() or "__MACOSX" in name or os.path.basename(name).startswith("._"):
                continue
            if name.lower().endswith(".zip"):
                nested = os.path.join(tmp_dir, f"nested-{uuid.uuid4().hex}.zip")
                with zf.open(info) as src, open(nested, "wb") as dst:
                    shutil.copyfileobj(src, dst, CHUNK)
                try:
                    process_zip(nested, source, raw_dir, tmp_dir, manifest, inner + "!/")
                finally:
                    os.remove(nested)
                continue
            if not name.lower().endswith(".csv"):
                log(f"  skipping non-CSV member {inner}")
                continue

            ym = year_month_from_path(inner)
            part = f"year={ym[0]}/month={ym[1]:02d}" if ym else "unknown_period"
            out_dir = os.path.join(raw_dir, part)
            os.makedirs(out_dir, exist_ok=True)
            out_name = re.sub(r"[^A-Za-z0-9._-]+", "_", os.path.basename(name)) + ".gz"
            out_path = os.path.join(out_dir, out_name)

            final_path = out_path
            if os.path.exists(out_path):  # same file name already landed (overlapping archives)
                final_path = out_path[:-3] + f".{uuid.uuid4().hex[:6]}.gz"
            sha, lines, header = stream_csv(zf, info, final_path)

            status = "ok"
            if final_path != out_path:
                existing = [r for r in read_manifest(manifest) if r.get("output_path") == out_path]
                if existing and existing[0].get("content_sha256") == sha:
                    os.remove(final_path)
                    final_path, status = out_path, "duplicate_skipped"
                else:
                    status = "name_collision_kept"
            if not ym:
                status = "period_unknown"

            rec = {
                "record": "file", "run_id": RUN_ID, "ingested_at": now_iso(),
                "source_key": source["key"], "source_url": f"{BUCKET_URL}/{source['key']}",
                "source_size": source["size"], "source_sha256": source.get("sha256"),
                "inner_path": inner, "inner_crc32": f"{info.CRC:08x}",
                "uncompressed_bytes": info.file_size, "content_sha256": sha,
                "line_count": lines, "header": header.decode("utf-8", "replace"),
                "header_sha256": header_fingerprint(header),
                "year": ym[0] if ym else None, "month": ym[1] if ym else None,
                "output_path": final_path, "status": status,
            }
            append_manifest(manifest, rec)
            log(f"  {inner} -> {final_path} ({lines:,} lines, {status})")


# ---------------------------------------------------------------- manifest

def read_manifest(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def append_manifest(path, rec):
    with open(path, "a") as f:
        f.write(json.dumps(rec) + "\n")
        f.flush()
        os.fsync(f.fileno())


def free_bytes(path):
    return shutil.disk_usage(path).free


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, nargs="+", default=DEFAULT_YEARS)
    ap.add_argument("--data-dir", default=os.path.expanduser("~/citibike/data"))
    args = ap.parse_args()

    raw_dir = os.path.join(args.data_dir, "raw", "citibike")
    landing = os.path.join(args.data_dir, "landing")
    manifest = os.path.join(args.data_dir, "manifest", "raw_manifest.jsonl")
    for d in (raw_dir, landing, os.path.dirname(manifest)):
        os.makedirs(d, exist_ok=True)

    log(f"run {RUN_ID}: years={args.years} data_dir={args.data_dir}")
    done = {r["source_key"] for r in read_manifest(manifest) if r.get("record") == "archive_done"}
    objects = list_bucket()
    log(f"bucket lists {len(objects)} objects")

    failures = 0
    for year in args.years:
        archives = select_archives(objects, year)
        if not archives:
            log(f"WARNING: no archives found for {year}")
            failures += 1
            continue
        log(f"{year}: {len(archives)} archive(s), {sum(a['size'] for a in archives) / 1e9:.1f} GB")
        for obj in archives:
            if obj["key"] in done:
                log(f"skip {obj['key']} (already done)")
                continue
            need = obj["size"] * 2 + MIN_FREE_BYTES
            if free_bytes(args.data_dir) < need:
                log(f"STOP: not enough disk for {obj['key']} "
                    f"(need {need / 1e9:.1f} GB, free {free_bytes(args.data_dir) / 1e9:.1f} GB)")
                sys.exit(2)
            dest = os.path.join(landing, os.path.basename(obj["key"]))
            try:
                log(f"downloading {obj['key']} ({obj['size'] / 1e6:,.0f} MB)")
                t0 = time.time()
                obj["sha256"] = download(obj, dest)
                log(f"  downloaded in {time.time() - t0:,.0f}s, extracting")
                process_zip(dest, obj, raw_dir, landing, manifest)
                append_manifest(manifest, {
                    "record": "archive_done", "run_id": RUN_ID, "finished_at": now_iso(),
                    "source_key": obj["key"], "source_size": obj["size"],
                    "source_sha256": obj["sha256"], "source_etag": obj["etag"],
                })
                os.remove(dest)
                log(f"done {obj['key']}; free disk {free_bytes(args.data_dir) / 1e9:.1f} GB")
            except Exception as e:
                failures += 1
                log(f"ERROR on {obj['key']}: {e!r} -- continuing with next archive")
                # leave .part (resumable); remove a fully downloaded but unprocessed archive
                if os.path.exists(dest):
                    os.remove(dest)

    log(f"finished run {RUN_ID} with {failures} failure(s)")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
