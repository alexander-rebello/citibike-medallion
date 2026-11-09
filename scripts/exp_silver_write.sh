#!/usr/bin/env bash
# Performance experiment: Silver on 2019 + 2024 (config/exp_*.toml), alternating variants, clean output before each run.
# Usage: scripts/exp_silver_write.sh base nosort base nosort      -> ~/citibike/lake/exp/_meta/exp_runs.tsv
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PY:-/home/ubuntu/spark-env/bin/python}
LAKE=~/citibike/lake/exp
export CITIBIKE_SPARK_MASTER=${CITIBIKE_SPARK_MASTER:-spark://192.168.26.92:7077}
for v in "$@"; do
  rm -rf "$LAKE"/silver/{trips,rejected,duplicates,quality,_staging} "$LAKE"/_meta/silver_ledger.json
  sync; sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"; ssh -o BatchMode=yes 192.168.26.96 "sync; sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'" || true   # cold page cache on both nodes
  t0=$(date +%s)
  CITIBIKE_ENV=exp_$v $PY scripts/run_silver.py > "$LAKE/_meta/exp_$v.$t0.log" 2>&1
  t1=$(date +%s)
  app=$(ls -t ~/citibike/spark-events | head -1)
  printf '%s\t%s\t%s\t%s\n' "$v" "$t0" "$((t1 - t0))" "$app" | tee -a "$LAKE/_meta/exp_runs.tsv"
done
