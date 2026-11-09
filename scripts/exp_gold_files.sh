#!/usr/bin/env bash
# Performance experiment: Gold build on the exp lake (2019 + 2024 Silver), then a typical read of gold_demand_hourly.
# Usage: scripts/exp_gold_files.sh <label> [repeats]   -> ~/citibike/lake/exp/_meta/exp_gold_runs.tsv
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PY:-/home/ubuntu/spark-env/bin/python}
LAKE=~/citibike/lake/exp
export CITIBIKE_SPARK_MASTER=${CITIBIKE_SPARK_MASTER:-spark://192.168.26.92:7077}
for i in $(seq "${2:-2}"); do
  sync; sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"; ssh -o BatchMode=yes 192.168.26.96 "sync; sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'" || true
  t0=$(date +%s); CITIBIKE_ENV=exp_nosort $PY scripts/run_gold.py > "$LAKE/_meta/exp_gold_$1.$t0.log" 2>&1; t1=$(date +%s)
  app=$(ls -t ~/citibike/spark-events | head -1)
  files=$(find "$LAKE/gold/gold_demand_hourly" -name '*.parquet' | wc -l)
  mb=$(du -sm "$LAKE/gold/gold_demand_hourly" | cut -f1)
  sync; sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"; ssh -o BatchMode=yes 192.168.26.96 "sync; sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'" || true
  r0=$(date +%s.%N)
  CITIBIKE_ENV=exp_nosort $PY - <<'PY' > /dev/null 2>&1
import os, sys
sys.path.insert(0, "src")
from citibike import config
from citibike.spark import get_spark
cfg = config.load(); spark = get_spark(cfg, "gold-read")
h = spark.read.parquet(os.path.expanduser("~/citibike/lake/exp/gold/gold_demand_hourly"))
h.groupBy("start_hour").sum("trips").collect()
spark.stop()
PY
  r1=$(date +%s.%N)
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%.1f\n' "$1" "$t0" "$((t1 - t0))" "$app" "$files" "$mb" "$(echo "$r1 - $r0" | bc)" | tee -a "$LAKE/_meta/exp_gold_runs.tsv"
done
