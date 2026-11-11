# Performance experiments (measured, with Spark event logs)

Event logs: `~/citibike/spark-events/`. Hardware: two nodes, 8 cores each (spark-vm 8 GB executor, calc 12 GB); data on spark-vm,
shared with calc over NFS. All times wall clock, one run each unless stated.

## 1. One node vs two nodes (Bronze, 161 months, 333.9 M rows)

| | 8 cores, local | 16 cores, cluster |
|---|---|---|
| Time | 8 min 39 s | 4 min 57 s (1.75x) |
| Tasks per node | 515 | 257 + 258 |
| Median heavy task | | 11.4 s (spark-vm), 11.1 s (calc) |

Hypothesis "calc is slower because it reads over NFS": **refuted** (11.1 s vs 11.4 s). The job is CPU-bound (gunzip, CSV parse,
zstd write); about 33 s are fixed overhead in both runs, and task time is 5 % higher on the cluster.
Caveat: CI ran on spark-vm during the cluster run.

## 2. Collecting station observations (667 M start/end occurrences)

| Version | Time | Note |
|---|---|---|
| A: regex on every row, `percentile_approx` per (id, name) | 14 min 33 s | baseline |
| B: group raw values into 10 m position cells first, regex and medians on the small table | 13 min 48 s (scan 806 s) | only 8 % better: **hypothesis refuted** |
| C: B plus compute the grouped table once (`localCheckpoint`) | **2 min 14 s** (scan 114 s) | 6.5x faster than A, identical result |

What the Spark UI (event log of run B) showed: the three largest stages, 20 / 21 / 22, each had 164 tasks, read 2.3 GB and cost
about 3,000 task-seconds. The grouped table feeds three aggregations (counts, latitude median, longitude median), and without
`localCheckpoint` Spark recomputed the whole Bronze scan for each. The grouped result is small (70 MB of shuffle), so computing it
once removed two of the three scans. Lesson: the expensive part was not the work per row but how often the scan ran.

## 3. Silver: the sort before every partitioned write (2019 + 2024, 64.8 M rows, cluster)

**Evidence that led here.** Event log of the full Silver run (`app-20261106224338-0014`, 19 min 17 s): the staging write
(705 s) and the publish of `silver/trips` (214 s) together spilled **83 GB** to disk although neither has a shuffle. The physical
plans show why: Spark 3.4+ plans a `Sort` by the partition columns (year, month, status) before `WriteFiles`, and the sort spills
full-width rows. Each input task holds only one month, so the sort buys almost nothing.

**Change.** `spark.sql.optimizer.plannedWrite.enabled=false` + `spark.sql.maxConcurrentOutputFileWriters=8`: no sort, each task keeps
up to 8 partition files open (falls back to sorting only if it needs more). Set in `[spark.conf]` of the env config.

**Setup.** Separate lake `~/citibike/lake/exp` (Bronze and stations symlinked from prod, read only), configs `config/exp_base.toml` /
`exp_nosort.toml`, script `scripts/exp_silver_write.sh`; runs alternated base / nosort / base / nosort, outputs deleted and page
caches dropped on both nodes before every run.

| | base (n=2) | no sort (n=2) |
|---|---|---|
| Whole run | 295 s, 264 s (mean 280) | 256 s, 249 s (mean 253, **-10 %**) |
| Staging write job (transform + write) | 142 s, 137 s | 138 s, 136 s (no change) |
| Publish `trips` job | 65 s, 49 s | 37 s, 35 s (**-36 %**) |
| Disk spill | 14.2 GB, 13.9 GB | **0** |
| Output | 2.6 GB | 2.6 GB, same row counts, similar file count (75 vs 69 in prod for these months) |

Event logs: `app-20261109163724-0021`, `-0022`, `-0023`, `-0024`.

**What did not work as expected.** The hypothesis was that the sort also dominated the 137 s staging job. It does not: that job is
CPU-bound (1,400 s CPU of 1,600 s task time) by parsing, rules and the station joins, and removing the sort left it unchanged. The win is
in the publish job (a plain read + write, where the sort was a big share) and in 14 GB less temporary disk I/O on a VM with 29 GB free.
Extrapolated to the full run (83 GB spill) this is worth more, but the full run was not repeated (n=0 at full scale; stated as such).

## 4. Gold: thousands of small files in `gold_demand_hourly`

**Evidence.** The published prod table had **10,304 files for 598 MB**, half of them under 100 KB. Cause: the aggregation shuffles into
64 partitions and every one of them writes a file into every year/month folder it holds (64 x 161 months).

**Change.** `repartition(year, month)` before every partitioned Gold write (one file per month). Script `scripts/exp_gold_files.sh`:
Gold build on the exp lake (2019 + 2024, 64.8 M trips), then a typical read (`trips` per hour over the whole table), caches dropped.

| | base (n=2) | repartition (n=2) |
|---|---|---|
| Files in `gold_demand_hourly` | 475 | **25** |
| Size | 82 MB | 67 MB (-18 %, better compression in larger files) |
| Gold build (wall) | 45 s, 43 s | 37 s, 38 s (**-15 %**) |
| Read query, Spark job time | 6.7 s, 7.2 s | 6.1 s, 5.5 s (-17 %) |

Event logs: build `app-20261109165606-0025`, `-0027` (base), `-0029`, `-0031` (repartition); reads `-0026`, `-0028`, `-0030`, `-0032`.
The extra shuffle costs less than writing and later listing hundreds of tiny files. At full scale (10,304 files) the effect on readers
should be larger; it will show in the next prod Gold run.

**Confirmed at full scale** (prod-pipeline run 42721947270, 9 Nov 2026): `gold_demand_hourly` now has **161 files / 356 MB**
(was 10,304 files / 598 MB), and the `gold.tables` step took 80.5 s (was 111 s). n=1 each.

## 5. Jobs not analysed yet

| Job | Time |
|---|---|
| Bronze profile (`profile_bronze.py`): explodes about 20 structs per row | 7 min 40 s |
| Silver rules report (`silver_rules_report.py`) | 10 min 22 s |
| Silver run, full history (`run_silver.py`) | 20 min 30 s (see §3) |
| Gold build and publish | 2 min 37 s |

Remaining candidates: repeated scans in the Silver run (the standardised frame is used for the main path and the duplicate rows),
`shuffle_partitions`, AQE on/off, broadcast vs sort-merge for the station join (the map is
4,205 rows and is already broadcast).
