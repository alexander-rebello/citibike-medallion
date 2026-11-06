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

## 3. Jobs that are slower than they should be (not yet analysed with their event logs)

| Job | Time |
|---|---|
| Bronze profile (`profile_bronze.py`): explodes about 20 structs per row | 7 min 40 s |
| Silver rules report (`silver_rules_report.py`) | 10 min 22 s |
| Silver run, full history (`run_silver.py`) | 20 min 30 s |
| Gold build and publish | 2 min 37 s |

Next candidates: repeated scans in the Silver run (the standardised frame is used for the main path and the duplicate rows),
`shuffle_partitions`, file sizing of `silver/trips` (14 GB), AQE on/off, broadcast vs sort-merge for the station join (the map is
4,205 rows and is already broadcast).
