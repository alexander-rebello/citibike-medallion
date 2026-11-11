# Report outline (8-10 pages) and evidence map

Rule: decisions, evidence, results, limits, reflection. Not a chronology, not a code dump. Silver gets the most space.
Page budget (target 9.5): Question 0.5 | Data 1 | Architecture 1 | Bronze 0.75 | **Silver 3** | Gold 0.75 | Performance 1.25 |
Operations 0.75 | Results 0.75 | Limits and reflection 0.5.

## 1. Question and consumer (0.5)
Did the pandemic and e-bikes change how New Yorkers use Citi Bike? Core years 2015 / 2019 / 2024, full history 2013-06..2026-10
as context. Consumer: city planners / Citi Bike. Four measures: casual share, daily peaks, e-bike vs classic trip length,
busiest stations moving out of central Manhattan.

## 2. Understanding the raw data (1)
- 161 months, 333,888,858 rows, two logical schemas, four header spellings, Jersey City files out of scope.
- 19 months present twice (yearly zip contains whole + split file sets); proven equal row counts, one set kept.
- **Our own mistake, kept in the report:** a Jan/Jun sample suggested one timestamp format per year; the full profile shows
  8 format runs by month (2015 mixes with and without seconds). Lesson: measure, then write rules.
- Evidence: docs/bronze_profile.md. Figure: timeline of the 8 timestamp format runs.

## 3. Architecture and environments (1)
Medallion on files (Parquet, zstd, partitioned year/month), dev (6 months covering every variant) and prod (all months),
two-node Spark standalone cluster (NFS shared lake, systemd services), GitHub Actions on a self-hosted runner,
feature branch -> PR -> CI -> merge. Figure: architecture diagram (raw -> Bronze -> Silver -> Gold -> dashboard; git/CI lane).

## 4. Bronze (0.75)
All-string schema, registry of header fingerprints (unknown header stops the run), manifest reconciliation per month,
ledger so Silver reads only reconciled months, idempotent re-runs. Evidence: prod run 161 months, 0 refused, 0 failed,
every row count exact.

## 5. Silver (3) - the main section
Table "problem -> fix -> evidence", one row per rule: timestamps per month run | station id styles (`3160.0`, `5303.06_`) |
`\N` birth years | member/casual mapping | synthetic trip id | service stations (reject), valet and Jersey City / Hoboken
(keep and flag) | duration rules | coordinates | daylight-saving repair | duplicates | station dimension and matching.
Must show: row accounting `bronze = accepted + rejected + duplicates removed`; per-rule impact per era
(docs/silver_rules_impact.md); quality table per month; hand-checked station-match sample; tests per era.
Highlights so far: 99.91 % accepted (299,565 rejected); every timestamp parses; the `< 60 s` rule barely bites (579 rows,
the source pre-filters); 2,431 trips repaired for daylight saving (1,760 modern + 671 legacy), 0 non-positive durations
left, one limitation stated. Figures: rule-impact table; before/after of one repaired trip.

## 6. Gold (0.75)
gold_riders_daily, gold_demand_hourly (+ station-year ranking), totals checked against accepted Silver, staged publish with
a _READY marker. Evidence: failure demo (check fails -> previous publication stays).

## 7. Performance with evidence (1.25)
- Bronze baseline vs cluster: 8 min 39 s (8 cores) vs 4 min 57 s (16 cores) = 1.75x; work split 257/258 tasks; calc not
  slower (NFS hypothesis disproved); CPU-bound; ~33 s fixed overhead; n = 1 each.
- Slow jobs worth examining: profile 7 min 40 s and Silver rules report 10 min 22 s (both heavier than Bronze). Candidate
  rewrites: conditional counts instead of exploding ~20 structs per row; avoid repeated regex on every row.
- To run with Spark UI evidence: explicit schema vs inference, broadcast vs sort-merge station join, file sizing, AQE on/off,
  shuffle partitions in Silver. Report what did not help too.

## 8. Operations (0.75)
Workflows (CI, prod with manual approval, monthly schedule picking up the newest month), run log, failure demo (unknown
header -> Bronze stops, Gold not republished), cluster as services (`infra/spark/cluster.sh`).

## 9. Results (0.75)
Answer the question with Gold numbers. Known from Bronze: e-bike share 14 % (2020) -> 66 % (2024) -> 73 % (2026).
Still needed from Gold: casual share by year, daily/hourly peaks, e-bike vs classic duration, station ranking outside Midtown.

## 10. Limitations and reflection (0.5)
Legacy bike type assumed classic; legacy vs modern rider definitions differ; undetectable daylight-saving cases; one run per
benchmark; NFS single point of failure; what we would do with more time.

## Evidence map
| Claim | Source |
|---|---|
| format runs, id styles, birth-year marker, station classes | docs/bronze_profile.md, lake/prod/_meta/profile/ |
| rule impact, 99.91 %, DST repair | docs/silver_rules_impact.md, lake/prod/_meta/silver_rules/ |
| 1.75x speed-up | event logs local-1793959854349 (baseline), app-20261106103028-0000 (cluster); ~/citibike/run-logs/bronze_prod_*.log |
| Bronze reconciliation | lake/prod/_meta/bronze_runs.jsonl, bronze_ledger.json |
| CI and review history | PRs #1-#6 |
