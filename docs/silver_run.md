# Silver run: design and result on the full history

Code: `src/citibike/silver_run.py`, `scripts/run_silver.py`. Output in `lake/prod/silver/`:
`trips` (accepted, Parquet, partitioned year/month), `rejected` (with all reasons), `duplicates` (what was removed and why),
`quality` (one row per schema and month), plus `_meta/silver_ledger.json` and `_meta/silver_runs.jsonl`.

## How a run works

1. Only months in the Bronze ledger are read. A month is re-processed only if its Bronze data, the previous month's Bronze data
   (cross-month duplicates), the rules file or the station table changed; otherwise it is skipped.
2. Rows are mapped to the common columns and the rules are applied (`silver.py`), then station keys are attached with a
   broadcast join on the 4,205-row station map.
3. **Duplicates.** Keys are found cheaply from key columns only; just the affected rows (a few hundred) are ranked.
   - same trip id in two files: keep the earlier file's copy (`same_month_trip_id`, `cross_month_trip_id`);
   - a new month whose trip id is already in the published previous month: removed (`cross_month_prior`);
   - one bike starting twice at the same moment (legacy only): keep the shorter trip (`same_bike_start`).
4. Everything is written once to `silver/_staging/<run_id>`, checked, and only months that pass are published.
   Checks per month: Bronze rows = accepted + rejected + duplicates removed; every accepted trip has its start station key.
   A failed month is not published and stays out of the ledger; the staging area is kept for inspection.

## Result (prod, 161 months)

| | Rows |
|---|---:|
| Bronze | 333,888,858 |
| accepted (`silver/trips`) | 333,588,780 (99.91 %) |
| rejected | 299,547 |
| duplicates removed | 531 |
| accepted without a start station key | 0 |

Rejected by first reason: `> 24 h` 158,950; no start station 94,360; service station 45,608; `< 60 s` 579; outside coordinates 50.
(Zero `time missing` and zero `duration <= 0`: every timestamp parses, daylight-saving cases are repaired.)

Duplicates: 531 = 512 modern (all trips that started on 30 April 2026 and ended after midnight, present in both the April and
May file; 483 identical, 29 differing only in the last digit of a coordinate) + 19 legacy (1 across months, 5 exact copies in
one month, 13 same bike and start with slightly different stop times; two of them materially different, e.g. 23 min vs 13.4 h).

Run time 20 min 30 s on the 2-node cluster; `silver/trips` is 14 GB. Gold on top of it: 2 min 37 s,
333,588,780 trips, totals match, `_READY` written.
