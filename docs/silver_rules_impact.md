# Silver step 1: what each rule does to the real data

`scripts/silver_rules_report.py` over all 161 prod months (cluster, 10 min 22 s; nothing written to Silver yet).
Per-month results: `lake/prod/_meta/silver_rules/{impact,service_matches}.jsonl`.
Every Bronze row is kept, either accepted or with a reject reason: **333,888,858 rows = 333,589,293 accepted
(99.91 %) + 299,565 rejected**. Checked per month: accepted + rows whose first reason is X = rows.

Rows hit by each rule (a row can break several; the first in this column order is its `reject_reason`). Counts are after
the daylight-saving repair, which is why `duration <= 0` is 0:

| Year | Rows | Accepted % | time missing | duration <= 0 | < 60 s | > 24 h | no start station | service station | outside coords | DST repaired |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2013 | 5,614,888 | 99.93 | 0 | 0 | 0 | 634 | 0 | 3,393 | 0 | 143 |
| 2014 | 8,081,216 | 99.99 | 0 | 0 | 0 | 553 | 0 | 0 | 0 | 31 |
| 2015 | 9,937,969 | 99.95 | 0 | 0 | 0 | 2,591 | 0 | 2,354 | 0 | 160 |
| 2016 | 13,845,655 | 99.95 | 0 | 0 | 0 | 3,577 | 0 | 3,625 | 146 | 153 |
| 2017 | 16,364,657 | 99.95 | 0 | 0 | 0 | 4,169 | 0 | 4,351 | 141 | 75 |
| 2018 | 17,548,339 | 99.94 | 0 | 0 | 0 | 4,524 | 2,497 | 4,321 | 82 | 58 |
| 2019 | 20,551,697 | 99.96 | 0 | 0 | 0 | 6,118 | 180 | 1,666 | 0 | 51 |
| 2020 | 19,562,314 | 99.91 | 0 | 0 | 85 | 15,231 | 111 | 3,303 | 4 | 0 |
| 2021 | 27,130,122 | 99.90 | 0 | 0 | 288 | 25,298 | 493 | 1,508 | 0 | 278 |
| 2022 | 29,838,806 | 99.86 | 0 | 0 | 14 | 40,185 | 49 | 2,070 | 1 | 357 |
| 2023 | 35,106,986 | 99.86 | 0 | 0 | 71 | 26,739 | 18,198 | 3,277 | 8 | 324 |
| 2024 | 44,303,209 | 99.89 | 0 | 0 | 111 | 10,969 | 29,835 | 6,764 | 179 | 319 |
| 2025 | 45,771,102 | 99.92 | 0 | 0 | 7 | 9,094 | 20,271 | 8,701 | 59 | 482 |
| 2026 | 40,231,898 | 99.91 | 0 | 0 | 3 | 9,285 | 23,322 | 4,669 | 0 | 0 |
| **All** | **333,888,858** | **99.91** | **0** | **0** | **579** | **158,967** | **94,956** | **50,002** | **620** | **2,431** |

Flags (rows are kept): `is_incomplete` (no end station) 707,235, mostly modern (0.25-0.3 % of rides since 2020);
`is_valet` 133,369 (2015-2017); `outside_nyc` (Jersey City / Hoboken) 13,910 (2020+);
`birth_year_nulled` 41,807 (legacy); `dst_repaired` 2,431;
`duration_mismatch` 13 left unexplained after the repair (legacy: 3 in 2013, 10 in 2014).

## What this shows

1. **Every timestamp parses.** 0 `time missing` in 161 months, so the per-month pattern table in `config/silver_rules.toml` is complete.
2. **`> 24 h` is the biggest rule** (158,967 rows, 0.05 %), present in every era; 2022 is the peak (40,185).
3. **`< 60 s` barely bites** (579 rows): the source already removes very short trips, and legacy has none at all.
4. **Zero / negative durations were a daylight-saving artifact, now repaired.** Before the repair, all 1,760 occurred in November
   2021-2025: local timestamps carry no offset, so a trip ending in the second pass of the repeated 01:00 hour read as ending
   before it started. After the repair 0 remain. Repaired: 1,760 modern trips (end + 1 h) and 671 legacy trips whose timestamps
   disagreed with the recorded `tripduration` by 3600 s (end rebuilt from start + duration). Some legacy repairs fall in October
   (2014: 1, 2015: 9): trips that started before the change and crossed it.
   Limitation: a modern trip that ends in the second pass but still reads as after its start (e.g. 00:50 -> 01:10) cannot be
   detected and keeps a duration 1 h too short.
5. **Service stations:** 55 distinct id/name pairs matched, all depots, workshops, test and demo points. No rider station was hit.
6. **Missing start station** is rare in legacy (2018: 2,497; 2019: 180) and 0.04-0.07 % of modern rides since 2023.
