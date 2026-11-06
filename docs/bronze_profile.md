# Bronze profile (prod, 161 months, 333,888,858 rows)

Produced by `scripts/profile_bronze.py` (`src/citibike/profiling.py`); raw results in
`lake/prod/_meta/profile/{values,distinct}.jsonl`. Every value is classified by shape and counted per month.
Run on the 2-node cluster: 7 min 40 s. 0 corrupt rows in either schema.

## Findings and the Silver rule each one drives

| # | Finding (measured) | Silver rule |
|---|---|---|
| 1 | Timestamp format changes by month, never within a month, 8 runs: 2013-06..2014-08 `yyyy-MM-dd HH:mm:ss`; 2014-09..2014-12 `M/d/yyyy H:mm:ss`; **2015-01..03 and 2015-06 `M/d/yyyy H:mm` (no seconds)**; 2015-04..05 and 2015-07..2016-09 `M/d/yyyy H:mm:ss`; 2016-10..2017-12 `yyyy-MM-dd HH:mm:ss`; 2018-01..2019-12 `...ss.SSSS`; modern `...ss.SSS`. No empty or unmatched timestamp anywhere. | One parse pattern per format run; parse-rate metric per month. Months without seconds take duration from `tripduration`. |
| 2 | Legacy station ids are `int` up to 2017, then 2018: 45.6 % and 2019: 37.8 % written as `3160.0`. Distinct start ids 2019: 1,821 vs 977 names (about 2x). | Strip `\.0+$` before matching; ids stay strings. |
| 3 | Modern ids look like `6584.12`. Some carry a trailing `_` (`5303.06_`), some are service points (`SYS016` Morgan Bike Mechanics, `Shop Morgan`, `SYS038`, `SYS033` Pier 40 X2). | Strip the trailing `_`; flag service stations and exclude them from rider trips (reason in `silver_rejected`). |
| 4 | Legacy and modern use different id systems (`455` vs `6584.12`). | Station table keyed by a surrogate; match by id, then name, then <=50 m. |
| 5 | Legacy `tripduration`: 0 rows under 60 s (source already filtered), 634..6,118 rows/year over 24 h (longest 44 days). | `>24 h` rule matters in all eras; `<60 s` rule only bites in modern data (duration from timestamps). |
| 6 | Birth year: `\N` marker in 2013-14 (1,247,644 rows), empty in 2015-17 (about 13-15 %), complete in 2018-19; 0.01-0.04 % before 1920. | Treat `\N` and empty as null; null birth years outside plausible range. |
| 7 | usertype: `Subscriber` / `Customer`, plus 0.26 % (2016) and 0.10 % (2017) empty. gender: 0/1/2, 0 = unknown. | member / casual; empty -> unknown (not guessed). |
| 8 | Legacy coordinates: all inside NYC except a few hundred rows per year: Montreal "8D" stations (lat 45.5), and (0, 0) depots ("WS Don't Use", "NYCBS Depot"). 2013: 18,059 empty end coordinates = rows with no end station. | Reject outside the NYC box and (0, 0). Flag missing end station. |
| 9 | Modern coordinates are per trip (13+ decimals), not station coordinates. Start coordinates empty in 0.05-0.07 % (2023+), end in 0.24-0.35 %. | Station coordinates = median over trips (`percentile_approx`). |
| 10 | Modern missing start station: 0.00-0.07 % of rows. Missing end station: 0.24-0.35 %. | "No start station" rejection is small; end station is flagged, not rejected. |

## Headline numbers for the question

- e-bike share of trips: 14.1 % (2020), 31.8 %, 39.3 %, 50.1 %, 66.1 % (2024), 70.5 %, 72.7 % (2026).
- casual share: 2013 16.2 %, 2014 9.8 %, 2015 13.2 %, 2019 14.0 % (legacy `Customer`); 2020 23.0 %, 2021 26.8 %, 2024 19.3 %.
  (Different source definitions; compare with care.)
- distinct start stations (by name): 375 (2013), 471 (2015), 977 (2019), 1,257 (2020), 2,211 (2024), 2,472 (2026).
- rows per year: 5.6 M (2013), 9.9 M (2015), 20.6 M (2019), 19.6 M (2020), 44.3 M (2024).

## Station classes (decided 2026-11-06)

Found by listing every station id/name that looks non-standard (ids starting with letters, or names with depot, shop,
valet, test, ...). The coordinate box cannot separate these, because Jersey City and Hoboken lie inside it.

| Class | Examples (trips, years) | Silver |
|---|---|---|
| Service point (depot, workshop, test, mobile unit) | `SYS016` Morgan Bike Mechanics 10,846 (2020-26); `SYS038` Morgan Loading Docks 7,777; `Shop Morgan` 4,936; `SYS035` Pier 40 Dock Station 4,924; `SYS033` Pier 40 X2 3,628; `NYCBS Depot - GOW` 5,720 (2017-19); `NYCBS Depot - SSP` 3,562 (2013-17); `NYCBS Test` 373; `Mobile 01`; `Lab - NYC` | **Reject**, reason `service_station`. Rule: id starts `SYS`, or name matches depot / mechanic / loading dock / shop / lab / test / mobile / "don't use" / QC / WH station. The match list is kept in config and its per-pattern counts go in the quality table. |
| Valet (staffed bike parking) | `Penn Station Valet` 121,554 (2015-17); `PABT Valet` 9,629 (2015-16); `E.T. Bike-In Movie Valet` 481 | **Keep, flag** `is_valet`. Real rider trips; Gold's busiest-station ranking excludes flagged stations, totals include them. |
| Jersey City / Hoboken (`JC*`, `HB*` ids) | `Paulus Hook`, `12 St & Sinatra Dr N`, `14 St Ferry`, ... cross-river trips in the NYC files | **Keep, flag** `outside_nyc` on the station. |
| Montreal "8D" and (0, 0) coordinates | `8D Mobile 01`, `WS Don't Use` | Rejected by the coordinate rule (and `8D`/`Don't use` also by name). |

Trailing `_` on modern ids (`5303.06_`) is stripped before matching.
