# Station dimension

Built and kept up to date by `scripts/update_stations.py` (incremental). Code: `src/citibike/stations.py`.
Output in `lake/prod/silver/`: `stations` (surrogate key, ids per era, names, position, validity), `station_map`
(schema + id + name -> key, used to join trips), `station_conflicts`, `station_coord_ambiguous`.

## What the data showed (before choosing rules)

| Question | Measured |
|---|---|
| Records (id, name pairs) | 4,205 = 1,162 legacy + 3,043 modern; 666,921,077 start/end occurrences |
| Same exact name in both eras: how far apart? | 1,076 of 1,086 pairs within 50 m, 9 at 50-200 m, 1 over 1 km (a reused name). Lower-casing / spacing adds no matches. |
| Same id, different names (renames) | legacy 68 pairs: 46 within 50 m, 12 at 50-200 m, 4 at 200 m-1 km, **6 over 1 km (id reused for another place)**; modern 163 pairs, 162 within 50 m |
| Same name, different ids | legacy 31 pairs (28 within 50 m); modern 203 (195 within 50 m, 1 over 1 km) |
| Legacy ids with no name match in modern | 124: 28 have a modern station within 50 m, 83 only at 50-200 m (mostly closed stations next to different ones) |

## Rules

1. Two records are the same station if they share an id (same schema) or an exact name **and** lie within 50 m
   (`match_radius_m`). A missing position cannot contradict a match.
2. Chains are followed (connected components, min-label propagation), so id and name links combine.
3. Stations that exist in only one era are matched by position (within 50 m) only if the match is mutual and unique;
   ambiguous candidates stay apart (none occurred).
4. A pair that shares an id or name but is farther than 50 m apart is never linked. These 44 pairs (52 m to 10.9 km) are
   listed in `station_conflicts`; all 44 ended up in different stations.
5. Station key = hash of the earliest record's label, so it does not change when later months are added.
6. A valet record (name contains "valet") is never merged with a regular station, even at the same position. Found when a
   rebuilt table merged `Broadway & E 22 St - Valet Scan` into `Broadway & E 21 St`, which would have flagged that busy regular
   station as valet and dropped it from the ranking.

## Result

4,205 records -> **2,834 stations**: 967 in both eras (523 M occurrences, 78 %), 123 legacy-only (mostly closed), 1,744 modern-only.
7 were matched by position only; 58 stations are involved in an id/name conflict; 5 valet stations; 104 in Jersey City / Hoboken.
Accounting: records, map rows and distinct keys agree (4,205 / 4,205 / 2,834); occurrences are identical in the three tables.

## Hand-check of the position-only matches (all 7)

| Legacy | Modern | Verdict |
|---|---|---|
| MacDonough St & Marcy Ave | Marcy Ave & MacDonough St | same (street order swapped) |
| W 13 St & Hudson St | Hudson St & W 13 St | same (street order swapped) |
| Sands St & Navy St | Sands St Gate | plausible rename |
| W 14 St & The High Line | 10 Ave & W 14 St | plausible rename |
| Warren St & Church St | Warren St & W Broadway | same: identical coordinates (40.71474, -74.009106), legacy ends 2018-04, modern starts 2020-10 |
| India St Pier | India St & East River | plausible rename |
| Bike The Branches - Central Branch | Plaza St East & Flatbush Ave | **doubtful**: temporary event station (300 trips, 2016-17), 37 m from a station that opened in 2021. Immaterial (300 of 479,187 trips). |

Random sample of 10 name/id matches in both eras: all correct (same street names, one renamed spelling `2 Ave  & E 104 St`).
Known quirk: station name `85` (a number only) is recorded for legacy id 2021 next to `W 45 St & 8 Ave`.

## Cost

Collecting the observations scans Bronze once and took 14 min 33 s on the cluster, longer than the Bronze load itself
(`percentile_approx` over 667 M rows plus regex on every row). Candidate improvement: pre-aggregate by rounded position
first, then take the median of the small result.


## Incremental update (for the monthly run)

`update_stations.py` scans only Bronze months that are new or changed (ledger digest), stores one small row per station and
month (`silver/station_observations_monthly`, 195,743 rows for the full history), merges the months (weighted median of the
monthly median positions) and rebuilds the station table, which takes seconds. It reports records that are new, records that
are gone and **station keys that changed**; Silver then re-processes only the months whose own stations changed (per-month
station fingerprint), not all months.

Positions are now medians over about 10 m cells instead of over every trip. That moved 5 of 4,205 records across the 50 m
limit compared with the first build (all borderline), which is why the table was rebuilt once and Silver re-run.
Result after adding the valet rule: 2,837 stations (967 in both eras).
