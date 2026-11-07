from citibike import silver, stations

RULES = silver.load_rules()
LEGACY_COLS = ("startstationid startstationname startstationlatitude startstationlongitude "
               "endstationid endstationname endstationlatitude endstationlongitude").split()


def legacy(spark, rows):
    cols = LEGACY_COLS + ["year", "month"]
    return spark.createDataFrame(rows, ", ".join(f"{c} {'int' if c in ('year', 'month') else 'string'}" for c in cols))


def obs(spark, rows):
    df = stations.merge_months(stations.monthly_observations(legacy(spark, rows), "legacy_v1", RULES))
    return {(r.station_id, r.station_name): r for r in df.collect()}


def test_start_and_end_occurrences_are_combined_and_ids_cleaned(spark):
    o = obs(spark, [("3160.0", "Central Park West & W 76 St", "40.7789", "-73.9737", "519", "Pershing Square North", "40.7518", "-73.9777", 2018, 1),
                    ("3160", "Central Park West & W 76 St", "40.7791", "-73.9739", "3160.0", "Central Park West & W 76 St", "40.7790", "-73.9738", 2019, 6)])
    r = o[("3160", "Central Park West & W 76 St")]
    assert (r.n, r.n_coords, r.first_ym, r.last_ym) == (3, 3, 201801, 201906)
    assert abs(r.lat - 40.7790) < 0.001 and abs(r.lng - -73.9738) < 0.001  # median position, to within a cell
    assert o[("519", "Pershing Square North")].n == 1


def test_service_points_and_empty_ids_are_left_out(spark):
    o = obs(spark, [("3432", "NYCBS Depot - GOW", "40.7", "-74.0", "", "", "", "", 2018, 1)])
    assert o == {}


def test_bad_coordinates_do_not_move_the_station_but_still_count(spark):
    o = obs(spark, [("3480", "Odd Station", "0.0", "0.0", "3480", "Odd Station", "40.70", "-73.99", 2017, 3)])
    r = o[("3480", "Odd Station")]
    assert (r.n, r.n_coords) == (2, 1) and abs(r.lat - 40.70) < 1e-3


def test_same_id_with_two_names_stays_two_observations(spark):
    o = obs(spark, [("100", "Old Name", "40.71", "-74.00", "100", "New Name", "40.71", "-74.00", 2016, 5)])
    assert set(o) == {("100", "Old Name"), ("100", "New Name")}


# ---------------------------------------------------------------- identity: which records are the same station

OBS_COLS = "schema string, station_id string, station_name string, n long, n_coords long, lat double, lng double, first_ym int, last_ym int"


def build(spark, rows):
    df = spark.createDataFrame(rows, OBS_COLS)
    out = stations.build(df, RULES)
    keys = {(r.schema, r.station_id, r.station_name): r.station_key for r in out["station_map"].collect()}
    return keys, out


def L(i, name, lat, lng, n=100, first=201306, last=201912):
    return ("legacy_v1", i, name, n, n, lat, lng, first, last)


def M(i, name, lat, lng, n=100, first=202001, last=202610):
    return ("modern_v2", i, name, n, n, lat, lng, first, last)


def test_same_name_in_both_eras_at_the_same_place_is_one_station(spark):
    keys, out = build(spark, [L("435", "W 21 St & 6 Ave", 40.7417, -73.9941), M("6140.05", "W 21 St & 6 Ave", 40.7417, -73.9941)])
    assert keys[("legacy_v1", "435", "W 21 St & 6 Ave")] == keys[("modern_v2", "6140.05", "W 21 St & 6 Ave")]
    (s,) = out["stations"].collect()
    assert s.era == "both" and s.legacy_ids == ["435"] and s.modern_ids == ["6140.05"] and not s.matched_by_position


def test_rename_with_the_same_id_stays_one_station_chain_links_all_records(spark):
    keys, _ = build(spark, [L("100", "Old Name", 40.7100, -74.0000), L("100", "New Name", 40.7100, -74.0001),
                            M("7000.01", "New Name", 40.7100, -74.0001)])
    assert len(set(keys.values())) == 1  # old name -(same id)-> new name -(same name)-> modern id


def test_id_reused_for_a_place_far_away_is_not_merged_and_is_reported(spark):
    keys, out = build(spark, [L("200", "Place A", 40.70, -74.00), L("200", "Place B", 40.80, -73.95)])
    assert len(set(keys.values())) == 2
    assert out["conflicts"].count() == 1 and all(r.has_conflict for r in out["stations"].collect())


def test_name_reused_far_away_is_not_merged(spark):
    keys, _ = build(spark, [L("1", "Main St", 40.70, -74.00), M("5000.01", "Main St", 40.85, -73.90)])
    assert len(set(keys.values())) == 2


def test_missing_coordinates_cannot_contradict_a_name_match(spark):
    keys, _ = build(spark, [("legacy_v1", "1", "No Coords St", 5, 0, None, None, 201306, 201306),
                            M("5000.01", "No Coords St", 40.70, -74.00)])
    assert len(set(keys.values())) == 1


def test_unmatched_by_name_but_unique_and_close_is_matched_by_position(spark):
    keys, out = build(spark, [L("300", "Old Street Name", 40.7000, -74.0000), M("6000.01", "New Street Name", 40.70020, -74.0000)])
    assert len(set(keys.values())) == 1
    (s,) = out["stations"].collect()
    assert s.matched_by_position and s.era == "both"


def test_two_close_candidates_are_ambiguous_and_stay_unmatched(spark):
    keys, out = build(spark, [L("300", "Old Street Name", 40.7000, -74.0000), M("6000.01", "Alpha", 40.70010, -74.0000),
                              M("6000.02", "Beta", 40.70020, -74.0000)])
    assert len(set(keys.values())) == 3 and out["coord_ambiguous"].count() == 2


def test_far_apart_legacy_only_station_stays_legacy_only(spark):
    _, out = build(spark, [L("400", "Closed St", 40.70, -74.00, last=201810), M("6000.01", "Other St", 40.7020, -74.0000)])
    assert sorted(r.era for r in out["stations"].collect()) == ["legacy_only", "modern_only"]


def test_valet_and_new_jersey_flags_on_the_station(spark):
    _, out = build(spark, [L("3230", "Penn Station Valet", 40.75, -73.99), M("JC072", "Morris Canal", 40.7121, -74.0382)])
    flags = {r.station_name: (r.is_valet, r.outside_nyc) for r in out["stations"].collect()}
    assert flags == {"Penn Station Valet": (True, False), "Morris Canal": (False, True)}


def test_months_are_merged_with_a_weighted_median_and_total_counts(spark):
    rows = [("100", "A", "40.7000", "-74.0000", "100", "A", "40.7000", "-74.0000", 2016, 1)] * 3 \
        + [("100", "A", "40.7500", "-74.0500", "100", "A", "40.7500", "-74.0500", 2016, 2)]
    df = stations.monthly_observations(legacy(spark, rows), "legacy_v1", RULES)
    monthly = {(r.year, r.month): r for r in df.collect()}
    assert monthly[(2016, 1)].n == 6 and monthly[(2016, 2)].n == 2
    (r,) = stations.merge_months(df).collect()
    assert (r.n, r.first_ym, r.last_ym) == (8, 201601, 201602) and abs(r.lat - 40.70) < 1e-3  # the busier month decides


def test_weighted_median_picks_the_value_where_half_the_weight_is_reached(spark):
    df = spark.createDataFrame([("k", 1.0, 1), ("k", 2.0, 1), ("k", 3.0, 5), ("j", 7.0, 2)], "g string, v double, w long")
    out = {r.g: r.m for r in stations.weighted_median(df, ["g"], "v", "w", "m").collect()}
    assert out == {"k": 3.0, "j": 7.0}


# ---------------------------------------------------------------- incremental update on a tiny lake

def test_update_scans_only_new_months_and_keeps_existing_keys(spark, tmp_path):
    from citibike import bronze
    from test_silver_run import LEG_ROW, write_bronze

    lake = str(tmp_path)
    cfg = {"env": "test", "paths": {"lake_root": lake}, "scope": {"months": "all"}}
    row = dict(LEG_ROW, startstationid="100", startstationname="S1", endstationid="200", endstationname="S2")
    bronze.write_ledger(lake, write_bronze(spark, lake, "legacy_v1", {"2015-01": [row, row]}))
    first = stations.update(spark, cfg, RULES)
    assert first["scanned_months"] == ["2015-01"] and first["stations"] == 2 and first["new_records"] == 2

    again = stations.update(spark, cfg, RULES)                      # nothing new: no scan, same result
    assert again["scanned_months"] == [] and again["stations"] == 2 and again["key_changes"] == 0

    new_row = dict(row, startstationid="300", startstationname="S3", startstationlatitude="40.80", startstationlongitude="-73.95")
    ledger = bronze.read_ledger(lake)
    ledger.update(write_bronze(spark, lake, "legacy_v1", {"2015-02": [new_row]}))
    bronze.write_ledger(lake, ledger)
    third = stations.update(spark, cfg, RULES)
    assert third["scanned_months"] == ["2015-02"] and third["stations"] == 3
    assert third["new_records"] == 1 and third["key_changes"] == 0   # the new month adds a station, the old keys stay
    keys = {r.station_name: r.station_key for r in spark.read.parquet(f"{lake}/silver/station_map").collect()}
    assert len(set(keys.values())) == 3


def test_valet_point_next_to_a_regular_station_is_never_merged_with_it(spark):
    keys, out = build(spark, [L("3439", "Broadway & E 22 St - Valet Scan", 40.7400, -73.9900),
                              M("6098.10", "Broadway & E 21 St", 40.7400, -73.9900)])
    assert len(set(keys.values())) == 2                      # same position, but one is a valet record
    flags = {r.station_name: r.is_valet for r in out["stations"].collect()}
    assert flags == {"Broadway & E 22 St - Valet Scan": True, "Broadway & E 21 St": False}   # the regular station is not flagged
