from citibike import silver, stations

RULES = silver.load_rules()
LEGACY_COLS = ("startstationid startstationname startstationlatitude startstationlongitude "
               "endstationid endstationname endstationlatitude endstationlongitude").split()


def legacy(spark, rows):
    cols = LEGACY_COLS + ["year", "month"]
    return spark.createDataFrame(rows, ", ".join(f"{c} {'int' if c in ('year', 'month') else 'string'}" for c in cols))


def obs(spark, rows):
    df = stations.observations(legacy(spark, rows), "legacy_v1", RULES)
    return {(r.station_id, r.station_name): r for r in df.collect()}


def test_start_and_end_occurrences_are_combined_and_ids_cleaned(spark):
    o = obs(spark, [("3160.0", "Central Park West & W 76 St", "40.7789", "-73.9737", "519", "Pershing Square North", "40.7518", "-73.9777", 2018, 1),
                    ("3160", "Central Park West & W 76 St", "40.7791", "-73.9739", "3160.0", "Central Park West & W 76 St", "40.7790", "-73.9738", 2019, 6)])
    r = o[("3160", "Central Park West & W 76 St")]
    assert (r.n, r.n_coords, r.first_ym, r.last_ym) == (3, 3, 201801, 201906)
    assert abs(r.lat - 40.7790) < 0.001 and abs(r.lng - -73.9738) < 0.001  # median of the three positions
    assert o[("519", "Pershing Square North")].n == 1


def test_service_points_and_empty_ids_are_left_out(spark):
    o = obs(spark, [("3432", "NYCBS Depot - GOW", "40.7", "-74.0", "", "", "", "", 2018, 1)])
    assert o == {}


def test_bad_coordinates_do_not_move_the_station_but_still_count(spark):
    o = obs(spark, [("3480", "Odd Station", "0.0", "0.0", "3480", "Odd Station", "40.70", "-73.99", 2017, 3)])
    r = o[("3480", "Odd Station")]
    assert (r.n, r.n_coords) == (2, 1) and abs(r.lat - 40.70) < 1e-6


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
