from citibike.sources import select


def rec(inner, lines, source="2013-citibike-tripdata.zip", status="ok", ym=(2013, 9), fp="f42ae6a7191bfe51"):
    out = "/raw/year={}/month={:02d}/{}.gz".format(*ym, inner.rsplit("/", 1)[-1])
    return {"record": "file", "source_key": source, "inner_path": inner, "line_count": lines,
            "status": status, "year": ym[0], "month": ym[1], "output_path": out, "header_sha256": fp}


def test_whole_month_file_duplicating_split_files_is_dropped():
    # real case from 2013-09: one 1,034,359-row file + the same month split at 1M rows
    sel = select([
        rec("2013-citibike-tripdata/201309-citibike-tripdata.csv", 1_034_360),
        rec("2013-citibike-tripdata/9_September/201309-citibike-tripdata_1.csv", 1_000_001),
        rec("2013-citibike-tripdata/9_September/201309-citibike-tripdata_2.csv", 34_360),
    ])["2013-09"]
    assert sel["status"] == "duplicate_sets_dropped"
    assert sel["rows"] == 1_034_359
    assert [p.rsplit("/", 1)[-1] for p in sel["selected"]] == [
        "201309-citibike-tripdata_1.csv.gz", "201309-citibike-tripdata_2.csv.gz"]
    assert sel["dropped"][0]["rows"] == 1_034_359


def test_already_skipped_duplicates_are_ignored():
    # 2018-04: top-level whole + top-level split; the month-folder copies were byte-identical
    sel = select([
        rec("2018-citibike-tripdata/201804-citibike-tripdata.csv", 1_307_544, ym=(2018, 4)),
        rec("2018-citibike-tripdata/201804-citibike-tripdata_1.csv", 1_000_001, ym=(2018, 4)),
        rec("2018-citibike-tripdata/201804-citibike-tripdata_2.csv", 307_544, ym=(2018, 4)),
        rec("2018-citibike-tripdata/4_April/201804-citibike-tripdata_1.csv", 1_000_001,
            ym=(2018, 4), status="duplicate_skipped"),
    ])["2018-04"]
    assert sel["status"] == "duplicate_sets_dropped"
    assert len(sel["selected"]) == 2 and sel["rows"] == 1_307_543


def test_sets_with_different_row_counts_are_a_conflict_not_a_silent_drop():
    sel = select([
        rec("2013-citibike-tripdata/201309-citibike-tripdata.csv", 900_000),
        rec("2013-citibike-tripdata/9_September/201309-citibike-tripdata_1.csv", 1_000_001),
    ])["2013-09"]
    assert sel["status"] == "conflict"
    assert sel["dropped"], "the other set must still be reported"


def test_single_set_month():
    sel = select([
        rec("202406-citibike-tripdata_1.csv", 1_000_001, source="202406-citibike-tripdata.zip", ym=(2024, 6)),
        rec("202406-citibike-tripdata_2.csv", 500_001, source="202406-citibike-tripdata.zip", ym=(2024, 6)),
    ])["2024-06"]
    assert sel["status"] == "single_set" and sel["rows"] == 1_500_000


def test_months_are_independent():
    sel = select([
        rec("2013-citibike-tripdata/7_July/201307-citibike-tripdata_1.csv", 843_417, ym=(2013, 7)),
        rec("2013-citibike-tripdata/8_August/201308-citibike-tripdata_1.csv", 1_000_001, ym=(2013, 8)),
    ])
    assert set(sel) == {"2013-07", "2013-08"}
