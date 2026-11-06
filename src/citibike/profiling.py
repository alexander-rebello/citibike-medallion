"""Bronze profiling: measure what the raw values look like, per month, before Silver rules are written.

Every value is classified by *shape* (which timestamp pattern, what kind of station id, is a duration plausible,
are coordinates inside NYC) and counted per year/month. Only DataFrame functions, no UDFs. Results are small
tables, so they can be inspected, kept as evidence and used to justify Silver rules.
"""
from pyspark.sql import Column, DataFrame, functions as F

TS_RULES = [
    ("iso_s", r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"),
    ("iso_frac3", r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$"),
    ("iso_frac4", r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{4}$"),
    ("iso_frac_other", r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+$"),
    ("us_s", r"^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2}$"),
    ("us_m", r"^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}$"),
]
ID_RULES = [
    ("int", r"^\d+$"),
    ("dot0", r"^\d+\.0+$"),          # 3160.0
    ("dotted", r"^\d+\.\d*[1-9]\d*$"),  # 6584.12
]
NYC_LAT = (40.4, 41.0)
NYC_LNG = (-74.3, -73.6)


def _c(col) -> Column:
    return F.col(col) if isinstance(col, str) else col


def _blank(c: Column) -> Column:
    return c.isNull() | (F.trim(c) == "")


def shape(col, rules) -> Column:
    """First matching rule name; 'empty' for null/blank; 'other' for no match."""
    c = _c(col)
    expr = F.when(_blank(c), F.lit("empty"))
    for name, rx in rules:
        expr = expr.when(c.rlike(rx), F.lit(name))
    return expr.otherwise(F.lit("other"))


def presence(col) -> Column:
    c = _c(col)
    return F.when(_blank(c), F.lit("empty")).otherwise(F.lit("present"))


def bucket(col, rules) -> Column:
    """Numeric classes: empty / non_numeric, then the first rule (label, predicate on the double value) that holds."""
    c = _c(col)
    v = c.cast("double")
    expr = F.when(_blank(c), F.lit("empty")).when(v.isNull() | F.isnan(v), F.lit("non_numeric"))
    for name, pred in rules:
        expr = expr.when(pred(v), F.lit(name))
    return expr.otherwise(F.lit("other"))


def duration_class(col) -> Column:
    return bucket(col, [("<=0", lambda v: v <= 0), ("<60s", lambda v: v < 60),
                        ("ok", lambda v: v <= 86400), (">24h", lambda v: v > 86400)])


def birth_year_class(col) -> Column:
    return bucket(col, [("<1900", lambda v: v < 1900), ("1900-1919", lambda v: v < 1920),
                        ("1920-2010", lambda v: v <= 2010), (">2010", lambda v: v > 2010)])


def coord_class(col, lo: float, hi: float) -> Column:
    """'in_nyc' inside [lo, hi]; 'other' = outside the box (swapped, wrong-hemisphere or far-away points)."""
    return bucket(col, [("zero", lambda v: v == 0), ("in_nyc", lambda v: (v >= lo) & (v <= hi))])


def _raw(col) -> Column:
    c = _c(col)
    return F.when(_blank(c), F.lit("<empty>")).otherwise(c)


SPECS = {
    "legacy_v1": {
        "ts": ["starttime", "stoptime"],
        "ids": ["startstationid", "endstationid"],
        "names": ["startstationname", "endstationname"],
        "cats": ["usertype", "gender"],
        "lat": ["startstationlatitude", "endstationlatitude"],
        "lng": ["startstationlongitude", "endstationlongitude"],
        "duration": ["tripduration"],
        "birth": ["birthyear"],
        "other_presence": ["bikeid"],
        "distinct": ["startstationid", "startstationname", "endstationid", "endstationname", "bikeid"],
    },
    "modern_v2": {
        "ts": ["startedat", "endedat"],
        "ids": ["startstationid", "endstationid"],
        "names": ["startstationname", "endstationname"],
        "cats": ["rideabletype", "membercasual"],
        "lat": ["startlat", "endlat"],
        "lng": ["startlng", "endlng"],
        "duration": [],
        "birth": [],
        "other_presence": ["rideid"],
        "distinct": ["startstationid", "startstationname", "endstationid", "endstationname"],
    },
}


def value_exprs(spec: dict) -> dict[str, Column]:
    """column name -> classification expression (what to count)."""
    ex: dict[str, Column] = {}
    ex.update({c: shape(c, TS_RULES) for c in spec["ts"]})
    ex.update({c: shape(c, ID_RULES) for c in spec["ids"]})
    ex.update({c: presence(c) for c in spec["names"] + spec["other_presence"]})
    ex.update({c: _raw(c) for c in spec["cats"]})
    ex.update({c: duration_class(c) for c in spec["duration"]})
    ex.update({c: birth_year_class(c) for c in spec["birth"]})
    ex.update({c: coord_class(c, *NYC_LAT) for c in spec["lat"]})
    ex.update({c: coord_class(c, *NYC_LNG) for c in spec["lng"]})
    ex["_corrupt_record"] = presence("_corrupt_record")
    return ex


def profile_values(df: DataFrame, schema: str) -> DataFrame:
    """(schema, year, month, column, value, n): how many rows have each shape/class, per column and month."""
    exprs = value_exprs(SPECS[schema])
    pairs = [F.struct(F.lit(c).alias("column"), e.alias("value")) for c, e in exprs.items()]
    long = df.select("year", "month", F.explode(F.array(*pairs)).alias("kv")).select("year", "month", "kv.*")
    return (long.groupBy("year", "month", "column", "value").count().withColumnRenamed("count", "n")
            .withColumn("schema", F.lit(schema)))


def profile_distinct(df: DataFrame, schema: str) -> DataFrame:
    """Approximate distinct counts per year (station ids/names, bikes): how much does identity drift?"""
    aggs = [F.approx_count_distinct(c).alias(c) for c in SPECS[schema]["distinct"]]
    return df.groupBy("year").agg(F.count(F.lit(1)).alias("rows"), *aggs).withColumn("schema", F.lit(schema))
