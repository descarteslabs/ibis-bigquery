import ibis.expr.datatypes as dt

# import ibis_bigquery.datatypes
import pytest


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        ("ARRAY<DOUBLE>", dt.Array(dt.double)),
        ("array<array<string>>", dt.Array(dt.Array(dt.string))),
        ("map<string, double>", dt.Map(dt.string, dt.double)),
        (
            "map<int64, array<map<string, int8>>>",
            dt.Map(dt.int64, dt.Array(dt.Map(dt.string, dt.int8))),
        ),
        ("set<uint8>", dt.Set(dt.uint8)),
        ([dt.uint8], dt.Array(dt.uint8)),
        ([dt.float32, dt.float64], dt.Array(dt.float64)),
        ({dt.string}, dt.Set(dt.string)),
        ("geography", dt.Geography()),
        ("geography;4326", dt.Geography(srid=4326)),
        ("geography;2000", dt.Geography(srid=2000)),
        ("geometry", dt.Geometry()),
        ("point", dt.point),
        ("point;4326", dt.point),
        ("point;4326:geometry", dt.point),
        ("point;4326:geography", dt.point),
        ("linestring", dt.linestring),
        ("linestring;4326", dt.linestring),
        ("linestring;4326:geometry", dt.linestring),
        ("linestring;4326:geography", dt.linestring),
        ("polygon", dt.polygon),
        ("polygon;4326", dt.polygon),
        ("polygon;4326:geometry", dt.polygon),
        ("polygon;4326:geography", dt.polygon),
        ("multilinestring", dt.multilinestring),
        ("multilinestring;4326", dt.multilinestring),
        ("multilinestring;4326:geometry", dt.multilinestring),
        ("multilinestring;4326:geography", dt.multilinestring),
        ("multipoint", dt.multipoint),
        ("multipoint;4326", dt.multipoint),
        ("multipoint;4326:geometry", dt.multipoint),
        ("multipoint;4326:geography", dt.multipoint),
        ("multipolygon", dt.multipolygon),
        ("multipolygon;4326", dt.multipolygon),
        ("multipolygon;4326:geometry", dt.multipolygon),
        ("multipolygon;4326:geography", dt.multipolygon),
    ],
)
def test_dtype(spec, expected):
    assert dt.dtype(spec) == expected
