# %%
import h3
from ibis import selectors as s

import numpy as np
import pandas as pd
import pytest
from shapely.geometry import box

import scalenav.oop as snoo
import scalenav.scale_nav as sn

from tests.conftest import table_from_df
from tests.test_utils import coords_xy, h3_res, x, y

OXFORD_POLY_WKT = (
    "POLYGON((-1.27 51.74, -1.24 51.74, -1.24 51.76, -1.27 51.76, -1.27 51.74))"
)


##############
# %%
@pytest.fixture
def conn():
    return snoo.connect()


# %%


@pytest.fixture
def table1(conn):
    return table_from_df(conn, "table1", coords_xy.copy())


@pytest.fixture
def table1_h3(table1):
    return snoo.project(table1, res=h3_res)


def _expected_h3(res):
    return [
        h3.latlng_to_cell(lng=lon, lat=lat, res=res)
        for (lon, lat) in zip(coords_xy["lon"], coords_xy["lat"])
    ]


def _valid_h3_ids(values, res=None):
    for cell in values:
        assert h3.is_valid_cell(cell)
        if res is not None:
            assert h3.get_resolution(cell) == res


# %%
def test_connect(conn):
    conn.raw_sql("LOAD spatial;")
    conn.raw_sql("LOAD h3;")
    assert isinstance(conn.list_tables(), list)


def test_project(table1, table1_h3):
    expected_ids = _expected_h3(h3_res)
    table1_h3_df = table1_h3.execute()

    assert "h3_id" in table1_h3_df.columns
    assert np.all(table1_h3_df["h3_id"] == expected_ids)
    assert {"lon", "lat"}.issubset(table1_h3_df.columns)

    dropped = snoo.project(table1, res=h3_res, keep=False).execute()
    assert "h3_id" in dropped.columns
    assert "lon" not in dropped.columns
    assert "lat" not in dropped.columns

    renamed = table1.rename(x="lon", y="lat")
    explicit = snoo.project(renamed, res=h3_res, columns=("x", "y")).execute()
    assert np.all(explicit["h3_id"] == expected_ids)

    auto = snoo.project(renamed, res=h3_res).execute()
    assert np.all(auto["h3_id"] == expected_ids)

    by_index = snoo.project(renamed, res=h3_res, columns=(0, 1)).execute()
    assert np.all(by_index["h3_id"] == expected_ids)

    h3_res_reproject = 11
    expected2 = _expected_h3(h3_res_reproject)
    table1_h3_overwrite_df = snoo.project(table1_h3, res=h3_res_reproject).execute()
    assert np.all(table1_h3_overwrite_df["h3_id"] == expected2)

    ambiguous = table1.mutate(lng=table1.lon)
    with pytest.raises(IOError, match="Ambiguous"):
        snoo.coords_columns(ambiguous)
    with pytest.raises(IOError, match="Ambiguous"):
        snoo.project(ambiguous, res=h3_res)

    missing = table1.rename(a="lon", b="lat")
    with pytest.raises(IOError, match="No coordinates"):
        snoo.coords_columns(missing)
    with pytest.raises(IOError, match="No coordinates"):
        snoo.project(missing, res=h3_res)


def test_coords_columns_lonlat(table1):
    assert snoo.coords_columns(table1) == ("lon", "lat")


# %%


def test_change_res(table1_h3):
    orig = table1_h3.execute().set_index("h3_id")
    orig_sum = orig["band_var"].sum()

    table_ds = snoo.change_res(table1_h3, levels=-1)
    table_ds_df = table_ds.execute().set_index("h3_id").sort_index()

    table_res_df = (
        sn.change_res(table1_h3.execute(), level=-1)
        .set_index("h3_id")
        .loc[:, ["band_var"]]
        .sort_index()
    )

    pd.testing.assert_series_equal(
        table_ds_df["band_var"],
        table_res_df["band_var"],
        check_names=False,
    )
    assert table_ds_df["band_var"].sum() == pytest.approx(orig_sum)

    up = snoo.change_res(table1_h3, levels=1).execute()
    assert up["band_var"].sum() == pytest.approx(orig_sum)
    for _, row in up.iterrows():
        parent = h3.cell_to_parent(row["h3_id"], h3_res)
        assert row["band_var"] == pytest.approx(orig.loc[parent, "band_var"] / 7)

    round_trip = snoo.change_res(snoo.change_res(table1_h3, levels=1), levels=-1).execute()
    round_trip = round_trip.set_index("h3_id").sort_index()
    pd.testing.assert_series_equal(
        round_trip["band_var"].astype("float64"),
        orig["band_var"].astype("float64").sort_index(),
        check_names=False,
    )


def test_add_centr(table1_h3):
    table1_centr = snoo.add_centr(table1_h3)
    geom_cols = table1_centr.select(s.of_type("GEOMETRY")).columns
    assert "geom" in geom_cols
    assert table1_centr.count().execute() == table1_h3.count().execute()

    xy = (
        table1_centr.alias("c")
        .sql("SELECT ST_X(geom::GEOMETRY) AS lon, ST_Y(geom::GEOMETRY) AS lat FROM c")
        .execute()
    )
    assert np.isfinite(xy["lon"]).all()
    assert np.isfinite(xy["lat"]).all()


def test_add_geom(table1_h3):
    with_geom = snoo.add_geom(table1_h3)
    assert with_geom.count().execute() == table1_h3.count().execute()
    assert "geometry" in with_geom.select(s.of_type("GEOMETRY")).columns
    types = (
        with_geom.alias("g")
        .sql("SELECT ST_GeometryType(geometry) AS gtype FROM g")
        .execute()["gtype"]
        .str.upper()
    )
    assert types.str.contains("POLYGON").all()


def test_reindex(conn, table1_h3):
    h3_ids = table1_h3.execute()["h3_id"].tolist()
    cell_a, cell_b = h3_ids[0], h3_ids[1]
    cats = pd.DataFrame(
        {
            "h3_id": [cell_a, cell_a, cell_b],
            "cat": ["A", "B", "A"],
            "id": [1, 2, 3],
        }
    )
    src = table_from_df(conn, "reindex_src", cats)
    out = snoo.reindex(
        src, names_from="cat", values_from="id", values_agg="count", values_fill=0
    ).execute()

    out = out.set_index("h3_id")
    assert out.loc[cell_a, "A"] == 1
    assert out.loc[cell_a, "B"] == 1
    assert out.loc[cell_b, "A"] == 1
    assert out.loc[cell_b, "B"] == 0


def test_rescale(conn, table1_h3):
    weight = conn.sql(
        f"""
        SELECT
            1 AS id,
            100.0 AS weight_var,
            ST_GeomFromText('{OXFORD_POLY_WKT}') AS geometry
        """
    )
    rescaled = snoo.rescale(table1_h3, weight=weight)
    dens_cols = [c for c in rescaled.columns if c.endswith("_dens_var")]
    assert dens_cols
    matched = rescaled.filter(rescaled.id.notnull())
    summed = matched.group_by("id").agg(total=getattr(matched, dens_cols[0]).sum()).execute()
    assert summed["total"].iloc[0] == pytest.approx(100.0)


def test_concat(conn, table1_h3):
    df = table1_h3.execute()[["h3_id", "band_var"]]
    a = table_from_df(conn, "concat_a", df.iloc[:2])
    b = table_from_df(conn, "concat_b", df.iloc[2:4])
    empty = table_from_df(conn, "concat_empty", df.iloc[0:0])

    stacked = snoo.concat({"a": a, "b": b, "empty": empty}).execute()
    assert len(stacked) == 4

    other = table_from_df(
        conn, "concat_other", pd.DataFrame({"h3_id": df["h3_id"].iloc[:1], "other_var": [1.0]})
    )
    with pytest.raises(Exception):
        snoo.concat({"a": a, "other": other}).execute()


def test_combine(conn, table1_h3):
    df = table1_h3.execute()[["h3_id", "band_var"]]
    left = table_from_df(conn, "combine_left", df.iloc[:3])
    right = table_from_df(
        conn,
        "combine_right",
        df.iloc[2:5].assign(band_var=df.iloc[2:5]["band_var"] * 2),
    )

    combined = snoo.combine(
        conn,
        {"L": left, "R": right},
        name="combined_layers",
        overwrite=True,
        fill_val=-1.0,
    ).execute()

    assert "h3_id" in combined.columns
    var_cols = [c for c in combined.columns if c.endswith("_var")]
    assert any("L" in c for c in var_cols)
    assert any("R" in c for c in var_cols)

    ids = set(df.iloc[:5]["h3_id"])
    assert set(combined["h3_id"]) == ids

    left_only = df.iloc[0]["h3_id"]
    right_vars = [c for c in var_cols if "R" in c]
    row = combined.set_index("h3_id").loc[left_only]
    assert any(row[c] == -1.0 for c in right_vars)

    again = snoo.combine(
        conn,
        {"L": left, "R": right},
        name="combined_layers",
        overwrite=True,
        fill_val=0.0,
    )
    assert again.count().execute() == len(ids)


@pytest.mark.skip(reason="constrain is not implemented")
def test_constrain():
    raise AssertionError("constrain should not run")


def test_join(conn, table1_h3):
    df = table1_h3.execute()[["h3_id", "band_var"]]
    left = table_from_df(
        conn, "join_left", df.iloc[:4].rename(columns={"band_var": "left_var"})
    )
    overlap = table_from_df(
        conn, "join_overlap", df.iloc[2:6].rename(columns={"band_var": "right_var"})
    )
    disjoint = table_from_df(
        conn,
        "join_disjoint",
        pd.DataFrame(
            {
                "h3_id": [h3.latlng_to_cell(0.0, 0.0, h3_res)],
                "right_var": [99.0],
            }
        ),
    )

    outer = snoo.join(left, overlap, how="outer")
    assert "h3_id_right" not in outer.columns
    assert outer.count().execute() == 6

    inner = snoo.join(left, overlap, how="inner")
    assert inner.count().execute() == 2

    disjoint_outer = snoo.join(left, disjoint, how="outer")
    assert disjoint_outer.count().execute() == 5
    assert "h3_id_right" not in disjoint_outer.columns


def test_table(conn, tmp_path):
    path = tmp_path / "points.parquet"
    coords_xy.to_parquet(path)

    loaded = snoo.table(conn, "parquet_pts", str(path), overwrite=True)
    assert loaded.count().execute() == len(coords_xy)

    bbox_tbl = snoo.table(
        conn,
        "parquet_bbox",
        str(path),
        bbox=[-1.3, 51.74, -1.24, 51.76],
        overwrite=True,
    )
    bbox_n = bbox_tbl.count().execute()
    assert 0 < bbox_n < len(coords_xy)

    replaced = snoo.table(
        conn,
        "parquet_pts",
        str(path),
        bbox=[-1.3, 51.74, -1.24, 51.76],
        overwrite=True,
    )
    assert replaced.count().execute() == bbox_n

    reconnected = snoo.table(
        conn, "parquet_pts", str(path), overwrite=False
    )
    assert reconnected.count().execute() == bbox_n


def test_reduce(conn):
    df = pd.DataFrame(
        {
            "h3_id": ["a", "b"],
            "K_var": [1.0, 2.0],
            "L_var": [3.0, 4.0],
            "M_var": [10.0, 20.0],
        }
    )
    src = table_from_df(conn, "reduce_src", df)
    reduced = snoo.reduce(src, {"KL": ["K_var", "L_var"]}, exclude=True).execute()
    assert "K_var" not in reduced.columns
    assert "L_var" not in reduced.columns
    assert "M_var" in reduced.columns
    assert reduced["KL"].tolist() == [4.0, 6.0]

    kept = snoo.reduce(src, {"KL": ["K_var", "L_var"]}, exclude=False).execute()
    assert {"K_var", "L_var", "KL"}.issubset(kept.columns)


def test_fill_poly(conn):
    poly = conn.sql(
        f"SELECT ST_GeomFromText('{OXFORD_POLY_WKT}') AS geometry"
    )
    filled = snoo.fill_poly(poly, res=8).execute()
    assert len(filled) == 1
    cells = list(filled["h3_id"].iloc[0])
    assert len(cells) > 0
    _valid_h3_ids(cells, res=8)


def test_unnest_h3(conn):
    cells = _expected_h3(h3_res)[:3]
    array_tbl = conn.sql(
        f"SELECT [{', '.join(repr(c) for c in cells)}] AS h3_id"
    )
    unnested = snoo.unnest_h3(array_tbl).execute()
    assert len(unnested) == 3
    assert unnested["band_var"].tolist() == [1, 1, 1]
    assert set(unnested["h3_id"]) == set(cells)

    scalar = table_from_df(
        conn, "unnest_scalar", pd.DataFrame({"h3_id": cells, "band_var": [2, 2, 2]})
    )
    left_alone = snoo.unnest_h3(scalar).execute()
    assert len(left_alone) == 3
    assert left_alone["band_var"].tolist() == [2, 2, 2]


def test_knn(table1_h3):
    k = 1
    neighbors = snoo.knn(table1_h3, k=k)
    col = f"h3_id_{k}nn"
    assert col in neighbors.columns
    exploded = neighbors.unnest(col).execute()
    n_rows = table1_h3.count().execute()
    assert len(exploded) == n_rows * 7
    _valid_h3_ids(exploded[col].tolist())


def test_simplify(conn):
    poly = conn.sql(
        f"SELECT ST_GeomFromText('{OXFORD_POLY_WKT}') AS geometry"
    )
    simplified = snoo.simplify(poly, tolerance=0.01)
    assert simplified.count().execute() == 1
    assert "geometry_simple" in simplified.select(s.of_type("GEOMETRY")).columns
    assert "geometry" in simplified.columns


def test_voronoi(table1_h3):
    seeds = table1_h3.limit(2)
    out = snoo.voronoi(seeds, limit=3)
    df = out.execute()
    assert len(df) >= 1
    assert "h3_id" in df.columns
    assert "h3_gridded" in df.columns
    _valid_h3_ids(df["h3_id"].tolist())
    for cells in df["h3_gridded"]:
        _valid_h3_ids(list(cells))


def test_dump_fill_h3(conn):
    poly = conn.sql(
        f"""
        SELECT
            1 AS id,
            ST_GeomFromText('{OXFORD_POLY_WKT}') AS geometry
        """
    )
    filled = snoo.dump_fill_h3(poly, res=8, keep=False).execute()
    assert len(filled) >= 1
    cells = filled["h3_id"].iloc[0]
    assert len(list(cells)) > 0
    _valid_h3_ids(list(cells), res=8)


def test_rast_fill_h3(conn):
    pts = pd.DataFrame(
        {
            "lon": [x[0], x[1], x[0] + 0.002],
            "lat": [y[0], y[1], y[0] + 0.002],
            "band_var": [7.0, 14.0, 21.0],
        }
    )
    layer = table_from_df(conn, "rast_pts", pts)
    envelope = box(min(pts["lon"]) - 0.01, min(pts["lat"]) - 0.01, max(pts["lon"]) + 0.01, max(pts["lat"]) + 0.01)
    out = snoo.rast_fill_h3(layer, envelope=envelope, res=10, geometry=False)
    df = out.execute()
    assert len(df) > 0
    _valid_h3_ids(df["h3_id"].tolist(), res=10)
    assert df["band_var"].sum() == pytest.approx(pts["band_var"].sum())
