# test the recursive functions for scale change.

# using the pytest framework : https://docs.pytest.org/en/stable/how-to/fixtures.html

# setup some `fixtures` to make the sample data.
import numpy as np
import geopandas as gpd
import pandas as pd
import h3
import scalenav.scale_nav as sn
import scalenav.data as sd
import scalenav.oop as snoo
import itertools as iter
import ibis as ib

## size of table
size = 10
h3_res = 10

x = np.random.uniform(low=-180, high=180, size=size).tolist()
y = np.random.uniform(low=-90, high=90, size=size).tolist()
band_var = np.random.exponential(scale=5, size=size).tolist()

coords_geom = gpd.GeoDataFrame(
    data={"band_var": band_var},
    geometry=gpd.GeoSeries.from_xy(x=x, y=y, crs="epsg:4326", name="geom"),
)

coords_xy = pd.DataFrame(
    data={
        "lon": x,
        "lat": y,
        "band_var": band_var,
    },
)


def test_project_on_grid():

    bench = [
        h3.latlng_to_cell(lng=x, lat=y, res=h3_res)
        for (x, y) in zip(coords_xy["lon"], coords_xy["lat"])
    ]
    res = sd.df_project_on_grid(coords_xy, res=h3_res)["h3_id"].to_list()

    assert res == bench


def test_change_scale_xy():

    res_h3 = sd.df_project_on_grid(coords_xy, res=h3_res)

    bench = list(iter.chain.from_iterable([[x / 7] * 7 for x in band_var]))

    res = sn.change_res(res_h3, 1)

    assert res["band_var"].to_list() == bench, ""


def test_scale_change_down():

    conn = snoo.sn_connect()

    test_start_res = 7
    test_levels = [1, 3]

    h3_id = [h3.latlng_to_cell(y, x, res=test_start_res) for (x, y) in zip(x, y)]

    resc_test = pd.DataFrame({"h3_id": h3_id})
    resc_test["vals_var"] = 7

    resc_test_back = conn.create_table("resc_test", resc_test)

    for lev in test_levels:

        res_pd = sn.change_res(resc_test, lev)
        # generating the data set with the new
        res_ib = snoo.sn_change_res(resc_test_back, lev)

        res_df_down = res_ib.execute()

        assert np.all(res_df_down.vals_var == res_pd.vals_var), ""
        assert res_df_down.shape[0] == res_pd.shape[0], ""
        assert (
            len(np.intersect1d(res_df_down.h3_id, res_pd.h3_id)) == res_df_down.shape[0]
        ), ""

        res_df_up = snoo.sn_change_res(res_ib, levels=-lev).execute()

        assert np.all(
            np.float32(res_df_up.vals_var) == np.float32(resc_test.vals_var)
        ), ""
        assert res_df_up.shape[0] == resc_test.shape[0], ""
        assert (
            len(np.intersect1d(res_df_up.h3_id, resc_test.h3_id)) == res_df_up.shape[0]
        ), ""
