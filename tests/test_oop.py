# %%
import ibis as ib
from ibis import selectors as s
from ibis import _

import h3
import os

import pytest
import numpy as np
import pandas as pd

import scalenav.oop as snoo
import scalenav.scale_nav as sn

from tests.test_utils import *
from tests.test_rast_converter import rast_ingest


##############
# %%
@pytest.fixture
def conn():
    return snoo.connect()


# %%


@pytest.fixture
def table1_h3(conn):

    # tdata_1 = conn.read_parquet("data/test_data_0.parquet")
    # tdata_1

    table1 = conn.create_table(
        "table1",
        obj=pd.DataFrame({"x": x, "y": y, "band_var": band_var}),
    )

    return snoo.project(table1, res=h3_res)


# %%
def test_project(table1_h3):
    coords_xy["h3_id"] = [
        h3.latlng_to_cell(lng=x, lat=y, res=h3_res)
        for (x, y) in zip(coords_xy["lon"], coords_xy["lat"])
    ]

    table1_h3_df = table1_h3.execute()

    assert "h3_id" in table1_h3_df.columns
    assert np.all(table1_h3_df["h3_id"] == coords_xy["h3_id"])

    # assigning a new resolution to a layer with an 'h3_id' column
    h3_res_reproject = 11

    coords_xy["h3_id"] = [
        h3.latlng_to_cell(lng=x, lat=y, res=h3_res_reproject)
        for (x, y) in zip(coords_xy["lon"], coords_xy["lat"])
    ]

    table1_h3_overwrite_df = snoo.project(table1_h3, res=h3_res_reproject).execute()

    assert "h3_id" in table1_h3_overwrite_df.columns
    assert np.all(table1_h3_overwrite_df["h3_id"] == coords_xy["h3_id"])

    ####### OO setup

    # assert np.all(
    #     table1.project(h3_res).select("h3_id").execute() == coords_xy["h3_id"]
    # )


# %%


def test_change_res(table1_h3):
    table_ds_df = snoo.change_res(table1_h3, levels=-1).execute().set_index("h3_id")

    table_res_df = (
        sn.change_res(table1_h3.execute(), level=-1)
        .set_index("h3_id")
        .loc[:, ["band_var"]]
    )

    assert np.all(table_ds_df - table_res_df == 0)


def test_add_centr(table1_h3):
    table1_centr = snoo.add_centr(table1_h3)
    assert "geom" in table1_centr.columns
    assert (table1_centr.select(s.of_type("GEOMETRY")).count() > 0).execute()
    # assert table1_centr.select("geom").execute().dtype == "geom"
    assert table1_centr.execute().shape[0] == table1_h3.count().execute()


def test_reindex():
    assert True
    # pass


def test_rescale():
    assert True
    # pass


def test_concat():
    assert True
    # pass


def test_combine():
    assert True


def test_constrain():
    assert True


def test_join():
    assert True
