# test the recursive functions for scale change.

# using the pytest framework : https://docs.pytest.org/en/stable/how-to/fixtures.html

# setup some `fixtures` to make the sample data.
import numpy as np
import geopandas as gpd
import pandas as pd
import h3
import scalenav.scale_nav as sn
import scalenav.data as sd
import itertools as iter

## size of table
size = 10
h3_res = 10

x = np.random.uniform(low=-180,high=180,size=size).tolist()
y = np.random.uniform(low=-90,high=90,size=size).tolist()
band_var = np.random.exponential(scale=5,size=size).tolist()

coords_geom = gpd.GeoDataFrame(
    data = {"band_var" : band_var},
    geometry=gpd.GeoSeries.from_xy(
                                x=x,
                                y=y,
                                crs="epsg:4326",
                                name="geom"),
)

coords_xy = pd.DataFrame(data={"x" : x,
                            "y" : y,
                            "band_var" : band_var,},
                            columns=["x","y","band_var"],
)


def test_project_on_grid():

    bench = [h3.latlng_to_cell(lng=x,lat=y,res=h3_res) for (x,y) in zip(coords_xy["x"],coords_xy["y"])]
    res = sd.df_project_on_grid(coords_xy,res = h3_res)["h3_id"].to_list()

    assert res==bench


def test_change_scale_xy():
    
    res_h3 = sd.df_project_on_grid(coords_xy,res = h3_res)

    bench = list(iter.chain.from_iterable([[x/7]*7 for x in band_var]))

    res = sn.change_res(res_h3,1)

    assert res["band_var"].to_list()==bench