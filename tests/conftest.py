import geopandas as gpd
import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def size():
    return 10


@pytest.fixture
def h3_res():
    return 10


@pytest.fixture
def x(size):
    return np.random.uniform(low=-180, high=180, size=size).tolist()


@pytest.fixture
def y(size):
    return np.random.uniform(low=-90, high=90, size=size).tolist()


@pytest.fixture
def band_var(size):
    return np.random.exponential(scale=5, size=size).tolist()


@pytest.fixture
def coords_geom(x, y, band_var):
    return gpd.GeoDataFrame(
        data={"band_var": band_var},
        geometry=gpd.GeoSeries.from_xy(x=x, y=y, crs="epsg:4326", name="geom"),
    )


@pytest.fixture
def coords_xy(x, y, band_var):
    return pd.DataFrame(
        data={
            "lon": x,
            "lat": y,
            "band_var": band_var,
        },
    )
