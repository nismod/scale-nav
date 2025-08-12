import numpy as np
import geopandas as gpd
import pandas as pd

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
