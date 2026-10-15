import geopandas as gpd
import pandas as pd

## size of table
h3_res = 10

# Fixed lon/lat so H3 ids are reproducible. The first two points are centroids
# of sibling res-10 cells that share a parent at res-9 (Oxford area).
x = [
    -1.2581133271935658,  # Oxford sibling A
    -1.2574742473233895,  # Oxford sibling B (same parent as A)
    -0.1278,  # London
    0.1218,  # Cambridge
    -0.9781,  # Reading
    -2.2426,  # Manchester
]

y = [
    51.75364443360959,
    51.752539103469246,
    51.5074,
    52.2053,
    51.4543,
    53.4808,
]

band_var = [7.0, 14.0, 21.0, 28.0, 35.0, 42.0]

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
