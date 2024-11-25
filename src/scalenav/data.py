from os.path import exists
from numpy import array,meshgrid,arange,log
from rasterio.transform import xy
from rasterio import open
# from rasterio.warp import re
from geopandas import GeoSeries, GeoDataFrame,read_file
from shapely.geometry import Point,box,Polygon
# import shapely as shp
from pandas import Series,concat, DataFrame
from math import pi,cos
import h3
from re import search
from scalenav.utils import earth_radius_meters,A,alpha
# from overloading import overload
from functools import singledispatch

def rast_to_centroid(out_path : str, in_paths : list[str]):
    """DEPRECATED
    Convert a bunch of raster files given by their file location in a list into a single geospatial file containing the centroids of cells.
    If the out_path exists, then simply read it. 
    This is kind of deprecated and replaced by the command line tool and higher performance function using pyArrow.

    Parameters
    -----------
    out_path : str, is a single path to a parquet file. 
    
    in_paths : list(str), is a list of paths to .tiff or .nc files.

    Returns
    ----------
    a pandas.DataFrame with columns lon, lat and band.

    """
    if not search(pattern=".parquet$",string=out_path):
        raise ValueError("Provide a filename with .parquet extension to write the outputs.")
    
    if exists(out_path):
        print("Reading existing file")    
        grid = read_file(out_path)

    else:
        grids = []
        for path in in_paths:
            with open(path) as src:
                # src.re
                band1 = src.read(1)
                height = band1.shape[0]
                width = band1.shape[1]
                cols, rows = meshgrid(arange(width), arange(height))
                xs, ys = xy(transform = src.transform, rows=rows, cols=cols)

                lons = array(xs)
                lats = array(ys)

                out = DataFrame({"band_var" : array(band1).flatten()
                                        ,'lon': lons.flatten()
                                        ,'lat': lats.flatten()})
                
                out.drop(index=out.loc[out.band_var==src.nodatavals].index,inplace=True)
                out.drop(index=out.loc[out.band_var<=0].index,inplace=True)
                grids.append(out)

        # concatenate all the data sets into 1, the columns match, and assign the geometry
        grid = DataFrame(concat(grids))
    
        return grid
    

@singledispatch
def df_project_on_grid(data : GeoDataFrame, res : int = 11) -> GeoDataFrame:
    """ Project a geopandas.GeoDataFrame containing points on the H3 grid at a given resolution. 

    Parameters
    -----------
    data : a geopandas.GeoDataFrame with a geometry column containing points.
    res : a integer value of H3 resolution.
    
    Return
    ----------
    The original data frame with a new column called h3_id ccontaining the H3 code for each geometry. 

    """

    data["h3_id"] = data["geometry"].apply(lambda point: h3.latlng_to_cell(lng=point.x,lat=point.y,res=res))
    return data


@df_project_on_grid.register
def _(data : DataFrame, res : int = 11) -> DataFrame:
    """ Project a pandas.DataFrame containing points coordinates on the H3 grid at a given resolution. 

    Parameters
    -----------
    data : a pandas.DataFrame with x,y column containing containing coordinates.
    res : a integer value of H3 resolution.
    
    Return
    ----------
    The original data frame with a new column called h3_id ccontaining the H3 code for each geometry. 

    """
    data["h3_id"] = data[["x","y"]].apply(lambda point: h3.latlng_to_cell(lng=point.loc["x"],lat=point.loc["y"],res=res),axis=1)
    return data

def pt_project_on_grid(lat : float,lon : float, res : int = 11) -> DataFrame:
    """ Get H3 index of lat,lon coordinates for a resolution. Simple wrapper around the H3.latlng_to_cell function.

    """
    return h3.latlng_to_cell(lng=lon,lat=lat,res=res)


def square_poly(lat : float, lon : float, distance : int = 10_000, ref : str = "arc") -> Polygon:
    """ Make a square box with side size given in the distance parameter centered on the (lon,lat) point. 
    The distance is expected to be in meters. The coordinates in degrees according to WGS:84. 
    
    """
    half_distance = distance/2

    lat_rad = pi*lat/180

    dphi = half_distance/cos(lat_rad)/earth_radius_meters/pi*180

    if ref == "m":
        print("Using angles for meter grid.")
        dtheta = half_distance/earth_radius_meters/pi*180
    elif ref == "arc":
        print("Using angles for arc grid.")
        dtheta = half_distance/cos(lat_rad)/earth_radius_meters/pi*180
    else :
        raise Warning("Provide a valid ref parameter.")
    

    xlim = (lon-dphi,lon+dphi)
    ylim = (lat-dtheta,lat+dtheta)

    res = box(minx=xlim[0],maxx=xlim[1],miny=ylim[0],maxy=ylim[1])

    return res 

# def centre_to_square(lat,lon,res,grid_param = 10_000):
    
#     """If centroid coordinates are known, return the square cover for a h3 resolution and original grid_param."""

#     h3_cell = h3.latlng_to_cell(lng=lon,lat=lat,res=res)

#     ref_cell = h3.cell_to_local_ij(origin=h3_cell,h=h3_cell)

#     (my_lat,my_lon) = h3.cell_to_latlng(h3_cell)

#     geom = square_poly(lat = my_lat,lon = my_lon, distance=grid_param)

#     neighbs = [h3.cell_to_local_ij(origin=h3_cell,h=cell) for cell in h3.geo_to_cells(geom,res=res)] 

#     neighb_id= [(neighb[0]-ref_cell[0],neighb[1]-ref_cell[1]) for neighb in neighbs]

#     # neighbs = [h3.local_ij_to_cell(origin=h3_cell,i=ref_cell[0]+id[0],j=ref_cell[1]+id[1]) for id in neighb_id]
#     # neighbs
#     # neighbs_geo = h3.cells_to_h3shape(neighbs)
#     return neighbs
# replace this direct computation of whatever number of cells exeeds 50


def rast_to_h3_map(x : float = 0.0, y : float = 51.51, ref : str = "m", dist : float = 0):
    """Allows adding a custom distance value for unusual grid shapes and specify if the raster cells are in projected or arc sizes."""

    grid_params = []
    res_params = []

    if dist > 0 : 

        grid_params = [10,100,1000,5000,10000,dist]
        grid_params.sort()

        res_params = [round(A-alpha*log(size)) for size in grid_params]

    else:
        grid_params = [10,100,1000,5000,10000]
        res_params = [14,12,11,10,8]

    # res_params

    rast_to_h3 = { str(size): {"h3_res" : res, "nn" : [] } for (size,res) in zip(grid_params,res_params)}

    # grid_params = [10,100,1000,5000,10000]
    # res_params = [13,12,11,10,8]

    # rast_to_h3 = {
    #     "10" : {"h3_res" : 13,
    #             "nn" : []},
    #     "100" : {"h3_res" : 12,
    #             "nn" : []},
    #     "1000" : {"h3_res" : 11,
    #                 "nn" : []},
    #     "5000" : {"h3_res" : 10,
    #                 "nn" : []},
    #     "10000" : {"h3_res" : 8,
    #                 "nn" : []},
    # }

    for grid in grid_params:
        # get the right h3 resolution for the grid param.
        res_h3 = rast_to_h3[str(grid)]["h3_res"]

        square = square_poly(lat=y,lon=x,distance=grid,ref=ref)

        ref_cell = h3.latlng_to_cell(lat=y,lng=x,res=res_h3)
        ref_cell_ij = h3.cell_to_local_ij(origin=ref_cell,h=ref_cell)

        cells = h3.geo_to_cells(square,res=res_h3)

        cells_ij = [h3.cell_to_local_ij(origin=ref_cell,h=cell) for cell in cells]

        rast_to_h3[str(grid)]["nn"] = [(cell_i-ref_cell_ij[0],cell_j-ref_cell_ij[1]) for (cell_i,cell_j) in cells_ij]

    return rast_to_h3


rast_to_h3 = rast_to_h3_map(x = 0, y = 0)


def centre_cell_to_square(h3_cell : str, neighbs : list[tuple[int]]) -> list[str]:
    """If a centroid h3 index is known, return the square cover for a cell and grid_param"""

    sql = """SQL
    
    select *, array_value h3_gridded

    h3_local_ij_to_cell(h3_id,neighbs[:][0],neighbs[:][1])

    """
    ref_cell_ij = h3.cell_to_local_ij(origin=h3_cell,h=h3_cell)

    return [h3.local_ij_to_cell(origin=h3_cell,i=cell_i+ref_cell_ij[0],j=cell_j+ref_cell_ij[1]) for (cell_i,cell_j) in neighbs]


def layer_constrain(layer : [DataFrame,GeoDataFrame], constraint : [DataFrame,Series]):
    """Apply a constraint to a layer. Remove the constrained cells from the layer and renormalise the values."""
    return layer


