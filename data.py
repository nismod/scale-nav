from os.path import exists
from numpy import array,meshgrid,arange
from rasterio.transform import xy
from rasterio import open
# from rasterio.warp import re
from geopandas import GeoSeries, GeoDataFrame,read_file
from shapely.geometry import Point,box
# import shapely as shp
from pandas import Series,concat, DataFrame
from math import pi,cos
import h3
from re import search

def rast_to_centroid(out_path,tiff_paths):
    """
    Convert a bunch of raster files given by their file location in a list into a single geospatial file containing the centroids of cells.
    If the out_path exists, then simply read it. 
    
    out_path : str, is a single path to a geojson file. 
    
    tiff_paths : list(str), is a list of paths to .tiff files.
    
    """
    if not search(pattern=".parquet$",string=out_path):
        raise ValueError("Provide a filename with .parquet extension to write the outputs.")
    
    if exists(out_path):
        print("Reading existing file")    
        grid = read_file(out_path)
    else:
        grids = []
        for path in tiff_paths:
            with open(path) as src:
                # src.re
                band1 = src.read(1)
                height = band1.shape[0]
                width = band1.shape[1]
                cols, rows = meshgrid(arange(width), arange(height))
                xs, ys = xy(transform = src.transform, rows=rows, cols=cols)

                lons = array(xs)
                lats = array(ys)

                out = DataFrame({"band" : array(band1).flatten()
                                        ,'lon': lons.flatten()
                                        ,'lat': lats.flatten()})
                
                out.drop(index=out.loc[out.band==src.nodatavals].index,inplace=True)
                out.drop(index=out.loc[out.band<=0].index,inplace=True)
                grids.append(out)

        # concatenate all the data sets into 1, the columns match, and assign the geometry
        grid = DataFrame(concat(grids))
    
        return grid
    

def df_project_on_grid(data : GeoDataFrame,res : int = 11):
    data["h3_id"] = data["geometry"].apply(lambda point: h3.latlng_to_cell(lng=point.x,lat=point.y,res=res))
    return data
    
def df_project_on_grid(data : DataFrame,res : int = 11):
    data["h3_id"] = data[["x","y"]].apply(lambda point: h3.latlng_to_cell(lng=point.loc["x"],lat=point.loc["y"],res=res),axis=1)
    return data

def pt_project_on_grid(lat,lon,res : int = 11):
    return h3.latlng_to_cell(lng=lon,lat=lat,res=res)


def square_poly(lat, lon, distance=10_000):
    """ distances in meters ! Return the shape of the raster cell from the centroid and size. Assuming square cells."""
    # distance *= 1
    distance /= 2
    earth_radius_meters = 6378137.0

    lat_rad = pi*lat/180
    # lon_rad = pi*lon/180

    dphi = distance/cos(lat_rad)/earth_radius_meters/pi*180
    dtheta = distance/earth_radius_meters/pi*180

    xlim = (lon-dphi,lon+dphi)
    ylim = (lat-dtheta,lat+dtheta)

    res = box(minx=xlim[0],maxx=xlim[1],miny=ylim[0],maxy=ylim[1])

    # return gpd.GeoSeries([res],crs=4326)
    # gpd.GeoDataFrame(geometry = gpd.GeoSeries([res],crs=4326))
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


def rast_to_h3_map(x : float = 51.51176, y : float = -0.1227):

    rast_to_h3 = {
        "10" : {"h3_res" : 13,
                "nn" : []},
        "100" : {"h3_res" : 12,
                "nn" : []},
        "1000" : {"h3_res" : 11,
                  "nn" : []},
        "10000" : {"h3_res" : 8,
                    "nn" : []},

    }

    grid_params = [10,100,1000,10000]
    
    for grid in grid_params:
        # get the right h3 resolution for the grid param.
        res_h3 = rast_to_h3[str(grid)]["h3_res"]

        square = square_poly(lat=y,lon=x,distance=grid)

        ref_cell = h3.latlng_to_cell(lat=y,lng=x,res=res_h3)
        ref_cell_ij = h3.cell_to_local_ij(origin=ref_cell,h=ref_cell)

        cells = h3.geo_to_cells(square,res=res_h3)

        cells_ij = [h3.cell_to_local_ij(origin=ref_cell,h=cell) for cell in cells]

        # neighbs = [(cell_i-ref_cell_ij[0],cell_j-ref_cell_ij[1]) for (cell_i,cell_j) in cells_ij]

        # ref_cell = h3.latlng_to_cell(lat=y,lng=x,res=res_h3)
        # ref_cell_ij = h3.cell_to_local_ij(origin=ref_cell,h=ref_cell)
        
        # square = square_poly(lat=y,lon=x,distance=grid)

        # neighbs = [h3.cell_to_local_ij(origin=ref_cell,h=cell) for cell in h3.geo_to_cells(square,res=res_h3)] 

        rast_to_h3[str(grid)]["nn"] = [(cell_i-ref_cell_ij[0],cell_j-ref_cell_ij[1]) for (cell_i,cell_j) in cells_ij]
        # [(neighb[0]-ref_cell_ij[0],neighb[1]-ref_cell_ij[1]) for neighb in neighbs]
        # centre_to_square(lat=y,lon=x,res=res_h3,grid_param=grid)
        # [(neighb[0]-ref_cell_ij[0],neighb[1]-ref_cell_ij[1]) for neighb in neighbs]

    return rast_to_h3


rast_to_h3 = rast_to_h3_map(x = 0, y = 0)


def centre_cell_to_square(h3_cell,neighbs,grid_param = 10_000):

    """If a centroid h3 index is known, return the square cover for a cell and grid_param"""

    # ref_cell = h3.cell_to_local_ij(origin=h3_cell,h=h3_cell)
    ref_cell_ij = h3.cell_to_local_ij(origin=h3_cell,h=h3_cell)

    # neighbs = rast_to_h3[str(grid_param)]["nn"]

    return [h3.local_ij_to_cell(origin=h3_cell,i=neighb[0]+ref_cell_ij[0],j=neighb[1]+ref_cell_ij[1]) for neighb in neighbs]

