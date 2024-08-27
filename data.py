from os.path import exists
from numpy import array,meshgrid,arange
from rasterio.transform import xy
from rasterio import open
from geopandas import GeoSeries, GeoDataFrame,read_file
from shapely.geometry import Point, box
from pandas import Series,concat
from math import pi,cos
import h3

def rast_to_centroid(out_path,tiff_paths):
    """
    Convert a bunch of raster files given by their file location in a list into a single geospatial file containing the centroids of cells.
    If the out_path exists, then simply read it. 
    
    out_path : str, is a single path to a geojson file. 
    
    tiff_paths : list(str), is a list of paths to .tiff files.
    
    """
    if exists(out_path):
        print("Reading existing file")    
        grid = read_file(out_path)
    else:
        grids = []
        for path in tiff_paths:
            with open(path) as src:
                band1 = src.read(1)
                height = band1.shape[0]
                width = band1.shape[1]
                cols, rows = meshgrid(arange(width), arange(height))
                xs, ys = xy(transform = src.transform, rows=rows, cols=cols)

                lons = array(xs)
                lats = array(ys)

                points = GeoSeries(Series(list(zip(lons.flatten(), lats.flatten()))).map(Point))

                geoms = points.values

                out = GeoDataFrame({"band" : array(band1).flatten()
                                        ,'geometry': geoms },crs=src.crs)
                
                grids.append(out)

        # concatenate all the data sets into 1, the columns match, and assign the geometry
        grid = GeoDataFrame(concat(grids)
                                ,geometry="geometry")
        # some crazy outliers in the ocean
        grid.loc[grid.band>1e9, "band"] = 0

        grid = grid[grid.band>0]
        # print("Saving file")
        # grid.to_file(out_path,driver="geojson")
        return grid
    

def df_project_on_grid(data : GeoDataFrame,res : int = 11):
    data["h3_id"] = data["geometry"].apply(lambda point: h3.latlng_to_cell(lng=point.x,lat=point.y,res=res))
    return data
    
def pt_project_on_grid(lat,lon,res : int = 11):
    return h3.latlng_to_cell(lng=lon,lat=lat,res=res)

def square_poly(lat, lon, distance=10_000):
    """ distances in meters ! Return the shape of the raster cell from the centroid and size. Assuming square cells."""
    distance *= 0.9
    distance /= 2
    earth_radius_meters = 6378137.0

    lat_rad = pi*lat/180
    # lon_rad = pi*lon/180

    dphi = distance/cos(lat_rad)/earth_radius_meters/pi*180
    dtheta = distance/earth_radius_meters/pi*180


    xlim = (lon-dphi,lon+dphi)
    ylim = (lat-dtheta,lat+dtheta)

    res = box(xmin=xlim[0],xmax=xlim[1],ymin=ylim[0],ymax=ylim[1])

    # return gpd.GeoSeries([res],crs=4326)
    # gpd.GeoDataFrame(geometry = gpd.GeoSeries([res],crs=4326))
    return res 


def centre_cell_to_square(h3_cell,res,grid_param = 10_000):

    """If a centroid h3 index is known, return the suqare cover for a resolution and grid_param"""

    ref_cell = h3.cell_to_local_ij(origin=h3_cell,h=h3_cell)

    (my_lat,my_lon) = h3.cell_to_latlng(h3_cell)

    geom = square_poly(lat = my_lat,lon = my_lon, distance=grid_param)

    neighbs = [h3.cell_to_local_ij(origin=h3_cell,h=cell) for cell in h3.geo_to_cells(geom,res=res)] 

    neighb_id= [(neighb[0]-ref_cell[0],neighb[1]-ref_cell[1]) for neighb in neighbs]

    neighbs = [h3.local_ij_to_cell(origin=h3_cell,i=ref_cell[0]+id[0],j=ref_cell[1]+id[1]) for id in neighb_id]
    # neighbs
    # neighbs_geo = h3.cells_to_h3shape(neighbs)
    return len(neighb_id),neighbs




def centre_to_square(lat,lon,res,grid_param = 10_000):
    
    """If centroid coordinates are known, return the square cover for a h3 resolution and original grid_param."""

    h3_cell = h3.latlng_to_cell(lng=lon,lat=lat,res=res)

    ref_cell = h3.cell_to_local_ij(origin=h3_cell,h=h3_cell)

    (my_lat,my_lon) = h3.cell_to_latlng(h3_cell)

    geom = square_poly(lat = my_lat,lon = my_lon, distance=grid_param)

    neighbs = [h3.cell_to_local_ij(origin=h3_cell,h=cell) for cell in h3.geo_to_cells(geom,res=res)] 

    neighb_id= [(neighb[0]-ref_cell[0],neighb[1]-ref_cell[1]) for neighb in neighbs]

    neighbs = [h3.local_ij_to_cell(origin=h3_cell,i=ref_cell[0]+id[0],j=ref_cell[1]+id[1]) for id in neighb_id]
    # neighbs
    # neighbs_geo = h3.cells_to_h3shape(neighbs)
    return len(neighb_id),neighbs
