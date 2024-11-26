# scale_down.py
# import geopandas as gpd
from geopandas import GeoDataFrame,GeoSeries
import h3
# import pandas as pd
from pandas import DataFrame
from re import search,compile
from scalenav.utils import res_upper_limit,res_lower_limit,child_num
from ibis import Table

def set_res(grid : [DataFrame,GeoDataFrame],final : int = 8) -> [DataFrame,GeoDataFrame] : # type: ignore
    """Similarly to the change_scale function, but sets the resolution to a specific value.
    
    Parameters
    -------------

    grid : a data frame with h3_id column.

    final : the final resolution value desired.

    Returns
    -----------

    A data table with the desired resolution and data transformations performed.

    """

    if final>res_upper_limit or final<res_lower_limit:
        raise ValueError("Please provide an allowed resolution value")
    
    levels = final - h3.get_resolution(grid.h3_id.to_list()[0])

    return change_res(grid=grid,level=levels)

def change_res(grid : [DataFrame,GeoDataFrame],level : int = 1) -> [DataFrame,GeoDataFrame] : # type: ignore
  
    f"""Change scale of a data frame using the H3 hieararchical index. Should any str containing column ( apart from osm_id ) be assumed as a factor ?
    
    Parameters
    ------------

    grid : a pandas.DataFrame or geopandas.GeoDataFrame that contains among others a column named `h3_id`. To signal columns that contain variables of interest, their name should end with the `_var` suffix.
    These variables will be assumed additive and have their values rescaled depending on the transformation (up or down the scale). Every other column will see it's value broadcasted if the resolution increases or the first will be taken if the resolution decreases.

    level : a positive or negative integer value giving the relative scale change to perform. In other words, the final H3 resolution of the data will be equal to the resolution before change + level. The final resolution should be within {res_lower_limit}-{res_upper_limit}.

    Returns
    -----------

    A data table that ...

    """

    res = h3.get_resolution(grid.h3_id.to_list()[0])

    if (res+level)>res_upper_limit or (res+level)<res_lower_limit: 
        raise ValueError(f"Resolution exceeded the allowed boundaries {res_lower_limit} - {res_upper_limit}")

    grid = grid.copy()
    
    p = compile("_var$")
    # p_cat = compile("")

    var_columns = [x for x in grid.columns if search(p,x)]
    var_operations = {x:"sum" for x in var_columns}

    try : 
        grid.drop(columns="geom",inplace=True)
        print("Skipping geom")
    except : ""

    if int(level)>=2: 
        grid["h3_id"] = grid.h3_id.apply(h3.cell_to_children)
        grid = grid.explode("h3_id").reset_index(drop=True)
        grid[var_columns] = grid[var_columns]/child_num
        return change_res(grid,level=level-1)

    if int(level)==1:
        grid["h3_id"] = grid.h3_id.apply(h3.cell_to_children)
        grid = grid.explode("h3_id").reset_index(drop=True)
        grid[var_columns] = grid[var_columns]/child_num
        return grid
    
    if int(level)==-1:

        grid["parent"] = grid.h3_id.apply(h3.cell_to_parent)
        grid=grid.groupby("parent").agg({"h3_id":list,**var_operations}).reset_index().rename(columns = {"h3_id":"child_cells"}).rename(columns = {"parent":"h3_id"})
        return grid
    
    if int(level)<=-2:
        
        grid["parent"] = grid.h3_id.apply(h3.cell_to_parent)
        grid=grid.groupby("parent").agg({"h3_id":list,**var_operations}).reset_index().rename(columns = {"h3_id":"child_cells"}).rename(columns = {"parent":"h3_id"})
        
        return change_res(grid=grid,level=level+1)

    return Warning("Could not successfully change scale.")


def add_geom(grid : [DataFrame,GeoDataFrame],crs="EPSG:4326") -> GeoDataFrame: # type: ignore
    """Add the geometries of the cells to a data frame projected on the H3 grid. Using the h3_id column as reference.

    Parameters
    -------------

    grid : A data table that contains an 'h3_id' column among others.
    crs : default 'epsg:4326'.

    Returns
    ----------
    A geopandas.GeoDataFrame with a column 'geom' containing the geometries of the hexagons. 
    
    """
    grid["geom"] = GeoSeries(grid.h3_id.apply(lambda x: h3.cells_to_h3shape([x]))) 
    return GeoDataFrame(grid,geometry="geom",crs=crs)


def h3_add_centr(input : Table):
    """Add centroid coordinates of h3 cell to table. Useful for further spatial operations."""
    return (input
            .alias("t_dens")
            .sql("""
                 Select * EXCLUDE latlng, ST_POINT(latlng[2],latlng[1]) as geom 
                 FROM (SELECT *, h3_cell_to_latlng(h3_id) as latlng FROM t_dens) 
                 AS h3_geom;"""))

