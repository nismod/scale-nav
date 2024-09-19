# scale_down.py
# import geopandas as gpd
from geopandas import GeoDataFrame,GeoSeries
import h3
# import pandas as pd
from pandas import DataFrame
from re import search,compile
from scalenav.utils import res_upper_limit,res_lower_limit,child_num


def set_scale(grid : [DataFrame,GeoDataFrame],final : int = 8) -> [DataFrame,GeoDataFrame] : # type: ignore
    
    """Similarly to the change_scale function, but sets the resolution to a specific value."""

    if final>res_upper_limit or final<res_lower_limit:
        raise ValueError("Please provide an allowed resolution value")
    
    levels = final - h3.get_resolution(grid.h3_id.to_list()[0])
    
    return change_scale(grid=grid,level=levels)


def change_scale(grid : [DataFrame,GeoDataFrame],level : int = 1) -> [DataFrame,GeoDataFrame] : # type: ignore
  
    f""" Change scale of a data frame using the H3 hieararchical index.

    Parameters
    ------------

    grid : a pandas.DataFrame or geopandas.GeoDataFrame that contains among others a column named `h3_id`. To signal columns that contain variable of interest, their name should end with the `_var` suffix.
    These variables will be assumed additibe and have their values rescaled depending on the transformation (up or down the scale). Every other column will see it's value broadcasted if the resolution increases or the first will be taken if the resolution decreases.

    level : a positive or integer value giving the relative scale change to perform. In other words, the final H3 resolution of the data will be equal to the resolution before change + level. The final resolution should be within {res_lower_limit}-{res_upper_limit}.

    """

    res = h3.get_resolution(grid.h3_id.to_list()[0])

    if (res+level)>res_upper_limit or (res+level)<res_lower_limit: 
        raise ValueError(f"Resolution exceeded the allowed boundaries {res_lower_limit} - {res_upper_limit}")

    grid = grid.copy()
    
    p = compile("_var$")

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
        return change_scale(grid,level=level-1)

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
        
        return change_scale(grid=grid,level=level+1)

    return Warning("Could not successfully change scale.")


def add_geom(grid : [DataFrame,GeoDataFrame],crs="EPSG:4326") -> GeoDataFrame: # type: ignore
    """ Add the geometries of the cells to a data frame projected on the H3 grid. Using the h3_id column as reference.
    
    """
    grid["geom"] = GeoSeries(grid.h3_id.apply(lambda x: h3.cells_to_h3shape([x]))) 
    return GeoDataFrame(grid,geometry="geom",crs=crs)
