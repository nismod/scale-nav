# scale_down.py
import geopandas as gpd
import h3
import pandas as pd
import re
import numpy as np

def change_scale(grid,level = 1):

    res = h3.get_resolution(grid.h3_id.to_list()[0])

    if (res+level)>11 or (res+level)<3: 
        raise ValueError("Resolution exceeded the allowed boundaries 3 - 11")

    grid = grid.copy()

    child_num=7
    
    p = re.compile("_var$")
    var_columns = [x for x in grid.columns if re.search(p,x)]
    
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

        var_operations = {x:"sum" for x in var_columns}
        grid["parent"] = grid.h3_id.apply(h3.cell_to_parent)
        # grid=grid.groupby("parent").agg("sum").reset_index()
        # grid = grid.h3_id.apply(h3.cell_to_parent)
        grid=grid.groupby("parent").agg({"h3_id":list,**var_operations}).reset_index().rename(columns = {"h3_id":"child_cells"}).rename(columns = {"parent":"h3_id"})
        return grid
    
    if int(level)<=-2:
        var_operations = {x:"sum" for x in var_columns}
        grid["parent"] = grid.h3_id.apply(h3.cell_to_parent)
        grid=grid.groupby("parent").agg({"h3_id":list,**var_operations}).reset_index().rename(columns = {"h3_id":"child_cells"}).rename(columns = {"parent":"h3_id"})
        
        return change_scale(grid=grid,level=level+1)

    return 0


def add_geom(grid,crs="EPSG:4326"):
    grid["geom"] = gpd.GeoSeries(grid.h3_id.apply(lambda x: h3.cells_to_h3shape([x]))) 
    return gpd.GeoDataFrame(grid,geometry="geom",crs=crs)


##### another function that might be more precise, using the k-ring of a cell

# def get_square(h3_cell: str, d: float = 10000):
#     """Returns the neighbourhood of the 'h3_cell' that approximates a square of side 'd'."""
#     h3_d = np.round(np.sqrt(h3.cell_area(h3_cell,unit="m^2")),2)
#     frag = int(np.ceil(d/h3_d))

#     h3_ref = h3.cell_to_local_ij(origin=h3_cell,h=h3_cell)

#     i_range = range(-frag,frag+1)
#     j_range = range(-frag,frag+1)

#     coords = [(i,j) for i in i_range for j in j_range]
#     # coords
#     ref_cell = h3.cell_to_local_ij(origin=h3_cell,h=h3_cell)

#     return [h3.local_ij_to_cell(origin=h3_cell,i=ref_cell[0]+id[0],j=ref_cell[1]+id[1]) for id in coords]


