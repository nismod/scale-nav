"""Module sketching out the OO backbone. This is intended to reduce the complexity of the user facing side of the workflow by wrapping the underlying functions
into methods associated to objects that have a defined structure and therefore are more failure robust."""

from pathlib import Path
import os 
from ibis import duckdb
from ibis import table,to_sql
import ibis as ib
import re
from numpy import random

configs = {
    "size_lim" : "500MB"
}

class ScalenavProcess:

    src_db : str = "duckdb://"

    def __init__(self) -> None:
        """Initiate a duckDB connection with the spatial and H3 extensions.
        """
        pass        

    def export() -> None:
        """Save the underlying duckDB data base in it's current state. It contains the table schemas for future use.
        """
        pass

class Layer(ScalenavProcess): 

    base_res : int
    current_res : int
    src : str

    def __init__(self) -> None:
        """Validation of the input source goes here. What is the schema ? """
        pass

    def project(self, resolution : int = 8, spatial_var : [tuple,str] = ("lon","lat")) -> None: # type: ignore
        """Project the data onto the h3 grid at the given resolution. 
        The column containing the spatial variables to use can be provided as argument in the sptaial_var paramater if it is not ('lon','lat').
        """
        pass

    def change_res(self):
        """Change resolution by a fixed 'delta' value. Can be a positive of negative integer. The resulting resolution will be equal to the current one plus the delta value. 
        The restriciton is that the final resolution has to be within the accepted limits. 
        """
        pass

    def set_res(self):
        """Set the reolution of a layer to a given one as target. If no column containing h3_ids is present, project the data to that resolution.
        """
        pass
    
    def restore(self):
        """Reloads the original data from the 'src' attribute. Usefull to reset the layer after some manipulations have been done and information potentially lost. 
        Overwrites the current state of the layer ?
        """
        pass

    def get_data(self):
        """Get the data table with the data as a regular pandas.DataFrame. Not recommended for very big layers (>1e6 rows)
        """
        pass

geom_column = ["x","lon","lng","longitude","y","lat","ltd","latitude","geom","geometry"]

class DataLayer(Layer):
    def __init__(self) -> None:
        super().__init__()


##########

def sn_connect(interactive : bool = True):
    """Create a duckDB connection with spatial and H3 extensions loaded.
    """
    
    ib.options.interactive = interactive
    
    conn = duckdb.connect()
    
    conn.raw_sql("""
        INSTALL spatial; 
        LOAD spatial;
        INSTALL h3 FROM community;
        LOAD h3;
    """)
    
    return conn

def h3_project(input : ib.Table,res : int = 8, columns : [tuple,None] = None) -> ib.Table: # type: ignore
    """Given an ibis table with coordinates columns, 
    return a table with a new column resulting from generating h3 ids for the points at given parameter h3 resolution.
    """

    alias_code = "".join([str(x) for x in random.randint(low=0,high=9,size=10)])
    alias_name = f"h3_project_{alias_code}"

    if columns is None:
        col_x = [x for x in input.columns if re.search(string=x,pattern=r"(^lon)|(^lng)|(^x)|(^east)")]
        col_y = [x for x in input.columns if re.search(string=x,pattern=r"(^lat)|(^ltd)|(^y)|(^north)")]

        if len(col_x)>1 or len(col_y)>1:
            raise IOError("Ambiguous coordinates column names, provide explicitly in 'columns' argument.")
        
        col_x=col_x[0]
        col_y=col_y[0]

        print(f"Assuming coordinates columns ('{col_x}','{col_y}')")
        
    elif type(columns) is tuple and type(columns[0]) is str and type(columns[1]) is str: 
        col_x=columns[0]
        col_x=columns[1]
        print(f"Using coordinates columns ('{col_x}','{col_y}')")

    elif type(columns) is tuple and type(columns[0]) is int and type(columns[1]) is int:
        col_x = input.columns[columns[0]]
        col_y = input.columns[columns[1]]
        print(f"Using coordinates columns ('{col_x}','{col_y}')")

    else :
        raise ValueError("Could not recognise coordinates columns")

    if "h3_id" in input.columns:
        print("Existing h3_id column will be overwritten")

    return input.alias(alias_name).sql(f"""Select *, h3_h3_to_string(h3_latlng_to_cell({col_y},{col_x},{res})) as h3_id from {alias_name};""")