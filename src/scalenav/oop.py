"""Module sketching out the OO backbone. This is intended to reduce the complexity of the user facing side of the workflow by wrapping the underlying functions
into methods associated to objects that have a defined structure and therefore are more failure robust."""

# from pathlib import Path
import os 
from ibis import table,to_sql,_,duckdb
import ibis.selectors as s
import ibis as ib
import re
from numpy import random,all
from duckdb import connect

configs = {
    "memory_limit" : "500MB"
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

def sn_head(input : ib.Table,n : int = 5):
    print(input.count())
    print(input.head(n))


def sn_connect(database = ':memory:',interactive : bool = True,**kwargs):
    """Create a duckDB connection with spatial and H3 extensions loaded.
    This function will not create a db from a jupyter notebook. run it in a script or a terminal to perform this action.
    """
    
    ib.options.interactive = interactive

    if database in ["memory", ':memory:', "mem"]:
        print("Connecting to a temporary in-memory DB instance.")
        conn = ib.duckdb.connect()
    elif os.path.exists(database):
        print("connecting to existing database.")
        conn = ib.duckdb.connect(database)
    else :
        print("Creating database at'",database,"'.")
        with connect(database) as con:
            pass
        conn = ib.duckdb.connect(database)

    conn.raw_sql("""
        INSTALL spatial; 
        LOAD spatial;
        INSTALL h3 FROM community;
        LOAD h3;
                    """)
    
    return conn

def sn_table(conn,name : str,path : str,*args,**kwargs):
    q_startup = "create"
    if "overwrite" in kwargs.keys():
        print("overwriting existing")
        q_startup = "create or replace"

    if "bbox" in kwargs.keys():
        print("reading bbox")
        conn.raw_sql(f"{q_startup} table {name} as (select * from '{path}' where lon>{kwargs["bbox"][0]} AND lon<{kwargs["bbox"][2]} AND lat>{kwargs["bbox"][1]} AND lat<{kwargs["bbox"][3]});")
        return conn.table(name)
    else : 
        try :
            conn.raw_sql(f"{q_startup} table {name} as (select * from '{path}');")
            return conn.table(name)
        except:
            if name in conn.list_tables():
                print("Backend table exists, connected.")
                return conn.table(name)
        return 0   

def sn_project(input : ib.Table,res : int = 8, columns : [tuple,None] = None, keep = True, for_gridding : bool = False) -> ib.Table: # type: ignore
    """Given an ibis table with coordinates columns, 
    return a table with a new column resulting from generating h3 ids for the points at given parameter h3 resolution.
    """

    if "h3_id" in input.columns:
        print("Existing h3_id column will be overwritten")
        input = input.drop("h3_id")
        # print("H3 column exists.")
        # return input

    alias_code = "".join([str(x) for x in random.randint(low=0,high=9,size=10)])
    alias_name = f"h3_project_{alias_code}"

    if columns is None:
        col_x = [x for x in input.columns if re.search(string=x,pattern=r"(^lon)|(^lng)|(^x)|(^east)",flags=re.IGNORECASE)]
        col_y = [x for x in input.columns if re.search(string=x,pattern=r"(^lat)|(^ltd)|(^y)|(^north)",flags=re.IGNORECASE)]

        if len(col_x)>1 or len(col_y)>1:
            raise IOError("Ambiguous coordinates column names, provide explicitly in 'columns' argument.")
        
        col_x=col_x[0]
        col_y=col_y[0]

        print(f"Assuming coordinates columns ('{col_x}','{col_y}')")
        
    elif type(columns) is tuple and type(columns[0]) is str and type(columns[1]) is str: 
        col_x=columns[0]
        col_y=columns[1]
        print(f"Using coordinates columns ('{col_x}','{col_y}')")

    elif type(columns) is tuple and type(columns[0]) is int and type(columns[1]) is int:
        col_x = input.columns[columns[0]]
        col_y = input.columns[columns[1]]
        print(f"Using coordinates columns ('{col_x}','{col_y}')")

    else :
        raise ValueError("Could not recognise coordinates columns")


    if keep :
        return input.alias(alias_name).sql(f"""Select *, h3_h3_to_string(h3_latlng_to_cell({col_y},{col_x},{res})) as h3_id from {alias_name};""")
    else:
        return input.alias(alias_name).sql(f"""Select * EXCLUDE ({col_x},{col_y}), h3_h3_to_string(h3_latlng_to_cell({col_y},{col_x},{res})) as h3_id from {alias_name};""")


def sn_change_res(input : ib.Table,levels : int = 1) -> ib.Table :
    """Projecting an H-indexed table through scales in the DuckDB backend."""

    # generate an alias to expose the tables in the back
    code = "".join([str(x) for x in random.randint(0,9,10)])
    alias_code = f"input_{code}"

    try:
        levels = int(levels)
    except:
        raise Warning("levels must be integer.")
    
    # need to be already projected.
    if "h3_id" not in input.columns:
        raise Warning("Project data into h3 first")
    
    res = input.alias(alias_code).sql(f"Select h3_get_resolution(h3_id) as h3_res from {alias_code} limit 1;")[0].as_scalar().execute()

    if levels>0:
        
        rescale_factor = 7**(-levels)
        # transformation expressions for navigating scales 
        transform_expr = { x : ( _[x] * rescale_factor ) for x in input.columns if re.search(pattern="_var$",string=x)}
    
        return (
            input
            .alias(alias_code)
            .sql(f"""Select *, h3_cell_to_children(h3_id,{res+levels}) as new_h3_id from {alias_code};""")
            .unnest("new_h3_id")
            .mutate(**transform_expr)
            .drop("h3_id")
            .rename(h3_id="new_h3_id")
)
    
    if levels<0:
        
        transform_expr_agg = { x : ( _[x].sum() ) for x in input.columns if re.search(pattern="_var$",string=x)}
        
        return (
            input
            .alias(alias_code)
            .sql(f"""Select *, h3_cell_to_parent(h3_id,{res+levels}) as new_h3_id from {alias_code};""")
            .group_by("new_h3_id")
            .agg(**transform_expr_agg)
            .rename(h3_id="new_h3_id")
)


def sn_add_centr(input : ib.Table):
    """Add the centroid of a hex cell in the table expression."""
    # generate an alias to expose the tables in the back
    code = "".join([str(x) for x in random.randint(0,9,10)])
    alias_code = f"input_{code}"

    return (input
            .alias(alias_code)
            .sql(f"""Select * EXCLUDE latlng, ST_POINT(latlng[2],latlng[1]) as geom
                    FROM
                        (SELECT *, h3_cell_to_latlng(h3_id) as latlng FROM {alias_code});""")
)


def sn_reindex(input : ib.Table,names_from=None,values_from="id",values_agg="count",values_fill=0,) : 
    """Transpose a variable containing categories associated to located data into an H3 
    indexed table with new columns based on the categorical values. This performs a pivot."""
    
    if names_from is None:
        names_from = input.select(s.of_type("string")).columns

    return (input
            .pivot_wider(
                id_cols="h3_id",
                names_from=names_from,
                values_from=values_from,
                values_agg=values_agg,
                values_fill=values_fill,
                )
)


def sn_rescale(input : ib.Table, weight : ib.Table,weight_var : [str] = "weight_var", weight_geom : str = "geometry", weight_id : str = "id", keep_left : [str] = None ,keep_right : [str] = None) -> ib.Table : 
    """This function takes two layers, assuming the secong parameter contains the layer with data to downscale. Therefore the first layer is supposed to be spatially more granular.
    It intersects them and rescales the values from the second layer by weighting according to the first layer."""

    if "h3_id" in input.columns: 
        input = sn_add_centr(input=input)
        
    if weight_geom not in weight.columns:
        weight_geom = [x for x in input.select(s.of_type("GEOMETRY")).columns if re.search(string=x,pattern="geom")][0]
        print("Assuming geometry column ",weight_geom)
    
    if weight_id not in weight.columns:
        weight = weight.mutate(id = ib.row_number())

    return (input
     .join(
         weight.select(weight_id,weight_var,weight_geom),
         how="left",
         predicates=input["geom"].intersects(weight[weight_geom]))
     .select(~s.of_type("GEOMETRY"))
     .mutate(
        s.across(s.matches("_var$"),
             _/_.sum().over(group_by=weight[weight_id]),
             names="{col}_dens"))
     .mutate(
        s.across(s.matches("_dens$"),
             _*weight[weight_var],
             names="{col}_var"))
     .select(~s.matches("_dens$"))
)

def sn_concat(input,**kwargs):
    """Stack and regroup a set of layers into 1. Similar effect to concat in pandas.
    """
    if all(["h3_id" in tab.columns for tab in list(input.values())]):
        return ib.union(*list(input.values()),**kwargs).group_by("h3_id").agg()
    return ib.union(*list(input.values()),**kwargs)


def sn_combine(conn, input : [ib.Table], name : str, overwrite : bool = False):
    """Provide a dict with named tables, the keys will be reused to create variable names in the binded data. 
    Similar effect to rbind in R.
    """
    id_col = ib.union(*[tab.select("h3_id") for tab in list(input.values())],distinct=True)
    
    for (nam,tab) in input.items():
        if "band_var" in tab.columns:
            tab = tab.rename({nam : "band_var"})
            id_col = id_col.join(tab,how="left",predicates="h3_id",rname="{name}_var").rename({nam+"_var" : nam}).drop(s.matches("h3_id_var"))
        else :
            id_col = id_col.join(tab,how="left",predicates="h3_id",rname="{name}_var").drop(s.matches("h3_id_var"))
    
    id_col_vars = id_col.select(s.matches("_var$")).columns

    return conn.create_table(obj=id_col.fill_null({col : 0 for col in id_col_vars}),name=name,overwrite=overwrite)#