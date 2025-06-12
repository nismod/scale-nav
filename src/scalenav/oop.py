"""Module sketching out the OO backbone. This is intended to reduce the complexity of the user facing side of the workflow by wrapping the underlying functions
into methods associated to objects that have a defined structure and therefore are more failure robust."""

# from pathlib import Path
import os 
from ibis import table,to_sql,_,duckdb
import ibis.selectors as s
from ibis.backends.duckdb import *
import ibis as ib
import re
from numpy import random,all,unique
from duckdb import connect
from scalenav.utils import *


configs = {
    "memory_limit" : "500MB"
}

################

class Layer(ib.Table): 

    res : int
    src : str

    def __init__(self, name : str = "", src : str = "") -> None:
        """Validation of the input source goes here. What is the schema ? """
        
        super().__init__()

        self.src = src
    
    def _factory(self, expr):
        # Override the factory to return Layer
        return Layer(expr)

    def project(self, resolution : int = 10, columns : tuple[str] = ("lon","lat"),**kwargs) -> None: # type: ignore
        """Project the data onto the h3 grid at the given resolution. 
        The column containing the spatial variables to use can be provided
        as argument in the sptaial_var paramater if it is not ('lon','lat').
        """
        self.res = resolution
        self.table = project(self.table, res=resolution,columns=columns,**kwargs)
        return self
        
    def head(self,n : int = 5):
        """Wrapper around the head function."""
        head(self,n=n)


    def change_res(self,levels,transforms = None,**kwargs):
        """Change resolution by a fixed 'delta' value. 
        Can be a positive of negative integer. The resulting resolution 
        will be equal to the current one plus the delta value. 
        The restriciton is that the final resolution has to be 
        within the accepted limits. 
        """
        return change_res(self,levels=levels,transform_expr=transforms)
        

    def set_res(self):
        """Set the reolution of a layer to a given one as target. 
        If no column containing h3_ids is present, 
        project the data to that resolution.
        """
        pass
    
    def restore(self):
        """Reloads the original data from the 'src' attribute. 
        Usefull to reset the layer after some manipulations have 
        been done and information potentially lost. 
        Overwrites the current state of the layer ?
        """
        pass

    def add_centr(self):
        return add_centr(self)
    
    def add_geom(self,column = "h3_id"):
        # add_centr
        # add_geom
        return add_geom(self, column=column)

    def reindex(self,names_from=None,values_from="id",values_agg="count",values_fill=0,**kwargs,):
        # reindex
        return reindex(self, names_from=names_from,values_agg=values_agg,values_from=values_from,values_fill=values_fill,**kwargs)

    def rescale(self, weight : ib.Table,weight_var : str = "weight_var", weight_geom : str = "geometry", weight_id : str = "id", keep_left : str = None ,keep_right : str = None):
        # rescale
        return rescale(self, weight, weight_var, weight_geom, weight_id, keep_left,keep_right)

    def join(self,table2,predicates : list = ["h3_id"],how="outer"):
        return join(self,table2,predicates,how)

    def constrain(self,):
        pass
    
    def voronoi(self,):
        pass

    def plot(self,):
        pass

geom_column = ["x","lon","lng","longitude","y","lat","ltd","latitude","geom","geometry"]

# class DataLayer(Layer):
#     def __init__(self) -> None:
#         super().__init__()

#############

class ScalenavProcess(Backend):
    
    src_db : str

    def __init__(self,src_db = ':memory:',**kwargs) -> None:
        """Initiate a duckDB connection with the spatial and H3 extensions.
        """

        super().__init__()

        self.src_db = src_db
        # self.con = connect(database=src_db,**kwargs)
        
        # Backend.__init__(self)

        if src_db in ["memory", ':memory:', "mem"]:
            print("Connecting to a temporary in-memory DB instance.")
            self.do_connect()
        elif os.path.exists(src_db):
            print("connecting to existing database.")
            self.do_connect(src_db)
        else :
            print("Creating database at'",src_db,"'.")
            with connect(src_db) as con:
                pass
            self.do_connect(src_db)
        
        self.raw_sql("""
        INSTALL spatial; 
        LOAD spatial;
        INSTALL h3 FROM community;
        LOAD h3;
        """)

    def add_table(self,name : str,path : str):
        return table(conn=self,name = name, path = path)

    def export() -> None:
        """Save the underlying duckDB data base in it's current state. It contains the table schemas for future use.
        """
        pass

    def combine(self, name: str, layers : dict = {}):
        return combine(conn=self,name=name,input=layers)
        
    def concat(self,**kwargs):
        return concat(**kwargs)

    def create_table(self,name : str, obj : str,**kwargs): # -> ib.Table:
        return Layer.__class__(super().create_table(name,obj=obj,**kwargs).op())
        
         

##########

def head(input : ib.Table,n : int = 5):
    print(input.count())
    print(input.head(n))

def connect(database = ':memory:',interactive : bool = True,**kwargs):
    """Create a duckDB connection with spatial and H3 extensions loaded.
    For details on the additional parameters, refer to : https://duckdb.org/docs/stable/configuration/overview.html

    Parameters
    ----------------

    database : str
        a str path to a duckdb data base, or any of ['memory, ':memory:', 'mem'] to create a temporary in memory instance in the current session.

    interatvie : bool
        to work on in a jupyter notebook

    kwargs : dict
        other parameters to the duckDB backend, such as memory limit, nounber of cores

    Returns
    --------------------
    ibis.Table
        A ibis/duckDB connection object. 

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

def table(conn,name : str,path : str,coords = ["lon","lat"],*args,**kwargs):
    """Create a table in the database out of a parquet file.
    This function 

    Parameters
    -----------------
    conn : ibis.backends.duckdb.Backend
        an ibis connection to a duckDB database into which the table should be added
    name : str
        a name for the table, good practice is to give the same name as the python variable
    path : str 
        a path to a parquet file or folder with parquet files with the data
    coords : list[str]
        the column names containing the coordinates
    kwargs : dict
        some additional arguments can be added, see details

    Notes
    ---------------
    The function support an additional keyword argument at the moment. You can provide a `bbox` to filter values from the data. The format for the parameter is [xmin,ymin,xmax,ymax]. 
    Another parameter is `overwrite` to overwirte an existing backend table with the same name.


    Returns
    ----------------
    ibis.Table
        An ibis table object connected to the backend table. 

    """
    
    q_startup = "create"

    if "overwrite" in kwargs.keys():
        if kwargs["overwrite"]:
            print("Overwriting existing.")
            q_startup = "create or replace"
        else:
            if name in conn.list_tables():
                return conn.table(name)

    if "bbox" in kwargs.keys():
        print("Reading bbox")
        conn.raw_sql(f"{q_startup} table {name} as (select * from '{path}' where {coords[0]}>{kwargs["bbox"][0]} AND {coords[0]}<{kwargs["bbox"][2]} AND {coords[1]}>{kwargs["bbox"][1]} AND {coords[1]}<{kwargs["bbox"][3]});")
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

def project(input : ib.Table,res : int = 8, columns : [tuple,None] = None, keep = True, for_gridding : bool = False) -> ib.Table: # type: ignore
    """Given an ibis table with coordinates columns,
    return a table with a new column resulting from generating h3 ids for the points at given h3 resolution.

    Parameters
    --------------
    input : 
        an ibis table
    res : 
        the resolution to project
    columns : 
        the columns containing the coordinates if they have unusual names. Anything like lon, y, easting should be automatically detected.
    keep : 
        keep the original coordinates columns in the output

    Returns
    --------------
    ib.Table
        An ibis table

    """

    if "h3_id" in input.columns:
        print("Existing h3_id column will be overwritten")
        input = input.drop("h3_id")
        # print("H3 column exists.")
        # return input

    alias_name = alias_generator()

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


def change_res(input : ib.Table,levels : int = 1,transform_expr : dict = None) -> ib.Table :
    """Projecting an H3 indexed table through scales in the DuckDB backend.
        
        Parameters
        --------------
        input : 
            a table with an `h3_id` column containing the index values. 
        levels : 
            the number of resolutions to change. positive numbers mean increasing, negative decreasing
        transform_expr : 
            A dictionary with column names as keys and functions to perform in values. See details for more...

        Notes
        --------------
        The transformation expression gives great flexibility on the actions to be performed on the columns of the table. It needs to be done in ibis. 
        Refer to the docs for deeper explanation : https://ibis-project.org/reference/expression-generic
        For negative `levels` values, the expression needs to perdofm an aggregation. For positive `levels`, it needs to be a mutation that is applied to the old values of each column.
        
        Returns
        ---------------
        ib.Table
            A downscaled ibis table where the columns identified with a `_var` suffix have been aggregated or disaggregated depending on the direction of the scale change.
        
    
    """

    # generate an alias to expose the tables in the back
    alias_code = alias_generator()

    try:
        levels = int(levels)
    except:
        raise Warning("levels must be integer.")
    
    # need to be already projected.
    if "h3_id" not in input.columns:
        raise Warning("Project data into h3 first with `project`.")
    
    res = (
        input
        .alias(alias_code)
        .sql(f"Select h3_get_resolution(h3_id) as h3_res from {alias_code} limit 1;")[0]
        .as_scalar()
        .execute()
    )

    if levels>0:
        
        rescale_factor = 7**(-levels)
        # transformation expressions for navigating scales 
        if transform_expr is None:
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
    
    elif levels<=0:
        
        if transform_expr is None:
            transform_expr = { x : ( _[x].sum() ) for x in input.columns if re.search(pattern="_var$",string=x)}
        
        return (
            input
            .alias(alias_code)
            .sql(f"""Select * EXCEPT (h3_id), h3_cell_to_parent(h3_id,{res+levels}) as h3_id from {alias_code};""")
            .group_by("h3_id")
            .agg(**transform_expr)
            
)


def add_centr(input : ib.Table):
    """Add the centroid of a hex cell in the table expression.
    
    Parameters
    ---------------
    input : ibis.Table
        A table in the scalenav database.

    Returns
    ----------------
    ib.Table    
        The table with a new column called `geom` containing centroids of cells from the `h3_id` column

    """
    # generate an alias to expose the tables in the back
    alias_code = alias_generator()

    return (input
            .alias(alias_code)
            .sql(f"""Select * EXCLUDE latlng, ST_POINT(latlng[2],latlng[1]) as geom
                    FROM
                        (SELECT *, h3_cell_to_latlng(h3_id) as latlng FROM {alias_code});""")
)

def add_geom(layer, column="h3_id"):
    
    alias = alias_generator()
    
    return layer.alias(alias).sql(f"""SELECT *, ST_GeomFromText(h3_cell_to_boundary_wkt({column}))::GEOMETRY as geometry from {alias};""")



def reindex(input : ib.Table,names_from=None,values_from="id",values_agg="count",values_fill=0,) : 
    """Transpose a variable containing categories associated to located data into an H3 
    indexed table with new columns based on the categorical values. This performs a pivot. This is essentially a pivot that will be applied to a table in a way to 
    make the h3_id column the index and a categorical variable associated to geolocated features into the columns of the output. 
    Values are aggregated by counting by default, but a function expression can be provided or another common function name as string.
    

    Returns
    ----------- 
    A table in which the h3_id column acts as an standard data table index.

    """
    
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

def rescale(input : ib.Table, weight : ib.Table,weight_var : str = "weight_var", weight_geom : str = "geometry", weight_id : str = "id", keep_left : str = None ,keep_right : str = None) -> ib.Table : 
    """This function takes two layers, assuming the secong parameter contains the layer with data to downscale. Therefore the first layer is supposed to be spatially more granular.
    It intersects them and rescales the values from the second layer by weighting according to the first layer.
    
    """

    if "h3_id" in input.columns: 
        input = add_centr(input=input)
        
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

def concat(input,**kwargs):
    """Stack and regroup a set of layers into 1. Similar effect to concat in pandas.

    Parameters
    ----------
    input : A dict of layers

    Returns
    -------
    Layer 
        One combined layer of the inputs stacked
    """    
    
    if all(["h3_id" in tab.columns for tab in list(input.values())]):
        return ib.union(*list(input.values()),**kwargs).group_by("h3_id").agg()
    return ib.union(*list(input.values()),**kwargs)


def combine(conn, input : dict[str,ib.Table], name : str, overwrite : bool = False, fill_val : float = 0):
    """Provide a dict with named tables, the keys will be reused to create variable names in the binded data. 
    Similar effect to rbind in R.

    Parameters
    ----------
    conn : ibis backend connection object, DuckDB recommended
        A connection object
    input : dictionary of ibis tables
        A dictionary containing the tables used in the process.
    name : str
        The name of the new table that will be created in the backend 
    overwrite : bool, optional
        overwrite a table with the same name if it already exists, by default False
    fill_val : float
        A value to fill the null cells resulting from the outer join

    Returns
    -------
    ib.Table
        An ibis table which is an outer merge of all the provided tables

    """    
    id_col = ib.union(*[tab.select("h3_id") for tab in list(input.values())],distinct=True)
    
    for (nam,tab) in input.items():
        if "band_var" in tab.columns:
            tab = tab.rename({nam : "band_var"})
            id_col = id_col.join(tab,how="left",predicates="h3_id",rname="{name}_var").rename({nam+"_var" : nam}).drop(s.matches("h3_id_var"))
        else :
            id_col = id_col.join(tab,how="left",predicates="h3_id",rname="{name}_var").drop(s.matches("h3_id_var"))
    
    id_col_vars = id_col.select(s.matches("_var$")).columns

    return conn.create_table(name,obj=id_col.fill_null({col : fill_val for col in id_col_vars}),overwrite=overwrite)#



def constrain(layer, constraint):
    """UNDER DEV

    Parameters
    ----------
    layer : _type_
        _description_
    constraint : _type_
        _description_
    """    
    pass

# def voronoi(layer, limit):
#     pass


def voronoi(layer, limit=10): 
    """UNDER DEV

    Parameters
    ----------
    layer : _type_
        _description_
    limit : int, optional
        _description_, by default 10

    Returns
    -------
    _type_
        _description_
    """    
    alias = alias_generator()
    h3_fill = layer.select("h3_id").alias(alias).sql(f"SELECT h3_grid_disk(h3_id,10) as h3_gridded from {alias};").select("h3_gridded").unnest("h3_gridded").distinct()
    # geom_voronoi = 
    for j in range(limit):
        if h3_fill.count().execute()>0:
            layer_int = layer.select("h3_id").alias(alias).sql(f"SELECT h3_id, h3_grid_disk(h3_id,{j}) as h3_gridded from {alias};").unnest("h3_gridded").distinct(on="h3_gridded")
            h3_fill = h3_fill.filter(~_.h3_gridded.isin(layer_int.h3_gridded))
        else :
            break
    return layer_int.group_by("h3_id").agg(h3_gridded=_.h3_gridded.collect())



def join(table1,table2,predicates : list = ["h3_id"],how="outer"):
    """Nicer way to outer merge two tables than proposed by ibis. 
    Essentially remove the double columns corresponding to the original predicates.

    Parameters
    ----------
    table1 : _type_
        _description_
    table2 : _type_
        _description_
    predicates : list, optional
        _description_, by default ["h3_id"]
    how : str, optional
        _description_, by default "outer"

    Returns
    -------
    _type_
        _description_
    """    
    
    if how=="inner":
        # if inner do the same.
        return ib.join(table1,table2,predicates=predicates,how="inner")
    # otherwise do this
    return (
        ib.join(table1,table2,predicates=predicates,how=how)
        .mutate(**{val : ib.ifelse(_[val].isnull(),_[val + "_right"],_[val]) for val in predicates})
        .select(~s.c(*[f"{val}_right" for val in predicates]))
        ) 


def knn(input : [ib.Table,Layer], k : int = 1,colname = None):
    """

    Parameters
    ----------
    input : ib.Table,Layer]
        _description_
    k : int, optional
        _description_, by default 1
    colname : _type_, optional
        _description_, by default None

    Returns
    -------
    _type_
        _description_

    Raises
    ------
    IndexError
        _description_
    """    
    
    if "h3_id" not in input.columns:
        raise IndexError("h3_id column not found.")
    if colname is None:
        colname = f"h3_id_{k}nn"
    knn_alias = alias_generator()
    return input.alias(knn_alias).sql(f"""
    Select *, h3_grid_disk(h3_id,{k}) as {colname} from h3_disk;
    """)
