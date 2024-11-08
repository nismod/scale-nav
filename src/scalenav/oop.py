"""Module sketching out the OO backbone. This is intended to reduce the complexity of the user facing side of the workflow by wrapping the underlying functions
into methods associated to objects that have a defined structure and therefore are more failure robust."""

from pathlib import Path
import os 

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