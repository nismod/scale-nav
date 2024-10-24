from pathlib import Path
import os 

configs = {
    "size_lim" : "500MB"
}






class Layer: 

    base_res : int
    current_res : int
    src : str

    def __init__(self) -> None:
        """Validation of the input source goes here. What is the schema ? """
        pass

    def change_res():
        pass

    def set_res():
        pass
    
    def restore():
        pass

    def get_data():
        pass

geom_column = ["x","lon","lng","longitude","y","lat","ltd","latitude","geom","geometry"]

class DataLayer(Layer):
    def __init__(self) -> None:
        super().__init__()