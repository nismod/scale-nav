# test_rast_converter.py
import ibis as ib
# this will be a pytest workflow
import pytest
from scalenav.rast_converter import rast_converter

@pytest.fixture
def raw_files():
    [f"data/test_data_{i}.tif" for i in range(3)]
    return 

@pytest.fixture(scope="session")
def rast_ingest(tmp_path_factory):
    
    files = raw_files()
    
    for i,file in enumerate(files):
        in_file = "data" / file
        out_file = tmp_path_factory+f"/test_data_{i}.parquet"
        rast_converter(in_path=in_file,out_path=out_file)

# sample_file = f"../../data/test_data_{i}.parquet"

def test_completion():
    """Successful completion with the sample data set.
    """
    pass

def test_format():
    """Recognition of different raster input formats.
    """
    pass

def test_output_dim():
    """Ouput dimensions.
    """
    pass

def test_output_meta():
    """The schema of the output. Ideally stick the raster grid size in here somewhere. 
    """
    pass

def test_input():
    """Validity of input. Folder with files.
    What to do if crs not found ? Have parameter set to None by daufault. 
    """
    pass


