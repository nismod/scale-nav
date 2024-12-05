# test_rast_converter.py
# this will be a pytest workflow
import pytest
import numpy as np

import rasterio
from rasterio import open
from rasterio.transform import from_origin
from rasterio.crs import CRS

from pandas import DataFrame

from scalenav.rast_converter import rast_converter, rast_convert_core, check_nodata, check_crs, check_path



# Directory to save rasters
output_dir = "data"

# Create the output directory if it doesn't exist
import os
os.makedirs(output_dir, exist_ok=True)

# Function to create a 6x6 raster with specified nodata and transform
def create_raster(filename, data, nodata_value, crs, transform,blockxsize=16,blockysize=16):
    with rasterio.open(
        filename,
        "w",
        driver="GTiff",
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype=data.dtype,
        crs=crs,  # WGS84
        transform=transform,
        nodata=nodata_value,
        tiled=True,  # Enable tiling
        blockxsize=blockxsize,
        blockysize=blockysize,
    ) as dst:
        dst.write(data, 1)

# Define a 6x6 grid of data with examples of nodata values
nodata_values = [np.nan, -9999, -1, 0, 9999, 255]

crs_values = ["epsg:4326", "epsg:4326", None, "epsg:4326", "epsg:4326", "epsg:4326",]

transforms = [
    from_origin(10, 50, 0.1, 0.1),  # Top-left corner (10, 50), cell size 0.1 x 0.1
    from_origin(-120, 35, 0.01, 0.01),
    from_origin(100, -45, 0.5, 0.5),
    from_origin(-75, 80, 0.05, 0.05),
    from_origin(25, 10, 0.2, 0.2),
    from_origin(-60, -15, 1.0, 1.0),
]

filenames = [os.path.join(output_dir, f"raster_{i+1}.tif") for i in range(0,len(nodata_values))]

# Generate 6 rasters with different nodata values and transforms
for transform, no_data, filename,crs_ in zip(transforms,nodata_values,filenames,crs_values):
    # Create random 6x6 data with the nodata value included
    data = np.random.randint(1, 100, (32, 32)).astype(float)
    if np.isnan(no_data):  # If nodata is NaN, set some cells to NaN
        data[1, 1] = np.nan
    else:  # Otherwise, use the specified nodata value
        data[2, 3] = no_data
    # Save the raster
    create_raster(filename, data, no_data, crs_, transform)

print(f"Rasters saved in '{output_dir}' directory.")

@pytest.fixture
def raw_files():
    [f"data/test_data_{i}.tif" for i in range(3)]
    return 

@pytest.fixture(scope="session")
def rast_ingest(tmp_path_factory: pytest.TempPathFactory):
    
    files = raw_files()
    
    for i,file in enumerate(files):
        in_file = "data" / file
        out_file = tmp_path_factory+f"/test_data_{i}.parquet"
        rast_converter(in_path=in_file,out_path=out_file)

# sample_file = f"../../data/test_data_{i}.parquet"

def test_path():
    path = "./data/"
    in_paths = check_path(path)
    assert len(in_paths)==9
    
    path_incomplete = "./data"
    in_paths = check_path(path_incomplete)
    assert len(in_paths)==9

def test_read_nodata():
    """Successful completion with the sample data set.
    """
    for file,nodat in zip(filenames,nodata_values):
        with open(file) as src:
            nodata = check_nodata(src)
            if np.isnan(nodata): 
                pass
            else :
                assert nodata==nodat

def test_read_crs():
    """Recognition of different raster input formats.
    """
    for file,nodat in zip(filenames,nodata_values):
        with open(file) as src:
            src_crs = check_crs(src)
            assert src_crs=="epsg:4326"


def test_output_dim():
    """Ouput dimensions.
    """
    for  filename in filenames:
        with open(filename) as src:
            transform = src.transform
            nodata = src.nodatavals

            # band1 = src.read()
            out = rast_convert_core(src,transform=transform)

            assert out.shape==(32*32,3)

            out.drop(index=out.loc[out.band_var==nodata].index,inplace=True)
            out.dropna(inplace=True)

            assert out.shape==(32*32-1,3)

# def test_input():
#     """Validity of input. Folder with files.
#     What to do if crs not found ? Have parameter set to None by default. 
#     """
#     pass