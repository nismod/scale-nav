# import sys
import argparse
from os.path import exists
from numpy import meshgrid,arange,array
from rasterio.transform import xy
from rasterio import open
# from rasterio.warp import re
# import shapely as shp
from pandas import DataFrame
# from math import pi,cos
# import h3
from re import search
from pyarrow import float32,schema,field,uint16,table,Table
from pyarrow.parquet import ParquetWriter
from glob import glob

def rast_converter(in_path, out_path="rast_convert.parquet"):
        
        """ Convert a rater file into a parquet table with at least 3 columns, 2 columns for the coordinates and the remaining for the value bands. The data is written into a external file.
        Also usable as a command line tool in which the function parameters are read from the console. 

        Future imporvements will include automatic recognition of the data epsg and grid cell size to add it into the metadata of the table.

        Parameters
        ------------
        in_path : the path to a folder containing the raster files to be converted. The function will look for files with a .tiff, .tif, .nc extensions. 
        
        out_path : the path to a parquet file into which to write the data.

        """

        if exists(out_path): return print("File exists;")    

        if not search(pattern=r".parquet$",string=out_path):
                    raise ValueError("Provide a 'parquet' filename to write the outputs.")
        
        rast_schema = schema([('lon',float32())
                         ,('lat',float32())
                         ,('band_var',float32())
                         ])

        rast_schema.with_metadata({
              "lon" : "Longitude coordinate",
              "lat" : "Latitude coordinate",
              "band_var" : "Value associated",
                                   })

        in_paths = [x for x in glob(in_path, recursive=True) if search(pattern = r"(.ti[f]{1,2}$)|(.nc$)", string = x)]

        if len(in_paths)==0: 
              raise IOError("No input files recognised.")
        
        print("Reading the following file(s): ",*in_paths)

        with ParquetWriter(out_path, rast_schema) as writer:
            for path in in_paths:
                with open(path) as src:
                        #   have a buffering here that can read and process chunks in parallel. 
                        band1 = src.read(1)
                        height = band1.shape[0]
                        width = band1.shape[1]
                        cols, rows = meshgrid(arange(width), arange(height))
                        xs, ys = xy(transform = src.transform, rows=rows, cols=cols)

                        lons = array(xs)
                        lats = array(ys)
                        
                        if len(src.nodatavals)>1:
                              nodata = src.nodatavals[0]
                        
                        out = DataFrame({"band_var" : array(band1).flatten()
                                                ,'lon': lons.flatten()
                                                ,'lat': lats.flatten()})
                        
                        out.drop(index=out.loc[out.band_var==nodata].index,inplace=True)
                        out.drop(index=out.loc[out.band_var<=0].index,inplace=True)

                        writer.write_table(Table.from_pandas(df=out,schema = rast_schema,preserve_index=False,safe=True))

if __name__=="__main__":

    parser = argparse.ArgumentParser(
                        prog='Rast Converter',
                        description='Convert rasters to parquet files',
                        epilog='')

    parser.add_argument('in_path',help="A path to a folder containing '.tif' files.")
    parser.add_argument('out_path',nargs="?",default='rast_convert.parquet',help="A '.parquet' file to save into. Will be created or overwriten on execution. Default: %(default)s")
    # parser.add_argument('grid_size',nargs="?",default=1000,help="A grid size value for the original rasters.", type=int)

    # parser.add_argument('-v', '--verbose',
    #                     action='store_true') 

    args = vars(parser.parse_args())

    in_path = args["in_path"]
    out_path = args["out_path"]
    # grid_size = args["grid_size"]

    rast_converter(
          in_path=in_path
          ,out_path=out_path
        #   ,grid_size=grid_size
          )