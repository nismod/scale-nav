"""Conversion of rasters into more user friendly tables. This module provides a command line tool and a function that both *ingests* a raster file and produce a 3 column parquet 
table with coordinates and band value.

See the data_ingestion notebook for templates of this. 

add clock using https://tqdm.github.io/docs/tqdm/
"""

import argparse
from os.path import exists
from numpy import meshgrid,arange,array
from rasterio.transform import xy
from rasterio import open
from rasterio.vrt import WarpedVRT
from pandas import DataFrame
from re import search
from pyarrow import float32,float16,schema,field,uint16,table,Table
from pyarrow.parquet import ParquetWriter
from glob import glob
from pyproj import Transformer


def rast_converter(in_path, out_path="rast_convert.parquet"):
        """Convert a rater file into a parquet table with at least 3 columns, 2 columns for the coordinates and the remaining for the value bands. The data is written into a external file.
        Also usable as a command line tool in which the function parameters are read from the console. 

        Future imporvements will include automatic recognition of the data epsg and grid cell size to add it into the metadata of the table.

        If working with large files of several thousand rows or columns, consider using the command line tool to process it. 
      
        Parameters
        ------------

        in_path : the path to a folder containing the raster files to be converted. The function will look for files with a .tiff, .tif, .nc extensions. 
        
        out_path : the path to a parquet file into which to write the data."""
        
        if exists(out_path): return Exception("File exists. Erase first to rewrite.")    

        if not search(pattern=r".parquet$",string=out_path):
                  #  raise ValueError("Provide a 'parquet' filename to write the outputs.")
                  print("Output needs to be a parquet file. Adding parquet extension to provided 'out_path'.")
                  out_path = out_path + ".parquet"

        rast_schema = schema([('lon',float16())
                        ,('lat',float16())
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
            with open(in_path) as src:
                  
                  src_crs = src.crs
                  win_transfrom = src.window_transform
                  
                  transformer = Transformer.from_crs(str(src_crs), 'EPSG:4326', always_xy=True)
                  
                  # print("No data : ", src.nodatavals)

                  if len(src.nodatavals)>1: 
                        nodata = src.nodatavals[0]
                  else : 
                        nodata = src.nodatavals

                  print("No data value : ", nodata)
                  print("Detected source crs : ", src_crs)

                  # Process the dataset in chunks.  Likely not very efficient.
                  for ij, window in src.block_windows():

                        band1 = src.read(window=window)

                        height = band1.shape[1]
                        width = band1.shape[2]
                        cols, rows = meshgrid(arange(width), arange(height))

                        xs, ys = xy(
                              transform = win_transfrom(window),
                              rows=rows,
                              cols=cols)
                        
                        lons,lats = transformer.transform(array(xs),array(ys))
                        
                        out = DataFrame({'lon': lons.flatten(),
                                                'lat': lats.flatten(),
                                                "band_var" : array(band1[0,:,:]).flatten(),
                                                })
                        
                        out.drop(index=out.loc[out.band_var==nodata].index,inplace=True)
                        out.drop(index=out.loc[out.band_var==0].index,inplace=True)
                        
                  if out.shape[0]!=0:
                              writer.write_table(Table.from_pandas(df=out,schema = rast_schema,preserve_index=False,safe=True))
      
if __name__=="__main__":
      
      parser = argparse.ArgumentParser(
                        prog='Rast Converter',
                        description='Convert rasters to parquet files',
                        epilog='')

      parser.add_argument('in_path',help="A path to a folder containing '.tif' files.")
      parser.add_argument('out_path',nargs="?",default='rast_convert.parquet',help="A '.parquet' file to save into. Will be created or overwriten on execution. Default: %(default)s")
      parser.add_argument('dst_crs',nargs="?",default='EPSG:4326',help="A crs value for the output.", type=str)

      # parser.add_argument('-v', '--verbose',
      #                     action='store_true') 

      args = vars(parser.parse_args())

      in_path = args["in_path"]
      out_path = args["out_path"]
      dst_crs = args["dst_crs"] # epsg:4326 by default.

      if not search(pattern=r".parquet$",string=out_path):
                        raise ValueError("Provide a 'parquet' filename to write the outputs.")

      vrt_options = {
            # 'resampling': Resampling.cubic,
            'crs': dst_crs,
            # 'transform': dst_transform,
            # 'height': dst_height,
            # 'width': dst_width,
      }

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
            with open(in_path) as src:

                  src_crs = src.crs

                  if len(src.nodatavals)>1:
                        nodata = src.nodatavals[0]
                  else :
                        nodata = src.nodatavals

                  print("No data value : ", nodata)
                  print("Detected source crs : ", src_crs)

                  with WarpedVRT(src, **vrt_options) as vrt:
                        
                        # At this point 'vrt' is a full dataset with dimensions,
                        # CRS, and spatial extent matching 'vrt_options'.
                        # Read all data into memory.
                        # data = vrt.read()
                        # Process the dataset in chunks.  Likely not very efficient.
                        
                        win_transfrom = vrt.window_transform

                        for _, window in vrt.block_windows():

                              band1 = vrt.read(window=window)
                              
                              height = band1.shape[1]
                              width = band1.shape[2]
                              cols, rows = meshgrid(arange(width), arange(height))

                              xs, ys = xy(
                                    transform = win_transfrom(window),
                                    rows=rows,
                                    cols=cols)

                              lons = array(xs)
                              lats = array(ys)
                              
                              out = DataFrame({"band_var" : array(band1).flatten()
                                                      ,'lon': lons.flatten()
                                                      ,'lat': lats.flatten()})
                              
                              out.drop(index=out.loc[out.band_var==nodata].index,inplace=True)
                              out.drop(index=out.loc[out.band_var<=0].index,inplace=True)

                              if out.shape[0]!=0:
                                    writer.write_table(Table.from_pandas(df=out,schema = rast_schema,preserve_index=False,safe=True))
