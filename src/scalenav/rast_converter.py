"""Conversion of rasters into tables. This module provides a command line tool and a function that both *ingest* a raster file and produce a 3 column parquet 
table with coordinates and band value.

See the data_ingestion notebook for templates of this.
"""

import argparse
from os.path import exists
from numpy import meshgrid,arange,array,nan
from rasterio.transform import xy
from rasterio import open
from rasterio.vrt import WarpedVRT
from pandas import DataFrame
from re import search
from pyarrow import float32,float16,schema,field,uint16,table,Table
from pyarrow.parquet import ParquetWriter
from glob import glob
from pyproj import Transformer
from tqdm import tqdm

def check_path(in_path):
       """What are we checking ?
       - input contains one of the desired file resolutions. 
       - if folder file, extract all raster format files from it.
       - if specific file, then just use that. 
       - some robustness to user input should be embedded here. For example when providing folder path: '/the/folder/with/rast/' and /the/folder/with/rast' as two potential accepted values.
       - also relative and absolute paths.

       """

       if in_path[len(in_path)-1]!= '/':
              in_path=in_path+'/'
    
       in_paths = [str(x) for x in glob(in_path + "**", recursive=True) if search(pattern = r"(.ti[f]{1,2}$)|(.nc$)", string = x)]

      #  print("Reading in from",len(in_paths), "files.")
       
       return in_paths

def check_nodata(source):
      """
      """
      if len(source.nodatavals)>1: 
            print("Using first no data value")
            return source.nodatavals[0]
      else : 
            return source.nodatavals[0]
      # return source.nodatavals

def check_crs(source): 
      """
      """
      return source.crs if source.crs is not None else "epsg:4326"

def rast_convert_core(band, transform, win = None):
      """ The core of the rast conversion can be put here 
       in order to centralise the most efficient workflow 
       that can be then used in the CL tool and function.
      """

      height = band.shape[1]
      width = band.shape[2]
      cols, rows = meshgrid(arange(width), arange(height))

      if win is not None:
            xs, ys = xy(
            transform = transform(win),
            rows=rows,
            cols=cols)
      else :
            xs, ys = xy(
            transform = transform,
            rows=rows,
            cols=cols)

      lons = array(xs)
      lats = array(ys)

      out = DataFrame({"band_var" : array(band).flatten()
                        ,'lon': lons.flatten()
                        ,'lat': lats.flatten()})

      return out



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
        
        in_paths = check_path(in_path=in_path)

        if len(in_paths)==0: 
            raise IOError("No input files recognised.")

        print("Reading the following file(s): ",*in_paths)

        with ParquetWriter(out_path, rast_schema) as writer:
            with open(in_path) as src:
                  
                  src_crs = src.crs
                  print("Detected source crs : ", src_crs)

                  win_transfrom = src.window_transform
                  
                  # transformer = Transformer.from_crs(str(src_crs), 'EPSG:4326', always_xy=True)
                  
                  print("No data : ", src.nodatavals)


                  print("No data value : ", nodata)
                  

                  # Process the dataset in chunks.  Likely not very efficient.
                  for ij, window in src.block_windows():

                        band1 = src.read(window=window)
                        out = rast_convert_core(band1,win_transfrom,window,nodata=nodata,include=False)

                        out.drop(index=out.loc[out.band_var==nodata].index,inplace=True)
                        out.dropna(inplace=True)

                        if not include:
                              out.drop(index=out.loc[out.band_var<=0].index,inplace=True)

                        if out.shape[0]!=0:
                                    writer.write_table(Table.from_pandas(df=out,schema = rast_schema,preserve_index=False,safe=True))
      
if __name__=="__main__":
      
      parser = argparse.ArgumentParser(
                        prog='Rast Converter',
                        description='Convert rasters to parquet files',
                        epilog='')

      parser.add_argument('in_path',
                          nargs=1,
                          help="A path to a file or folder with rasters.",
                          type=str,
                          )
      
      parser.add_argument('out_path',
                          nargs=1,
                          default='rast_convert.parquet',
                          help="A '.parquet' file to save into. Will be created or overwriten on execution. Default: %(default)s",
                          type=str,
                          )
      
      parser.add_argument('--out_crs',
                          "-ocrs",
                          nargs=1,
                          required=False,
                          default='EPSG:4326',
                          help="A crs value for the output.",
                          type=str
                          )
      
      parser.add_argument('--include_negative',
                          "-in",
                          nargs=1,
                          required=False,
                          default=False,
                          help="Whether the data to process includes relevant negative or 0 values. Can have a significant impact on running time and output size.",
                          type=bool
                          )
      
      # parser.add_argument('-v', '--verbose',
      #                     action='store_true') 

      args = vars(parser.parse_args())

      in_path = args["in_path"][0]
      out_path = args["out_path"][0]
      dst_crs = args["out_crs"] # epsg:4326 by default.
      include = args["include_negative"] # exclude non positive values by default

      # print(args)

      if not search(pattern=r".parquet$",string=out_path):
                        raise ValueError("Provide a 'parquet' filename to write the outputs.")

      vrt_options = {
            'crs': dst_crs,
      }

      rast_schema = schema([('lon',float16())
            ,('lat',float16())
            ,('band_var',float32())
            ])
      rast_schema.with_metadata({
            "lon" : "Longitude coordinate",
            "lat" : "Latitude coordinate",
            "band_var" : "Value associated",
                              })


      if in_path[len(in_path)-1]!= '/':
                in_path=in_path+'/'
    
      in_paths = [str(x) for x in glob(in_path + "**", recursive=True) if search(pattern = r"(.ti[f]{1,2}$)|(.nc$)", string = x)]

      print("Reading in from ",len(in_paths), " files.")

      # in_paths = check_paths(in_path)

      if len(in_paths)==0: 
            raise IOError("No input files recognised.")

      # add clock using https://tqdm.github.io/docs/tqdm/
      
      with ParquetWriter(out_path, rast_schema) as writer:
            for path in in_paths:
                  with open(path) as src:

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
                              # Process the dataset in chunks.  Likely not very efficient.
                              
                              win_transfrom = vrt.window_transform

                              for _, window in tqdm(vrt.block_windows()):

                                    band1 = vrt.read(window=window)
                                    
                                    # height = band1.shape[1]
                                    # width = band1.shape[2]
                                    # cols, rows = meshgrid(arange(width), arange(height))

                                    # xs, ys = xy(
                                    #       transform = win_transfrom(window),
                                    #       rows=rows,
                                    #       cols=cols)

                                    # lons = array(xs)
                                    # lats = array(ys)
                                    
                                    # out = DataFrame({"band_var" : array(band1).flatten()
                                    #                         ,'lon': lons.flatten()
                                    #                         ,'lat': lats.flatten()})
                                    
                                    # out.drop(index=out.loc[out.band_var==nodata].index,inplace=True)
                                    # out.dropna(inplace=True)

                                    # if not include:
                                    #       out.drop(index=out.loc[out.band_var<=0].index,inplace=True)

                                    out = rast_convert_core(band=band1,transform=win_transfrom,win=window,nodata=nodata,include=include)

                                    out.drop(index=out.loc[out.band_var==nodata].index,inplace=True)
                                    out.dropna(inplace=True)

                                    if not include:
                                          out.drop(index=out.loc[out.band_var<=0].index,inplace=True)

                                    if out.shape[0]!=0:
                                          writer.write_table(Table.from_pandas(df=out,schema = rast_schema,preserve_index=False,safe=True))
