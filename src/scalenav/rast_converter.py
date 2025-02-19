"""Conversion of rasters into tables. This module provides a command line tool and a function that both *ingest* a raster file and produce a 3 column parquet 
table with coordinates and band value.

See the data_ingestion notebook for templates of this.
"""

import argparse
from os.path import exists,isdir,isfile
from re import search
from glob import glob
from numpy import meshgrid,arange,array,nan,dtype

from pandas import DataFrame
from pyproj import Transformer

from rasterio.transform import xy
from rasterio import open
from rasterio.vrt import WarpedVRT
from rasterio.io import DatasetReader
from rasterio.crs import CRS

from pyarrow import float32,float16,schema,field,uint16,table,Table,from_numpy_dtype
from pyarrow.parquet import ParquetWriter

from tqdm import tqdm

def check_path(in_path : str) :
      """What are we checking ?
      UNDER DEV
      - input contains one of the desired file resolutions. 
      - if folder file, extract all raster format files from it.
      - if specific file, then just use that. 
      - some robustness to user input should be embedded here. For example when providing folder path: '/the/folder/with/rast/' and /the/folder/with/rast' as two potential accepted values.
      - also relative and absolute paths.
      """
      # check if parameter is directory, else treat it as endpoint to a tiff. 
      # if it's a list, assume mix of folders and endpoints, therefore tranform it into a list of endpoints only.  
      
      if isfile(in_path):
            return in_path if search(pattern = r"(.ti[f]{1,2}$)|(.nc$)", string = in_path) else ""

      if isdir(in_path):
            if in_path[len(in_path)-1]!= '/':
                  in_path=in_path + '/'

      return [str(x) for x in glob(in_path + "**", recursive=True) if search(pattern = r"(.ti[f]{1,2}$)|(.nc$)", string = x)]
      
def check_nodata(source : DatasetReader):
      """NOT much to check actually
      """
      return source.nodatavals[0]

def check_crs(source : DatasetReader, in_crs : str): 
      """
      """
      return source.crs if source.crs is not None else in_crs

def infer_dtype(source : DatasetReader):

      np_dtype = source.dtypes[0]
      try:
            return from_numpy_dtype(dtype(np_dtype)) # the numpy function
      except:
            return float32()

def rast_convert_core(src, transform, win = None, band : int = 1):
      """ The core of the rast conversion can be put here 
       in order to centralise the most efficient workflow 
       that can be then used in the CL tool and function.
      """

      if win is not None:
            band_ = src.read(indexes = band,window=win)
            height = band_.shape[0]
            width = band_.shape[1]
            cols, rows = meshgrid(arange(width), arange(height))

            xs, ys = xy(
            transform = transform(win),
            rows=rows,
            cols=cols)

      else :
            band_ = src.read(indexes=band)
            height = band_.shape[0]
            width = band_.shape[1]
            cols, rows = meshgrid(arange(width), arange(height))

            xs, ys = xy(
            transform = transform,
            rows=rows,
            cols=cols)

      lons = array(xs)
      lats = array(ys)

      return DataFrame({"band_var" : array(band_).flatten()
                        ,'lon': lons.flatten()
                        ,'lat': lats.flatten()})

#  checking inputs from the cl
def check_out_crs(val):
      try:
            return CRS.from_string(val)
      except:
            return CRS.from_string("epsg:4326")

      
if __name__=="__main__":
      parser = argparse.ArgumentParser(
                        prog='Rast Converter',
                        description='Convert rasters to parquet files efficiently',
                        epilog='')

      parser.add_argument('in_path',
                          nargs="?",
                          help="A path to a file or folder with rasters.",
                          type=str,
                          )
      
      parser.add_argument('--out_path',
                          '-o_p',
                          nargs="?",
                          default='rast_convert.parquet',
                          help="A '.parquet' file to save into. Will be created or overwriten on execution. Default: %(default)s",
                          type=str,
                          )

      parser.add_argument('--in_crs',
                          '-i_c',
                          nargs='?',
                          default=None,
                          help="CRS of the input if it is not providee in the data. Default: %(default)s",
                          type=str,
                          )
      
      parser.add_argument('--out_crs',
                          '-o_c',
                          nargs='?',
                          default=None,
                          help="The crs for the output. Default: %(default)s",
                          type=str,
                          )
      
      parser.add_argument('--include_negative',
                          '-i_n',
                          nargs='?',
                          default=False,
                          help="Whether the data to process includes relevant negative or 0 values. Can have a significant impact on running time and output size.",
                          type=bool
                          )

      args = vars(parser.parse_args())

      in_path = args["in_path"]
      in_crs = args["in_crs"]
      out_path = args["out_path"]
      out_crs = args["out_crs"] # epsg:4326 by default.
      include = args["include_negative"] # exclude non positive values by default
      
      if not search(pattern=r".parquet$",string=out_path):
                        raise ValueError("Provide a 'parquet' filename to write the outputs.")


      if out_crs is not None:
            out_crs = check_out_crs(out_crs)
      else : 
             out_crs=in_crs

      print("Output CRS : ",str(out_crs))

      vrt_options = {
            # 'resampling': Resampling.cubic,
            'crs': out_crs,
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
                        # check the source and get the needed info once for user down the road. 
                        src_crs = check_crs(src,in_crs=in_crs)
                        print("Input CRS : ", src_crs)

                        nodata = check_nodata(src)
                        print("No data value : ", nodata)
                        with WarpedVRT(src, **vrt_options) as vrt:
                              
                              # At this point 'vrt' is a full dataset with dimensions,
                              # CRS, and spatial extent matching 'vrt_options'.
                              # Process the dataset in chunks.  Likely not very efficient.
                              
                              win_transfrom = vrt.window_transform

                              for _, window in tqdm(vrt.block_windows()):

                                    out = rast_convert_core(src=vrt,transform=win_transfrom,win=window,
                                                            # nodata=nodata,include=include
                                                            )

                                    out.drop(index=out.loc[out.band_var==nodata].index,inplace=True)
                                    out.dropna(inplace=True)

                                    if not include:
                                          out.drop(index=out.loc[out.band_var<=0].index,inplace=True)

                                    if out.shape[0]!=0:
                                          writer.write_table(Table.from_pandas(df=out,schema = rast_schema,preserve_index=False,safe=True))