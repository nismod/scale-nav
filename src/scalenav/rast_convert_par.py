# rast_convert_par.py

import itertools
import os 
from glob import glob
from re import search

from rasterio.windows import Window
from rasterio.vrt import WarpedVRT
from rasterio.crs import CRS
from rasterio import open

import numpy as np
from pandas import DataFrame

import concurrent.futures

from pyarrow import float32,schema,Table
from pyarrow.parquet import ParquetWriter

from scalenav.rast_converter import rast_convert_core,check_crs,check_nodata,check_path,infer_dtype,check_out_crs

import argparse

#####

#### input and output options

dst_crs = CRS.from_epsg(4326)

##### Process 

# src_file = check_path(src_file)

def process(
            out_file : str,
            src_file : str,
            windows : itertools.batched,
            rast_schema : dict,
            vrt_options : dict,
            nodata : tuple | float | int,
            include : bool = False,
            band : int = 1
            ):
      
      print("Writing into : ",out_file)

      with open(src_file) as src:
                  
            with WarpedVRT(src, **vrt_options) as vrt:

                  win_transfrom = vrt.window_transform

                  with ParquetWriter(out_file, rast_schema) as writer: 
                        
                        for window in windows:

                              out = rast_convert_core(vrt,transform=win_transfrom,win=window,band=band)
                              out.drop(index=out.loc[out.band_var==nodata].index,inplace=True)

                              if not include:
                                    out.drop(index=out.loc[out.band_var<=0].index,inplace=True)
                              
                              if out.shape[0]>0:
                                    writer.write_table(Table.from_pandas(df=out,schema = rast_schema,preserve_index=False))


# if __name__=="__main__":

def rast_converter(args=None):

      parser = argparse.ArgumentParser(
                        prog='Rast Converter',
                        description='Convert rasters to parquet files efficiently',
                        epilog='')

      parser.add_argument('in_path',
                          nargs='?',
                          help="A path to a raster for processing.",
                          type=str,
                          )
      
      parser.add_argument('--out_path',
                          '-o_p',
                          nargs='?',
                          default='rast_convert_result',
                          help="A folder to save into. Will be created or overwriten on execution. Default: %(default)s",
                          type=str,
                          )
      
      parser.add_argument('--band',
                          '-bd',
                          nargs = '?',
                          default = 1,
                          help = "The band to read from.",
                          type = int,
                          )
      
      parser.add_argument('--in_crs',
                          '-i_c',
                          nargs = '?',
                          default = None,
                          help = "CRS of the input if not available.: %(default)s",
                          type = str,
                          )
      
      parser.add_argument('--out_crs',
                          '-o_c',
                          nargs='?',
                          default="epsg:4326",
                          help="CRS of the output. Default: %(default)s",
                          type=str,
                          )
      
      parser.add_argument('--include_negative',
                          '-i_n',
                          nargs='?',
                          default=False,
                          help="Whether the data to process includes relevant negative or 0 values. Can have a significant impact on running time and output size.",
                          type=bool
                          )
      
      parser.add_argument('--workers',
                          '-w',
                          nargs='?',
                          default=4,
                          help="The number of workers to run in parallel.",
                          type=int,
                          )

      args = vars(parser.parse_args(args))

      #### Process parameters 
      src_file = args["in_path"]
      out_fold = args["out_path"]
      band = args["band"]
      in_crs = args["in_crs"]
      out_crs = args["out_crs"]
      include = args["include_negative"] # exclude non positive values by default
      num_workers = args["workers"]


      if out_crs is not None:
            out_crs = check_out_crs(out_crs)
      else :
            out_crs = check_out_crs(in_crs)

      print("Output CRS : ",str(out_crs))

      # options here : https://rasterio.readthedocs.io/en/stable/api/rasterio.vrt.html#rasterio.vrt.WarpedVRT
      vrt_options = {
            'crs': out_crs,
      }

      if not os.path.exists(out_fold):
            os.mkdir(out_fold)

      filename = src_file.split("/")[-1].split(".")[0]

      out_files  = ""

      with open(src_file) as src:
            
            # print(src.count)
            if band>src.count:
                  raise IOError("Band parameter exceeds available bands.","Available bands : ",src.count)
            if src.count==1:
                  out_files = [out_fold + "/" + filename + "_" + str(i) + ".parquet" for i in range(num_workers)]
            elif src.count>1:
                  out_files = [out_fold + "/" + filename+ "_band_" + str(band) + "_" + str(i) + ".parquet" for i in range(num_workers)]
            # check the source and get the needed info once for user down the road. 
            src_crs = check_crs(src,in_crs=in_crs)
            vrt_options["src_crs"] = src_crs
            
            print("Input CRS : ", src_crs)
            
            nodata = check_nodata(src)
            print("No data value : ", nodata)

            # windows
            windows = [window for _, window in WarpedVRT(src, **vrt_options).block_windows()]

            # computing individual batch size for each core
            batch_size = int(np.ceil(len(windows)/(num_workers)))
            print("Batch size : ", batch_size)
            # dividing into batches
            batches = itertools.batched(windows,batch_size)
            
            # inferring the dtype
            band_var_dtype = infer_dtype(src)

            print("Variable type infered : ",band_var_dtype)

            # making the raster with the information
            rast_schema = schema([('lon',float32())
                    ,('lat',float32())
                    ,('band_var',band_var_dtype)
                    ])

            rast_schema.with_metadata({
                  "lon" : "Longitude coordinate",
                  "lat" : "Latitude coordinate",
                  "band_var" : "Value associated",
                                    })

      # We map the process() function over the list of
      # windows.
      with concurrent.futures.ProcessPoolExecutor(
            max_workers=num_workers
      ) as executor:
            futures = []
            for (w,v,win) in zip(out_files,[src_file]*num_workers,batches):
                  futures.append(
                        executor.submit(process,w,v,win,rast_schema,vrt_options,nodata,include, band)
                  )
            # Wait for all tasks to complete
            concurrent.futures.wait(futures)