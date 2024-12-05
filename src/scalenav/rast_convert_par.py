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

from scalenav.rast_converter import rast_convert_core,check_crs,check_nodata,check_path

import argparse

#####

#### input and output options

dst_crs = CRS.from_epsg(4326)

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

##### Process 

# src_file = check_path(src_file)

def process(
            out_file : str,
            src_file : str,
            windows : itertools.batched,
            rast_schema : dict,
            vrt_options : dict,
            nodata : tuple | float | int,
            include : bool = False
            ):
      
      print("Writing into : ",out_file)

      with open(src_file) as src:
                  
            with WarpedVRT(src, **vrt_options) as vrt:

                  win_transfrom = vrt.window_transform

                  with ParquetWriter(out_file, rast_schema) as writer: 
                        
                        for window in windows:

                              out = rast_convert_core(vrt,transform=win_transfrom,win=window)
                              out.drop(index=out.loc[out.band_var==nodata].index,inplace=True)

                              if not include:
                                    out.drop(index=out.loc[out.band_var<=0].index,inplace=True)
                              
                              if out.shape[0]>0:
                                    writer.write_table(Table.from_pandas(df=out,schema = rast_schema,preserve_index=False))


if __name__=="__main__":


      parser = argparse.ArgumentParser(
                        prog='Rast Converter',
                        description='Convert rasters to parquet files',
                        epilog='')

      parser.add_argument('in_path',
                        #   nargs=1,
                          help="A path to a raster for processing.",
                          type=str,
                          )
      
      parser.add_argument('--out_path',
                        #   nargs=1,
                          default='rast_convert_result',
                          help="A folder to save into. Will be created or overwriten on execution. Default: %(default)s",
                          type=str,
                          )
      
      parser.add_argument('--workers',
                          '-w',
                        #   nargs=1,
                          default=4,
                          help="The number of workers to run in parallel.",
                          type=int,
                          )

      parser.add_argument('--include_negative',
                          "-in",
                        #   nargs=1,
                          default=False,
                          help="Whether the data to process includes relevant negative or 0 values. Can have a significant impact on running time and output size.",
                          type=bool
                          )
      
      args = vars(parser.parse_args())

      #### Process parameters 

      src_file = args["in_path"]
      out_fold = args["out_path"]
      num_workers = args["workers"]
      include = args["include_negative"] # exclude non positive values by default

      if not os.path.exists(out_fold):
            os.mkdir(out_fold)

      out_files = [out_fold + "/" + out_fold + "_" + str(i) + ".parquet" for i in range(num_workers)]

      with open(src_file) as src:
            # check the source and get the needed info once for user down the road. 
            src_crs = check_crs(src)

            print("Using CRS : ", src_crs)

            nodata = check_nodata(src)
            
            print("No data value : ", nodata)

            windows = [window for _, window in WarpedVRT(src, **vrt_options).block_windows()]

            batch_size = int(np.ceil(len(windows)/(num_workers)))

            print("Batch size : ", batch_size)
            
            batches = itertools.batched(windows,batch_size)

      # We map the process() function over the list of
      # windows.
      with concurrent.futures.ProcessPoolExecutor(
            max_workers=num_workers
      ) as executor:
            futures = []
            for (w,v,win) in zip(out_files,[src_file]*num_workers,batches):
                  futures.append(
                        executor.submit(process,w,v,win,rast_schema,vrt_options,nodata)
                  )
            # Wait for all tasks to complete
            concurrent.futures.wait(futures)