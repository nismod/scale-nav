# rast_convert_par.py
import argparse
import concurrent.futures
import gc
import os

from pyarrow import float32, schema
from pyarrow.parquet import ParquetWriter
from rasterio import open
from rasterio.vrt import WarpedVRT

from scalenav.rast_converter import (
    check_crs,
    check_nodata,
    check_out_crs,
    infer_dtype,
    rast_convert_window_to_batch,
    _dbg_log,
)

GC_INTERVAL = 50

writer_args = {
    "write_statistics": False,
}


def _configure_gdal_cache():
    """Bound GDAL block cache per worker to limit memory on HPC."""
    cache_mb = int(os.environ.get("RASTAPAR_GDAL_CACHE_MB", "64"))
    try:
        from osgeo import gdal

        gdal.SetCacheMax(cache_mb * 1024 * 1024)
    except ImportError:
        os.environ["GDAL_CACHEMAX"] = str(cache_mb)


def process(
    worker_id: int,
    num_workers: int,
    out_file: str,
    src_file: str,
    rast_schema,
    vrt_options: dict,
    nodata: tuple | float | int,
    include: bool = False,
    band: int = 1,
):
    """Process a shard of raster block windows and write to a parquet file.

    Each worker iterates all block windows and processes those assigned to it
    via ``block_index % num_workers == worker_id``.
    """
    _configure_gdal_cache()
    print("Writing into : ", out_file)

    _dbg_log(
        "rast_convert_par.py:process:entry",
        "Worker process started",
        {"out_file": out_file, "worker_id": worker_id, "num_workers": num_workers},
        "A",
    )

    processed = 0

    with open(src_file) as src:
        with WarpedVRT(src, **vrt_options) as vrt:
            win_transform = vrt.window_transform

            with ParquetWriter(
                where=out_file, schema=rast_schema, **writer_args
            ) as writer:
                for block_idx, (_, window) in enumerate(vrt.block_windows()):
                    if block_idx % num_workers != worker_id:
                        continue

                    batch = rast_convert_window_to_batch(
                        vrt,
                        transform=win_transform,
                        win=window,
                        rast_schema=rast_schema,
                        band=band,
                        nodata=nodata,
                        include=include,
                    )

                    if batch is not None:
                        writer.write(batch)

                    processed += 1
                    if processed % GC_INTERVAL == 0:
                        gc.collect()

                    _dbg_log(
                        "rast_convert_par.py:process:after_write",
                        "Window processed",
                        {
                            "block_idx": block_idx,
                            "processed": processed,
                            "win_height": getattr(window, "height", 0),
                            "win_width": getattr(window, "width", 0),
                        },
                        "D",
                    )


def rast_converter(args=None):
    """The core function of the CLI tool.

    Parameters
    ----------
    args : optional
        by default None

    Raises
    ------
    IOError
        If inputs are not properly specified
    """
    parser = argparse.ArgumentParser(
        prog="Rast Converter",
        description="Convert rasters to parquet files efficiently",
        epilog="",
    )

    parser.add_argument(
        "in_path",
        nargs="?",
        help="A path to a raster for processing.",
        type=str,
    )

    parser.add_argument(
        "--out_path",
        "-o_p",
        nargs="?",
        default="rast_convert_result",
        help="A folder to save into. Will be created or overwriten on execution. Default: %(default)s",
        type=str,
    )

    parser.add_argument(
        "--band",
        "-bd",
        nargs="?",
        default=1,
        help="The band to read from.",
        type=int,
    )

    parser.add_argument(
        "--in_crs",
        "-i_c",
        nargs="?",
        default=None,
        help="CRS of the input if not available.: %(default)s",
        type=str,
    )

    parser.add_argument(
        "--out_crs",
        "-o_c",
        nargs="?",
        default="epsg:4326",
        help="CRS of the output. Default: %(default)s",
        type=str,
    )

    parser.add_argument(
        "--include_negative",
        "-i_n",
        nargs="?",
        default=False,
        help="Whether the data to process includes relevant negative or 0 values. Can have a significant impact on running time and output size.",
        type=bool,
    )

    parser.add_argument(
        "--workers",
        "-w",
        nargs="?",
        default=4,
        help="The number of workers to run in parallel.",
        type=int,
    )

    parser.add_argument(
        "--gdal-cache-mb",
        nargs="?",
        default=64,
        help="GDAL block cache size in MB per worker. Default: %(default)s",
        type=int,
    )

    args = vars(parser.parse_args(args))

    src_file = args["in_path"]
    out_fold = args["out_path"]
    band = args["band"]
    in_crs = args["in_crs"]
    out_crs = args["out_crs"]
    include = args["include_negative"]
    num_workers = args["workers"]
    gdal_cache_mb = args["gdal_cache_mb"]

    os.environ["RASTAPAR_GDAL_CACHE_MB"] = str(gdal_cache_mb)

    if out_crs is not None:
        out_crs = check_out_crs(out_crs)
    else:
        out_crs = check_out_crs(in_crs)

    print("Output CRS : ", str(out_crs))

    vrt_options = {
        "crs": out_crs,
    }

    if not os.path.exists(out_fold):
        os.mkdir(out_fold)

    filename = os.path.splitext(os.path.basename(src_file))[0]

    with open(src_file) as src:
        if band > src.count:
            raise IOError(
                "Band parameter exceeds available bands.",
                "Available bands : ",
                src.count,
            )

        if src.count == 1:
            out_files = [
                out_fold + "/" + filename + "_" + str(i) + ".parquet"
                for i in range(num_workers)
            ]
        else:
            out_files = [
                out_fold
                + "/"
                + filename
                + "_band_"
                + str(band)
                + "_"
                + str(i)
                + ".parquet"
                for i in range(num_workers)
            ]

        src_crs = check_crs(src, in_crs=in_crs)
        vrt_options["src_crs"] = src_crs

        print("Input CRS : ", src_crs)

        nodata = check_nodata(src)
        print("No data value : ", nodata)

        band_var_dtype = infer_dtype(src)
        print("Variable type infered : ", band_var_dtype)

        rast_schema = schema(
            [("lon", float32()), ("lat", float32()), ("band_var", band_var_dtype)]
        )

        rast_schema.with_metadata(
            {
                "lon": "Longitude coordinate",
                "lat": "Latitude coordinate",
                "band_var": "Value associated",
            }
        )

    _dbg_log(
        "rast_convert_par.py:rast_converter:before_executor",
        "Right before ProcessPoolExecutor (fork point)",
        {"num_workers": num_workers},
        "F",
    )

    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(
                process,
                worker_id,
                num_workers,
                out_file,
                src_file,
                rast_schema,
                vrt_options,
                nodata,
                include,
                band,
            )
            for worker_id, out_file in enumerate(out_files)
        ]
        for future in futures:
            future.result()


if __name__ == "__main__":
    rast_converter()
