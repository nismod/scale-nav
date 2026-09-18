# rast_convert_par.py
import argparse
import concurrent.futures
import gc
import os
from dataclasses import dataclass
from fractions import Fraction

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


@dataclass(frozen=True)
class _BlockRegion:
    """A rectangular, half-open region of a raster's block grid."""

    row_start: int
    row_stop: int
    col_start: int
    col_stop: int

    @property
    def row_count(self) -> int:
        return self.row_stop - self.row_start

    @property
    def col_count(self) -> int:
        return self.col_stop - self.col_start


def _split_block_region(
    region: _BlockRegion,
    worker_count: int,
    block_height: int,
    block_width: int,
) -> list[_BlockRegion]:
    """Recursively split a block region into compact worker regions."""
    block_count = region.row_count * region.col_count
    if worker_count < 1 or worker_count > block_count:
        raise ValueError("worker count must fit within the block region")
    if worker_count == 1:
        return [region]

    can_split_rows = region.row_count > 1
    can_split_cols = region.col_count > 1
    split_rows = can_split_rows and (
        not can_split_cols
        or region.row_count * block_height >= region.col_count * block_width
    )

    axis_length = region.row_count if split_rows else region.col_count
    cross_length = region.col_count if split_rows else region.row_count
    candidates = []
    for candidate_workers in range(1, worker_count):
        # Try both integer cuts around the ideal proportional position.
        cut_floor = axis_length * candidate_workers // worker_count
        cut_ceil = (
            axis_length * candidate_workers + worker_count - 1
        ) // worker_count
        for candidate_cut in {cut_floor, cut_ceil}:
            if candidate_cut < 1 or candidate_cut >= axis_length:
                continue

            first_block_count = candidate_cut * cross_length
            second_block_count = block_count - first_block_count
            second_workers = worker_count - candidate_workers
            if (
                candidate_workers > first_block_count
                or second_workers > second_block_count
            ):
                continue

            per_worker_imbalance = Fraction(
                abs(
                    first_block_count * second_workers
                    - second_block_count * candidate_workers
                ),
                candidate_workers * second_workers,
            )
            candidates.append(
                (
                    per_worker_imbalance,
                    abs(candidate_workers - second_workers),
                    abs(axis_length - 2 * candidate_cut),
                    candidate_cut,
                    candidate_workers,
                )
            )

    if not candidates:
        raise RuntimeError("could not split a populated block region")

    _, _, _, cut_at, first_workers = min(candidates)
    second_workers = worker_count - first_workers

    if split_rows:
        split_at = region.row_start + cut_at
        first = _BlockRegion(
            region.row_start,
            split_at,
            region.col_start,
            region.col_stop,
        )
        second = _BlockRegion(
            split_at,
            region.row_stop,
            region.col_start,
            region.col_stop,
        )
    else:
        split_at = region.col_start + cut_at
        first = _BlockRegion(
            region.row_start,
            region.row_stop,
            region.col_start,
            split_at,
        )
        second = _BlockRegion(
            region.row_start,
            region.row_stop,
            split_at,
            region.col_stop,
        )

    return _split_block_region(
        first, first_workers, block_height, block_width
    ) + _split_block_region(second, second_workers, block_height, block_width)


def _partition_block_grid(
    block_rows: int,
    block_cols: int,
    num_workers: int,
    block_height: int = 1,
    block_width: int = 1,
) -> list[_BlockRegion | None]:
    """Assign compact, deterministic block regions to workers.

    Workers in excess of the number of blocks receive ``None`` so callers can
    still create the requested number of output files.
    """
    if num_workers < 1:
        raise ValueError("num_workers must be at least 1")
    if block_rows < 1 or block_cols < 1:
        raise ValueError("block grid dimensions must be at least 1")
    if block_height < 1 or block_width < 1:
        raise ValueError("block dimensions must be at least 1")

    active_workers = min(num_workers, block_rows * block_cols)
    regions = _split_block_region(
        _BlockRegion(0, block_rows, 0, block_cols),
        active_workers,
        block_height,
        block_width,
    )
    regions.sort(
        key=lambda region: (
            region.row_start,
            region.col_start,
            region.row_stop,
            region.col_stop,
        )
    )
    return regions + [None] * (num_workers - active_workers)


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
    """Process one spatial block region and write it to a parquet file."""
    if num_workers < 1:
        raise ValueError("num_workers must be at least 1")
    if worker_id < 0 or worker_id >= num_workers:
        raise ValueError("worker_id must identify one of num_workers")

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
            block_height, block_width = vrt.block_shapes[band - 1]
            block_rows = (vrt.height + block_height - 1) // block_height
            block_cols = (vrt.width + block_width - 1) // block_width
            region = _partition_block_grid(
                block_rows,
                block_cols,
                num_workers,
                block_height,
                block_width,
            )[worker_id]

            region_data = None
            if region is not None:
                region_data = {
                    "row_start": region.row_start,
                    "row_stop": region.row_stop,
                    "col_start": region.col_start,
                    "col_stop": region.col_stop,
                }
            _dbg_log(
                "rast_convert_par.py:process:region",
                "Spatial block region assigned",
                {
                    "worker_id": worker_id,
                    "block_rows": block_rows,
                    "block_cols": block_cols,
                    "region": region_data,
                },
                "S",
            )

            with ParquetWriter(
                where=out_file, schema=rast_schema, **writer_args
            ) as writer:
                if region is None:
                    return

                for block_row in range(region.row_start, region.row_stop):
                    for block_col in range(region.col_start, region.col_stop):
                        window = vrt.block_window(band, block_row, block_col)

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
                                "block_row": block_row,
                                "block_col": block_col,
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

    if num_workers < 1:
        parser.error("--workers must be at least 1")

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
