# test_rast_converter.py
# this will be a pytest workflow
import os

import numpy as np
import pytest
import rasterio
from pyarrow import concat_tables, float32, schema
from pyarrow.parquet import read_table
from rasterio import open
from rasterio.crs import CRS
from rasterio.transform import from_origin
from rasterio.vrt import WarpedVRT

from scalenav.rast_convert_par import (
    _BlockRegion,
    _partition_block_grid,
    process,
    rast_converter,
)
from scalenav.rast_converter import (
    check_crs,
    check_nodata,
    check_path,
    infer_dtype,
    rast_convert_core,
    rast_convert_window_to_batch,
)

# Directory to save rasters
output_dir = "data"

# Create the output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)


def _filter_core_output(out, nodata, include=False):
    """Apply the same filtering as the parallel rastapar path."""
    if nodata is not None and not (isinstance(nodata, float) and np.isnan(nodata)):
        out = out[out.band_var != nodata]
    out = out.dropna()
    if not include:
        out = out[out.band_var > 0]
    return out.reset_index(drop=True)


def _rast_schema_for(src):
    return schema(
        [("lon", float32()), ("lat", float32()), ("band_var", infer_dtype(src))]
    )


# Function to create a 6x6 raster with specified nodata and transform
def create_raster(
    filename, data, nodata_value, crs, transform, blockxsize=16, blockysize=16
):
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

crs_values = [
    "epsg:4326",
    "epsg:4326",
    None,
    "epsg:4326",
    "epsg:4326",
    "epsg:4326",
]

transforms = [
    from_origin(10, 50, 0.1, 0.1),  # Top-left corner (10, 50), cell size 0.1 x 0.1
    from_origin(-120, 35, 0.01, 0.01),
    from_origin(100, -45, 0.5, 0.5),
    from_origin(-75, 80, 0.05, 0.05),
    from_origin(25, 10, 0.2, 0.2),
    from_origin(-60, -15, 1.0, 1.0),
]

filenames = [
    os.path.join(output_dir, f"raster_{i+1}.tif") for i in range(0, len(nodata_values))
]

# Generate 6 rasters with different nodata values and transforms
for transform, no_data, filename, crs_ in zip(
    transforms, nodata_values, filenames, crs_values
):
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
    return [f"data/test_data_{i}.tif" for i in range(3)]


@pytest.fixture(scope="session")
def rast_ingest(tmp_path_factory: pytest.TempPathFactory):

    files = raw_files()

    for i, file in enumerate(files):
        in_file = "data" / file
        out_file = tmp_path_factory + f"/test_data_{i}.parquet"
        process(src_file=in_file, out_file=out_file)


# sample_file = f"../../data/test_data_{i}.parquet"


def test_path():
    path = "./data/"
    in_paths = check_path(path)
    assert len(in_paths) == 9

    path_incomplete = "./data"
    in_paths = check_path(path_incomplete)
    assert len(in_paths) == 9


def test_read_nodata():
    """Successful completion with the sample data set."""
    for file, nodat in zip(filenames, nodata_values):
        with open(file) as src:
            nodata = check_nodata(src)
            if np.isnan(nodata):
                pass
            else:
                assert nodata == nodat


def test_read_crs():
    """Recognition of different raster input formats."""
    for file, nodat in zip(filenames, nodata_values):
        with open(file) as src:
            src_crs = check_crs(src, in_crs="epsg:4326")
            assert src_crs == "epsg:4326"


def test_output_dim():
    """Ouput dimensions."""
    for filename in filenames:
        with open(filename) as src:
            transform = src.transform
            nodata = src.nodatavals

            out = rast_convert_core(src, transform=transform)

            assert out.shape == (32 * 32, 3)

            out.drop(index=out.loc[out.band_var == nodata].index, inplace=True)
            out.dropna(inplace=True)

            assert out.shape == (32 * 32 - 1, 3)


def test_window_to_batch_matches_core():
    """Batch conversion should match core conversion after filtering."""
    filename = filenames[1]

    with open(filename) as src:
        nodata = check_nodata(src)
        rast_schema = _rast_schema_for(src)

        for _, window in src.block_windows():
            core_out = _filter_core_output(
                rast_convert_core(src, transform=src.transform, win=window),
                nodata,
            )
            batch = rast_convert_window_to_batch(
                src,
                transform=src.transform,
                win=window,
                rast_schema=rast_schema,
                nodata=nodata,
                include=False,
            )

            if core_out.shape[0] == 0:
                assert batch is None
            else:
                batch_df = batch.to_pandas().reset_index(drop=True)
                core_sorted = core_out.sort_values(
                    ["lon", "lat", "band_var"]
                ).reset_index(drop=True)
                batch_sorted = batch_df.sort_values(
                    ["lon", "lat", "band_var"]
                ).reset_index(drop=True)
                np.testing.assert_allclose(
                    core_sorted["lon"].values,
                    batch_sorted["lon"].values,
                    rtol=1e-5,
                )
                np.testing.assert_allclose(
                    core_sorted["lat"].values,
                    batch_sorted["lat"].values,
                    rtol=1e-5,
                )
                np.testing.assert_allclose(
                    core_sorted["band_var"].values,
                    batch_sorted["band_var"].values,
                    rtol=1e-5,
                )
            break


def test_window_to_batch_sparse_raster(tmp_path):
    """Sparse rasters should produce fewer rows than full window size."""
    sparse_file = tmp_path / "sparse.tif"
    data = np.zeros((32, 32), dtype=np.float32)
    data[5, 5] = 42.0
    create_raster(
        str(sparse_file),
        data,
        nodata_value=0,
        crs="epsg:4326",
        transform=from_origin(0, 32, 1, 1),
    )

    with open(sparse_file) as src:
        nodata = check_nodata(src)
        rast_schema = _rast_schema_for(src)
        _, window = next(src.block_windows())
        batch = rast_convert_window_to_batch(
            src,
            transform=src.transform,
            win=window,
            rast_schema=rast_schema,
            nodata=nodata,
            include=False,
        )

    assert batch is not None
    assert batch.num_rows == 1
    assert batch.column("band_var")[0].as_py() == 42.0


@pytest.mark.parametrize(
    ("block_rows", "block_cols", "num_workers", "block_height", "block_width"),
    [
        (4, 4, 1, 16, 16),
        (7, 5, 4, 16, 16),
        (7, 5, 5, 16, 16),
        (2, 12, 4, 16, 16),
        (12, 2, 4, 16, 16),
        (2, 2, 6, 16, 16),
        (3, 3, 8, 16, 16),
        (5, 7, 3, 8, 32),
    ],
)
def test_partition_block_grid_has_exact_coverage(
    block_rows, block_cols, num_workers, block_height, block_width
):
    """Every block should belong to exactly one rectangular worker region."""
    regions = _partition_block_grid(
        block_rows,
        block_cols,
        num_workers,
        block_height,
        block_width,
    )

    assert len(regions) == num_workers
    assert sum(region is not None for region in regions) == min(
        num_workers, block_rows * block_cols
    )

    owners = {}
    for worker_id, region in enumerate(regions):
        if region is None:
            continue
        assert region.row_start < region.row_stop
        assert region.col_start < region.col_stop
        for block_row in range(region.row_start, region.row_stop):
            for block_col in range(region.col_start, region.col_stop):
                owners.setdefault((block_row, block_col), []).append(worker_id)

    assert set(owners) == {
        (block_row, block_col)
        for block_row in range(block_rows)
        for block_col in range(block_cols)
    }
    assert all(len(worker_ids) == 1 for worker_ids in owners.values())

    active_regions = [region for region in regions if region is not None]
    assert active_regions == sorted(
        active_regions,
        key=lambda region: (
            region.row_start,
            region.col_start,
            region.row_stop,
            region.col_stop,
        ),
    )


def test_partition_square_grid_into_quadrants():
    """Four workers on a square grid should receive compact quadrants."""
    assert _partition_block_grid(4, 4, 4, 16, 16) == [
        _BlockRegion(0, 2, 0, 2),
        _BlockRegion(0, 2, 2, 4),
        _BlockRegion(2, 4, 0, 2),
        _BlockRegion(2, 4, 2, 4),
    ]


def test_partition_balances_prime_worker_count():
    """Prime worker counts should not create one disproportionately large shard."""
    regions = _partition_block_grid(10, 10, 3, 16, 16)
    block_counts = [
        region.row_count * region.col_count
        for region in regions
        if region is not None
    ]

    assert max(block_counts) - min(block_counts) <= 5


def test_partition_rejects_invalid_worker_count():
    with pytest.raises(ValueError, match="num_workers must be at least 1"):
        _partition_block_grid(4, 4, 0)


def test_cli_rejects_invalid_worker_count():
    with pytest.raises(SystemExit):
        rast_converter(["unused.tif", "--workers", "0"])


def test_worker_spatial_sharding_and_parity(tmp_path):
    """Worker files should be spatially coherent and match one worker output."""
    filename = str(tmp_path / "spatial_blocks.tif")
    data = np.ones((512, 2048), dtype=np.float32)
    create_raster(
        filename,
        data,
        nodata_value=0,
        crs="epsg:4326",
        transform=from_origin(-10, 80, 0.01, 0.01),
        blockxsize=128,
        blockysize=128,
    )
    vrt_options = {"crs": CRS.from_epsg(4326), "src_crs": "epsg:4326"}

    with open(filename) as src:
        with WarpedVRT(src, **vrt_options) as vrt:
            block_height, block_width = vrt.block_shapes[0]
            block_rows = (vrt.height + block_height - 1) // block_height
            block_cols = (vrt.width + block_width - 1) // block_width

    assert block_rows * block_cols >= 4

    # Give every VRT block a unique value so shard membership is observable in
    # the Parquet outputs without relying on iterator order.
    for block_row in range(block_rows):
        row_start = block_row * block_height
        row_stop = min(row_start + block_height, data.shape[0])
        for block_col in range(block_cols):
            col_start = block_col * block_width
            col_stop = min(col_start + block_width, data.shape[1])
            data[row_start:row_stop, col_start:col_stop] = (
                block_row * block_cols + block_col + 1
            )

    with rasterio.open(filename, "r+") as dst:
        dst.write(data, 1)

    with open(filename) as src:
        nodata = check_nodata(src)
        rast_schema = _rast_schema_for(src)

    num_workers = 4
    regions = _partition_block_grid(
        block_rows,
        block_cols,
        num_workers,
        block_height,
        block_width,
    )
    out_files = [str(tmp_path / f"shard_{i}.parquet") for i in range(num_workers)]

    for worker_id, out_file in enumerate(out_files):
        process(
            worker_id,
            num_workers,
            out_file,
            filename,
            rast_schema,
            vrt_options,
            nodata,
            False,
            1,
        )

        region = regions[worker_id]
        assert region is not None
        expected_values = {
            block_row * block_cols + block_col + 1
            for block_row in range(region.row_start, region.row_stop)
            for block_col in range(region.col_start, region.col_stop)
        }
        assert set(read_table(out_file).column("band_var").to_pylist()) == (
            expected_values
        )

    single_out = str(tmp_path / "single.parquet")
    process(0, 1, single_out, filename, rast_schema, vrt_options, nodata, False, 1)

    sharded = concat_tables([read_table(f) for f in out_files])
    single = read_table(single_out)

    assert sharded.num_rows == single.num_rows
    assert sharded.num_rows > 0

    sort_columns = ["lon", "lat", "band_var"]
    sharded_df = sharded.to_pandas().sort_values(sort_columns).reset_index(drop=True)
    single_df = single.to_pandas().sort_values(sort_columns).reset_index(drop=True)
    for column in sort_columns:
        np.testing.assert_allclose(sharded_df[column], single_df[column])


def test_excess_worker_writes_empty_parquet(tmp_path):
    """Workers without a block region should still create a valid file."""
    filename = filenames[1]
    vrt_options = {"crs": CRS.from_epsg(4326), "src_crs": "epsg:4326"}

    with open(filename) as src:
        nodata = check_nodata(src)
        rast_schema = _rast_schema_for(src)
        with WarpedVRT(src, **vrt_options) as vrt:
            block_height, block_width = vrt.block_shapes[0]
            block_rows = (vrt.height + block_height - 1) // block_height
            block_cols = (vrt.width + block_width - 1) // block_width

    num_workers = block_rows * block_cols + 1
    out_file = str(tmp_path / "empty_shard.parquet")
    process(
        num_workers - 1,
        num_workers,
        out_file,
        filename,
        rast_schema,
        vrt_options,
        nodata,
        False,
        1,
    )

    empty = read_table(out_file)
    assert empty.num_rows == 0
    assert empty.schema == rast_schema


def test_process_selected_band(tmp_path):
    """Spatial processing should use the selected band's block layout."""
    filename = str(tmp_path / "multiband.tif")
    data = np.stack(
        [
            np.ones((32, 32), dtype=np.float32),
            np.full((32, 32), 7, dtype=np.float32),
        ]
    )
    with rasterio.open(
        filename,
        "w",
        driver="GTiff",
        height=32,
        width=32,
        count=2,
        dtype=data.dtype,
        crs="epsg:4326",
        transform=from_origin(0, 32, 1, 1),
        nodata=0,
        tiled=True,
        blockxsize=16,
        blockysize=16,
    ) as dst:
        dst.write(data)

    vrt_options = {"crs": CRS.from_epsg(4326), "src_crs": "epsg:4326"}
    with open(filename) as src:
        nodata = check_nodata(src)
        rast_schema = _rast_schema_for(src)

    out_file = str(tmp_path / "band_2.parquet")
    process(
        0,
        1,
        out_file,
        filename,
        rast_schema,
        vrt_options,
        nodata,
        False,
        2,
    )

    output = read_table(out_file)
    assert output.num_rows == 32 * 32
    assert set(output.column("band_var").to_pylist()) == {7.0}


# def test_input():
#     """Validity of input. Folder with files.
#     What to do if crs not found ? Have parameter set to None by default.
#     """
#     pass
