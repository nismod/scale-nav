# Memory Debug Guide: rastapar (rast_convert_par)

## Summary

`rastapar` previously OOM-killed on SLURM because Linux workers forked from a parent that had already opened rasters, built a full window list, and held large GDAL state. Peak memory was further amplified by per-window pandas/meshgrid allocations and unbounded GDAL caches in each worker.

These issues have been addressed in the parallel path (`src/scalenav/rast_convert_par.py`):

1. **Minimal prefork footprint** — parent reads metadata only, then closes the raster before starting `ProcessPoolExecutor`.
2. **Worker-side window sharding** — each worker iterates `block_windows()` and processes `block_index % num_workers == worker_id`.
3. **Mask-first conversion** — `rast_convert_window_to_batch()` filters nodata/zero pixels before computing coordinates and writes PyArrow `RecordBatch` directly (no pandas in the hot path).
4. **Bounded GDAL cache** — each worker sets cache via `RASTAPAR_GDAL_CACHE_MB` (default 64 MB) or `--gdal-cache-mb`.

---

## Root Causes (Historical)

| ID | Hypothesis | Status |
|----|------------|--------|
| **A** | Fork inheritance: workers inherit parent memory at spawn | **Fixed** — parent no longer holds raster/windows at fork |
| **B** | Per-window peak memory from meshgrid + DataFrame + from_pandas | **Fixed** in parallel path via `rast_convert_window_to_batch` |
| **C** | DataFrame `inplace=True` drops cause fragmentation | **Fixed** in parallel path — no pandas filtering |
| **D** | GDAL/rasterio per-handle caches multiply with workers | **Mitigated** — bounded cache per worker |
| **E** | Full `windows` list kept in main process | **Fixed** — list removed |
| **F** | GDAL default cache not tuned for HPC | **Fixed** — `--gdal-cache-mb` / `RASTAPAR_GDAL_CACHE_MB` |

---

## Why HPC vs Local Differed

- **Linux fork vs macOS spawn** — forked workers inherited the parent's RSS; macOS uses spawn with a clean address space.
- **Stricter memory limits** — SLURM cgroup limits trigger OOM kills; local machines often have more headroom.
- **Larger production rasters** on HPC increase per-window and per-worker pressure.

---

## SLURM Job Script (Recommended)

```bash
#!/bin/bash
#SBATCH --job-name=rastapar
#SBATCH --mem=16G          # tune to raster size and worker count
#SBATCH --cpus-per-task=4

export RASTAPAR_GDAL_CACHE_MB=64

rastapar /path/to/raster.tif \
  --out_path ./out \
  --workers 4 \
  --gdal-cache-mb 64
```

If jobs still OOM:

- Reduce `--workers` (each worker needs peak window memory + GDAL cache).
- Lower `--gdal-cache-mb` (e.g. 32).
- Increase `#SBATCH --mem`.

---

## Debug Instrumentation

Optional NDJSON logging is available when `RASTAPAR_DEBUG_LOG` is set:

```bash
export RASTAPAR_DEBUG_LOG=/path/to/debug.log
rastapar /path/to/raster.tif --out_path ./out --workers 4
```

Each log line includes `rss_mb`, `pid`, `hypothesisId`, and `data`. Useful for confirming memory stays bounded on the cluster after the fix.

---

## HPC Validation Checklist

1. Run the same raster and `--workers` count that previously OOM-killed.
2. Confirm the job completes without scheduler OOM.
3. Compare total output row count against a known-good local run:
   ```python
   import pyarrow.parquet as pq
   from pyarrow import concat_tables
   tables = [pq.read_table(f"out/raster_{i}.parquet") for i in range(num_workers)]
   total_rows = sum(t.num_rows for t in tables)
   ```
4. Optionally enable `RASTAPAR_DEBUG_LOG` and verify RSS does not grow unbounded across windows.

---

## Sequential CLI Note

The single-process CLI in `src/scalenav/rast_converter.py` still uses `rast_convert_core()` with pandas. Only the parallel `rastapar` entry point uses the optimized batch path.
