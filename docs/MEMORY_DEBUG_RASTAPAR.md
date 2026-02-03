# Memory Debug Guide: rastapar (rast_convert_par)

## Summary

Your suspicions appear **founded**. Several patterns in `rastapar` and its dependencies can cause high memory use on HPC, especially when workers or raster size increase.

---

## Hypotheses

| ID | Hypothesis | Evidence in Code |
|----|------------|------------------|
| **A** | Fork inheritance: workers inherit parent memory at spawn | Main process does heavy setup (opens raster, builds full `windows` list) **inside** `with open(src_file) as src:` **before** `ProcessPoolExecutor`. On Unix, workers are forked and inherit the parent's address space (copy-on-write). |
| **B** | Per-window peak memory: multiple large arrays alive at once | `rast_convert_core` allocates: `band_`, `meshgrid(cols, rows)`, `xs, ys`, `lons, lats`, `DataFrame`, then `RecordBatch.from_pandas` copies again. For a 1024×1024 window, meshgrid alone is ~16 MB (2 × 1024² × 8 bytes). |
| **C** | DataFrame `inplace=True` drops cause fragmentation / delayed GC | `out.drop(index=out.loc[out.band_var == nodata].index, inplace=True)` creates intermediate boolean masks and index views. `inplace=True` can fragment memory; Python GC may not reclaim promptly. |
| **D** | GDAL/rasterio per-handle caches multiply with workers | Each worker opens the same raster independently. GDAL block cache, WarpedVRT warp buffers, and rasterio internals are per-handle. With `-w 8`, you get 8× those caches. |
| **E** | Full `windows` list kept in main process | `windows = [window for _, window in WarpedVRT(src, **vrt_options).block_windows()]` materializes all windows. For large rasters (many blocks), this list is non-trivial and lives until the executor finishes. |
| **F** | GDAL default cache not tuned for HPC | `GDAL_CACHEMAX` controls block cache. Unset defaults can be large; combined with multiple workers and WarpedVRT, total cache can spike early. |

---

## Why HPC vs Local Differs

- **Larger rasters on HPC** → more blocks, larger windows, more memory per window.
- **More workers** (`-w`) → more concurrent GDAL handles and caches.
- **Stricter memory limits** → job killed when RSS exceeds allocation; local machines often have more RAM.
- **Different filesystem** (e.g. Lustre) → different I/O buffering and caching behavior.

---

## Debug Instrumentation

Instrumentation has been added to log:

- **Main process**: memory and `windows` size before the executor (fork point).
- **Workers**: entry, before/after `rast_convert_core`, after `writer.write`; window dimensions, DataFrame size, RSS.
- **rast_convert_core**: window size, output rows, band array size.

Logs are written to `.cursor/debug.log` in NDJSON. Each line includes `rss_mb`, `pid`, `hypothesisId`, and `data`.

---

## Reproduction Steps

<reproduction_steps>
1. Delete any existing `.cursor/debug.log` (or ensure it is cleared before the run).
2. Run rastapar on a raster that triggers the issue on HPC (or locally with a large raster and `--workers 4` or higher):
   ```
   rastapar /path/to/your/raster.tif --out_path ./out --workers 4
   ```
3. Let the job run until it completes or is killed by the scheduler (OOM).
4. If on HPC: copy `.cursor/debug.log` back to your local workspace (or inspect it on the login node).
5. Proceed so the logs can be analyzed.
</reproduction_steps>

---

## Mitigations to Try (After Log Analysis)

1. **Set `GDAL_CACHEMAX`** (e.g. in your job script):
   ```bash
   export GDAL_CACHEMAX=256   # MB; tune for your allocation
   ```

2. **Reduce workers** to lower concurrent GDAL handles and cache use.

3. **Avoid holding full `windows` list**: generate batches lazily and pass only batch iterators; consider `ThreadPoolExecutor` if GIL is not the bottleneck (rasterio/GDAL release it).

4. **Reduce peak memory in `rast_convert_core`**:
   - Use `arange` with smaller dtypes where possible.
   - Avoid materializing full meshgrid if you can iterate over indices.
   - Ensure intermediate arrays are explicitly `del`'d or go out of scope before creating the next large object.

5. **Replace inplace drops** with filtering that returns new DataFrames and lets the old one be collected:
   ```python
   out = out[out.band_var != nodata]
   if not include:
       out = out[out.band_var > 0]
   ```

6. **Process pool after minimal setup**: move `open(src_file)` and window discovery into workers, or use a smaller prefork footprint (e.g. open, read metadata only, close, then start the pool).
