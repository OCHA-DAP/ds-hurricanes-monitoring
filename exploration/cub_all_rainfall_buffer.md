---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.1
  kernelspec:
    display_name: ds-hurricanes-monitoring
    language: python
    name: ds-hurricanes-monitoring
---

# Cuba all rainfall buffer

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import geopandas as gpd
import pandas as pd
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt

from tqdm.auto import tqdm

from src.datasources import codab, ibtracs, nhc, imerg
from src.utils import blob
from src.utils.raster import upsample_dataarray
```

```python
D_THRESH = 230
```

```python
adm_all = codab.load_combined_codab()
adm_cub = adm_all[adm_all["ADM_PCODE"].str.contains("CU")]
```

```python
adm_cub_dissolve = adm_cub.dissolve()
```

```python
adm_cub_dissolve.plot()
```

```python
df_tracks = ibtracs.load_ibtracs_with_wind(wind_provider="usa")
```

```python
blob_name = (
    f"{blob.PROJECT_PREFIX}/processed/ibtracs/cub_d{D_THRESH}_buffers.shp"
)
df_buffers = blob.load_gdf_from_blob(blob_name)
```

```python
df_tracks
```

```python
dfs = []
error_dates = []
extend_daterange = 1
for sid, buffer in tqdm(df_buffers.groupby("sid")):
    dff_tracks = df_tracks[df_tracks["sid"] == sid].copy()
    cols = ["lat", "lon", "usa_wind"]
    df_interp = (
        dff_tracks.set_index("time")[cols]
        .resample("30min")
        .interpolate()
        .reset_index()
    )
    gdf = gpd.GeoDataFrame(
        df_interp,
        geometry=gpd.points_from_xy(df_interp.lon, df_interp.lat),
        crs="EPSG:4326",
    ).to_crs(3857)
    gdf["distance"] = (
        gdf.geometry.distance(adm_cub_dissolve.to_crs(3857).iloc[0].geometry)
        / 1000
    )
    gdf_dist = gdf[gdf["distance"] < D_THRESH]
    if gdf_dist.empty:
        continue
    start_day = pd.Timestamp(gdf_dist["time"].min().date()) - pd.Timedelta(
        days=extend_daterange
    )
    end_day_late = pd.Timestamp(
        gdf_dist["time"].max().date() + pd.Timedelta(days=extend_daterange + 1)
    )
    dates = pd.date_range(start_day, end_day_late)
    das = []
    for date in dates:
        try:
            da_in = imerg.open_imerg_raster(date)
        except Exception as e:
            print(date)
            error_dates.append(date)
            continue
        da_in.attrs["_FillValue"] = np.NaN
        da_in = da_in.rio.write_crs(4326)
        da_in = da_in.where(da_in >= 0).squeeze(drop=True)
        da_in["date"] = date
        da_in = da_in.persist()
        das.append(da_in)
    da = xr.concat(das, dim="date")
    da_up = upsample_dataarray(da, lat_dim="y", lon_dim="x", resolution=0.05)
    da_clip_track = da_up.rio.clip(adm_cub.geometry).rio.clip(buffer.geometry)
    df_out = (
        da_clip_track.mean(dim=["x", "y"])
        .to_dataframe("mean")["mean"]
        .reset_index()
    )
    df_out["roll2"] = df_out["mean"].rolling(window=2).sum()
    df_out["sid"] = sid
    dfs.append(df_out)
    # break
```

```python
df_out
```

```python
df = pd.concat(dfs, ignore_index=True)
```

```python
blob_name = (
    f"{blob.PROJECT_PREFIX}/processed/imerg/cub_imerg_buffer_agg.parquet"
)
blob.upload_parquet_to_blob(blob_name, df)
```

```python
df.groupby("sid")["roll2"].max().reset_index().sort_values(
    "roll2", ascending=False
).iloc[:20]
```

```python
df.sort_values("mean", ascending=False).iloc[:20]
```

```python
da_test = imerg.open_imerg_raster(pd.Timestamp(2000, 9, 16))
da_test.attrs["_FillValue"] = np.NaN
# da_test = da_test.where(da_in >= 0)
da_test_clip = da_test.rio.clip(adm_cub.geometry)
```

```python
fig, ax = plt.subplots(dpi=300)
adm_cub.boundary.plot(ax=ax)
da_test_clip.plot(ax=ax)
# buffer.boundary.plot(ax=ax, color="red")
```

```python
error_dates
```

```python
fig, ax = plt.subplots(dpi=300)
adm_cub.boundary.plot(ax=ax)
da_clip_track.isel(date=5).plot(ax=ax)
buffer.boundary.plot(ax=ax, color="red")
```

```python

```
