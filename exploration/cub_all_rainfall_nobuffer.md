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

# Cuba rainfall no buffer

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
adm_cub.plot()
```

```python
df_tracks = ibtracs.load_ibtracs_with_wind(wind_provider="usa")
```

```python
df_stats = ibtracs.load_all_adm_wind_stats()
df_stats_cub = df_stats[df_stats["ADM_PCODE"].str.contains("CU")]
```

```python
df_stats_cub_max = (
    df_stats_cub.groupby("sid")["adm_wind"]
    .max()
    .reset_index()
    .sort_values("adm_wind", ascending=False)
)
```

```python
df_stats_cub_max_worst = df_stats_cub_max.iloc[:100]
```

```python
dfs = []
dicts_sum = []
extend_daterange = 1
error_dates = []
for sid in tqdm(df_stats_cub_max_worst["sid"].unique()):
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
    da_clip = da_up.rio.clip(adm_cub.geometry)

    # date-wise aggregations
    var_name = "q"
    df_out = (
        da_clip.quantile(0.9, dim=["x", "y"])
        .to_dataframe(var_name)[var_name]
        .reset_index()
    )
    var_name = "mean"
    df_out.loc[:, var_name] = (
        da_clip.mean(dim=["x", "y"])
        .to_dataframe(var_name)[var_name]
        .reset_index()[var_name]
    )
    var_name = "max"
    df_out.loc[:, var_name] = (
        da_clip.max(dim=["x", "y"])
        .to_dataframe(var_name)[var_name]
        .reset_index()[var_name]
    )

    for var_name in ["q", "mean", "max"]:
        df_out[f"roll2_{var_name}"] = df_out[var_name].rolling(window=2).sum()
    df_out["sid"] = sid
    dfs.append(df_out)

    # sum-over-dates aggregations
    da_sum = da_clip.sum(dim="date")
    dicts_sum.append(
        {
            "sid": sid,
            "mean": float(da_sum.mean(dim=["x", "y"])),
            "max": float(da_sum.max(dim=["x", "y"])),
            "q90": float(da_sum.quantile(0.9, dim=["x", "y"])),
        }
    )
```

```python
df_sum = pd.DataFrame(dicts_sum)
```

```python
df_sum
```

```python
df = pd.concat(dfs, ignore_index=True)
```

```python
df
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/processed/imerg/cub_imerg_nobuffer_multi_agg.parquet"
blob.upload_parquet_to_blob(blob_name, df)
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/processed/imerg/cub_imerg_nobuffer_multi_sum_agg.parquet"
blob.upload_parquet_to_blob(blob_name, df_sum)
```

```python
dates = pd.date_range("2017-09-08", "2017-09-11")
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
da_clip = da_up.rio.clip(adm_cub.geometry)
```

```python
da_clip.isel(date=0).plot()
```

```python
da_clip.isel(date=1).plot()
```

```python
da_clip.isel(date=1).quantile(0.9).compute()
```

```python
da_clip.isel(date=2).plot()
```

```python
da_clip.isel(date=3).plot()
```
