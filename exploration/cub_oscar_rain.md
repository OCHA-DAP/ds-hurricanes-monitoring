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

# Cuba- Oscar rainfall

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import geopandas as gpd
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import xarray as xr
from shapely.geometry import LineString

from src.datasources import codab, ibtracs, nhc, imerg
from src.datasources.ibtracs import estimate_wind_at_distance
from src.utils.raster import upsample_dataarray
```

```python
OSCAR_ATCF_ID = "al162024"
```

```python
AOI_ADM_PCODES = ["CU07", "CU06", "CU15"]
```

```python
adm_all = codab.load_combined_codab()
adm_aoi = adm_all[adm_all["ADM_PCODE"].isin(AOI_ADM_PCODES)]
adm_cub = adm_all[adm_all["ADM_PCODE"].str.contains("CU")]
```

```python
df_obsv = nhc.load_recent_glb_obsv()
df_obsv_oscar = df_obsv[df_obsv["id"] == OSCAR_ATCF_ID]
```

```python
cols = ["latitude", "longitude", "intensity"]
df_interp = (
    df_obsv_oscar.set_index("lastUpdate")[cols]
    .resample("30min")
    .interpolate()
    .reset_index()
)
gdf = gpd.GeoDataFrame(
    df_interp,
    geometry=gpd.points_from_xy(df_interp.longitude, df_interp.latitude),
    crs="EPSG:4326",
).to_crs(3857)
```

```python
for pcode, row in adm_aoi.to_crs(3857).set_index("ADM_PCODE").iterrows():
    gdf[f"distance_{pcode}"] = gdf.geometry.distance(row.geometry) / 1000
    gdf[f"adm_wind_{pcode}"] = estimate_wind_at_distance(
        gdf["intensity"], gdf[f"distance_{pcode}"]
    )
```

```python
gdf["distance_min"] = gdf[[x for x in gdf.columns if "distance_CU" in x]].min(
    axis=1
)
```

```python
D_THRESH = 230
```

```python
gdf_dist = gdf[gdf["distance_min"] < D_THRESH]
```

```python
start_day = pd.Timestamp(gdf_dist["lastUpdate"].min().date())
end_day_late = pd.Timestamp(
    gdf_dist["lastUpdate"].max().date() + pd.Timedelta(days=1)
)
```

```python
dates = pd.date_range(start_day, end_day_late)
```

```python
dates
```

```python
das = []
for date in dates:
    da_in = imerg.open_imerg_raster(date)
    da_in.attrs["_FillValue"] = np.NaN
    da_in = da_in.rio.write_crs(4326)
    da_in = da_in.where(da_in >= 0).squeeze(drop=True)
    da_in["date"] = date
    das.append(da_in)
```

```python
da = xr.concat(das, dim="date")
```

```python
da_up = upsample_dataarray(da, lat_dim="y", lon_dim="x", resolution=0.05)
da_clip = da_up.rio.clip(adm_aoi.geometry)
```

```python
df_imerg = (
    da_clip.mean(dim=["x", "y"]).to_dataframe("mean")["mean"].reset_index()
)
```

```python
df_imerg["roll2"] = df_imerg["mean"].rolling(window=2).sum()
df_imerg
```

```python
fig, ax = plt.subplots(dpi=300)
adm_aoi.boundary.plot(ax=ax)
da_clip.isel(date=1).plot(ax=ax)
```

```python

```

## With 90th percentile

```python
adm_cub.plot()
```

```python
df_imerg_q = (
    da_up.rio.clip(adm_cub.geometry)
    .quantile(0.9, dim=["x", "y"])
    .to_dataframe("q")["q"]
    .reset_index()
)
```

```python
da_up.isel(date=1).rio.clip(adm_cub.geometry).plot()
```

```python
da_sum = da_up.rio.clip(adm_cub.geometry).sum(dim="date")
```

```python
da_sum = da_sum.rio.clip(adm_cub.geometry)
```

```python
float(da_sum.max(dim=["x", "y"]))
```

```python
fig, ax = plt.subplots(dpi=300, figsize=(10, 3))
adm_cub.boundary.plot(ax=ax, linewidth=0.5, color="white")
da_sum.plot(ax=ax)
ax.axis("off")
ax.set_title(
    f"Cuba total rainfall {start_day.date()} to "
    f"{end_day_late.date()}\n(mm, IMERG Late)"
)
```

```python
da_up.isel(date=0).rio.clip(adm_cub.geometry).quantile(0.9).compute()
```

```python
var_name = "q"
da_clip = da_up.rio.clip(adm_cub.geometry)
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
```

```python
float(da_clip.max())
```

```python
df_imerg_q["roll2"] = df_imerg_q["q"].rolling(window=2).sum()
df_imerg_q
```

## With buffer rain calc

```python
line = LineString(gdf.geometry.tolist())
```

```python
line_gdf = gpd.GeoDataFrame(geometry=[line], crs=gdf.crs)
```

```python
RAIN_BUFFER_KM = D_THRESH
buffered_line = line_gdf.buffer(RAIN_BUFFER_KM * 1000)
```

```python
buffered_gdf = gpd.GeoDataFrame(geometry=buffered_line, crs=gdf.crs)
```

```python
da_clip_track = da_up.rio.clip(adm_cub.geometry).rio.clip(
    buffered_gdf.geometry.to_crs(4326)
)
```

```python
fig, ax = plt.subplots(dpi=300)
adm_cub.boundary.plot(ax=ax)
da_clip_track.isel(date=1).plot(ax=ax)
buffered_gdf.to_crs(4326).boundary.plot(ax=ax, color="red")
```

```python
df_imerg_buffer = (
    da_clip_track.mean(dim=["x", "y"])
    .to_dataframe("mean")["mean"]
    .reset_index()
)
```

```python
df_imerg_buffer
```
