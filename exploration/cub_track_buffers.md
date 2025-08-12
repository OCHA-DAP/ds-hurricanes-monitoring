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

# Track buffers for Cuba

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from shapely.geometry import LineString

from src.datasources import codab, ibtracs, nhc
from src.monitoring import monitoring_utils
from src.datasources.ibtracs import estimate_wind_at_distance
from src.utils import blob
```

```python
adm_all = codab.load_combined_codab()
adm_cub = adm_all[adm_all["ADM_PCODE"].str.contains("CU")]
```

```python
df_stats = ibtracs.load_all_adm_wind_stats()
df_stats_cub = df_stats[df_stats["ADM_PCODE"].str.contains("CU")]
```

```python
df_tracks = ibtracs.load_ibtracs_with_wind(wind_provider="usa")
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
df_stats_cub_max_worst
```

```python
df_tracks
```

```python
D_THRESH = 230
```

```python
cols = ["lat", "lon", "usa_wind"]

gdfs = []
for sid in df_stats_cub_max_worst["sid"].unique():
    dff_tracks = df_tracks[df_tracks["sid"] == sid].copy()
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
    line = LineString(gdf.geometry.tolist())
    line_gdf = gpd.GeoDataFrame(geometry=[line], crs=gdf.crs)
    buffered_line = line_gdf.buffer(D_THRESH * 1000)
    buffered_gdf = gpd.GeoDataFrame(
        geometry=buffered_line, crs=gdf.crs
    ).to_crs(4326)
    buffered_gdf["sid"] = sid
    gdfs.append(buffered_gdf)
```

```python
gdf_buffers = pd.concat(gdfs, ignore_index=True)
```

```python
fig, ax = plt.subplots()
gdf_buffers[gdf_buffers["sid"] == "2017242N16333"].boundary.plot(ax=ax)
adm_cub.plot(ax=ax)
```

```python
blob_name = (
    f"{blob.PROJECT_PREFIX}/processed/ibtracs/cub_d{D_THRESH}_buffers.shp"
)
blob.upload_gdf_to_blob(gdf_buffers, blob_name)
```

```python
test = blob.load_gdf_from_blob(blob_name)
```

```python
test[test["sid"] == "2017242N16333"].boundary.plot()
```
