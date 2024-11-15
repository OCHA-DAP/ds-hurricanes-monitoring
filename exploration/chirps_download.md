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
    display_name: ds-hurricanes-monitoring-39
    language: python
    name: ds-hurricanes-monitoring-39
---

# CHIRPS download

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import geopandas as gpd
from tqdm.auto import tqdm

from src.datasources import chirps, ibtracs, codab
from src.constants import *
```

```python
adm = codab.load_codab_from_blob("hnd", admin_level=0)
```

```python
ib_df = ibtracs.load_ibtracs_with_wind()
ib_df_eta_iota = ib_df[ib_df["sid"].isin([ETA_SID, IOTA_SID])]
```

```python
gdf = gpd.GeoDataFrame(
    data=ib_df_eta_iota,
    geometry=gpd.points_from_xy(ib_df_eta_iota["lon"], ib_df_eta_iota["lat"]),
    crs=4326,
)
```

```python
gdf["distance_km"] = (
    gdf.to_crs(3857).geometry.distance(adm.to_crs(3857).iloc[0].geometry)
    / 1000
)
```

```python
gdf_close = gdf[gdf["distance_km"] < 500]
```

```python
dates = gdf_close["time"].dt.date.unique()
```

```python
for date in tqdm(gdf_close["time"]):
    chirps.download_chirps_daily(date, adm.total_bounds, "hnd")
```

```python
gdf_close
```

```python
chirps.process_chirps_daily("hnd")
```

```python

```
