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

# Cuba- Rafael wind speed

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import geopandas as gpd
import pandas as pd

from src.datasources import codab, ibtracs, nhc
from src.monitoring import monitoring_utils
from src.datasources.ibtracs import estimate_wind_at_distance
from src.constants import *
```

```python
adm_all = codab.load_combined_codab()
```

```python
AOI_ADM_PCODES = [
    x for x in adm_all["ADM_PCODE"].unique() if x.startswith("CU")
]
```

```python
adm_aoi = adm_all[adm_all["ADM_PCODE"].isin(AOI_ADM_PCODES)]
```

```python
adm_aoi
```

```python
df_sid_names = ibtracs.load_sid_names()
```

```python
df_sid_names
```

```python
df_stats = ibtracs.load_all_adm_wind_stats()
df_stats_cub = df_stats[df_stats["ADM_PCODE"].str.contains("CU")]
```

```python
df_stats_cub
```

```python
CERF_SIDS = [
    "2017242N16333",
    "2008238N13293",
    "2016273N13300",
    "2012296N14283",
    "2008245N17323",
    "2022266N12294",
]
```

```python
df_stats_cub_max = (
    df_stats_cub.groupby("sid")["adm_wind"]
    .max()
    .reset_index()
    .sort_values("adm_wind", ascending=False)
    .merge(df_sid_names)
)
df_stats_cub_max["cerf"] = df_stats_cub_max["sid"].isin(CERF_SIDS)
df_stats_cub_max.iloc[:20]
```

```python
df_stats_cub_max
```

```python
adm_hti = adm_all[adm_all["ADM_PCODE"] == "HT"]
```

```python
adm_aoi.plot()
```

```python
adm_hti.to_crs(3857).area.sum() / 1000**2
```

```python
adm_aoi.to_crs(3857).area.sum() / 1000**2
```

```python
df_monitoring = monitoring_utils.load_existing_monitoring_points(
    "fcast", "all"
)
```

```python
df_monitoring_rafael = df_monitoring[
    df_monitoring["atcf_id"] == RAFAEL_ATCF_ID
]
```

```python
df_monitoring_rafael.groupby("ADM_PCODE")[
    "max_adm_wind"
].max().reset_index().sort_values("max_adm_wind", ascending=False).iloc[:20]
```

```python
df_obsv = nhc.load_recent_glb_obsv()
```

```python
df_obsv_rafael = df_obsv[df_obsv["id"] == RAFAEL_ATCF_ID]
```

```python
cols = ["latitude", "longitude", "intensity"]
df_interp = (
    df_obsv_rafael.set_index("lastUpdate")[cols]
    .resample("30min")
    .interpolate()
    .reset_index()
)
df_interp
gdf = gpd.GeoDataFrame(
    df_interp,
    geometry=gpd.points_from_xy(df_interp.longitude, df_interp.latitude),
    crs="EPSG:4326",
).to_crs(3857)
```

```python
dicts = []

for pcode, row in adm_aoi.to_crs(3857).set_index("ADM_PCODE").iterrows():
    gdf[f"distance_{pcode}"] = gdf.geometry.distance(row.geometry) / 1000
    gdf[f"adm_wind_{pcode}"] = estimate_wind_at_distance(
        gdf["intensity"], gdf[f"distance_{pcode}"]
    )
    dicts.append(
        {"ADM_PCODE": pcode, "adm_max_wind": gdf[f"adm_wind_{pcode}"].max()}
    )

df_obsv_monitoring = pd.DataFrame(dicts)
```

```python
gdf[gdf["distance_CU09"] < 100]
```

```python
df_obsv_monitoring.merge(adm_aoi[["ADM_PCODE", "ADM_NAME"]]).sort_values(
    "adm_max_wind", ascending=False
)
```

```python
df_obsv_monitoring_max = pd.DataFrame(
    [
        {
            "sid": "RAFAEL",
            "adm_wind": df_obsv_monitoring["adm_max_wind"].max(),
            "name": "RAFAEL",
            "year": 2024,
            "nameyear": "Rafael 2024",
            "cerf": False,
        }
    ]
)
```

```python
wind_col_name = "Max wind Cuba<br>(knots)"

df_wind_merge = pd.concat(
    [df_obsv_monitoring_max, df_stats_cub_max], ignore_index=True
).sort_values("adm_wind", ascending=False)
df_wind_merge[wind_col_name] = df_wind_merge["adm_wind"].apply(round)
```

```python
df_wind_merge
```

```python
def color_rows(row):
    style = None
    if row["cerf"]:
        style = ["background-color: lightcoral" for _ in row]
    elif row["nameyear"] == "Rafael 2024":
        style = ["background-color: skyblue" for _ in row]
    return style


cols = ["nameyear", wind_col_name, "cerf"]
df_wind_merge.iloc[:20][cols].style.apply(color_rows, axis=1)
```

```python

```

```python

```
