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

# Monitoring testing - all

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import geopandas as gpd
import pandas as pd
from tqdm.auto import tqdm

from src.datasources import codab, nhc
from src.monitoring import monitoring_utils
from src.utils import blob
```

```python
adm = codab.load_combined_codab()
```

```python
adm.plot()
```

```python
adm
```

```python
clobber = False
verbose = False
thorough_check = False
```

```python
blob_name = monitoring_utils.get_blob_name("fcast", "all")
df_existing_monitoring = blob.load_parquet_from_blob(blob_name)
df_existing_monitoring
```

```python
df_tracks = nhc.load_recent_glb_forecasts()
df_tracks = df_tracks[df_tracks["basin"] == "al"]
```

```python
dicts = []
for issue_time, issue_group in tqdm(
    df_tracks.groupby("issuance"), disable=not verbose
):
    if not thorough_check and not clobber:
        if issue_time in df_existing_monitoring["issue_time"].to_list():
            if verbose:
                print(
                    f"not doing thorough check, {issue_time} already monitored"
                )
            continue
    for atcf_id, group in issue_group.groupby("id"):
        cols = ["latitude", "longitude", "maxwind"]
        df_interp = (
            group.set_index("validTime")[cols]
            .resample("30min")
            .interpolate()
            .reset_index()
        )
        gdf = gpd.GeoDataFrame(
            df_interp,
            geometry=gpd.points_from_xy(
                df_interp.longitude, df_interp.latitude
            ),
            crs="EPSG:4326",
        ).to_crs(3857)
        for pcode, row in adm.set_index("ADM_PCODE").iterrows():
            monitor_id = f"{atcf_id}_fcast_{issue_time.isoformat().split('+')[0]}_{pcode}"
            if (
                monitor_id in df_existing_monitoring["monitor_id"].unique()
                and not clobber
            ):
                if verbose:
                    print(f"already monitored for {monitor_id}")
                continue
            else:
                print(f"monitoring for {monitor_id}")
            gdf["distance"] = gdf.geometry.distance(row.geometry) / 1000
            gdf["leadtime"] = gdf["validTime"] - issue_time

            landfall_row = gdf.loc[gdf["distance"].idxmin()]
            time_to_landfall = landfall_row["leadtime"]
            landfall_s = landfall_row["maxwind"]

            dicts.append(
                {
                    "monitor_id": monitor_id,
                    "atcf_id": atcf_id,
                    "ADM_PCODE": pcode,
                    "name": group["name"].iloc[0],
                    "issue_time": issue_time,
                    "time_to_closest": time_to_landfall,
                    "closest_s": landfall_s,
                    "min_dist": gdf["distance"].min(),
                }
            )
```

```python
df_new_monitoring = pd.DataFrame(dicts)
```

```python
df_new_monitoring
```

```python
if clobber:
    df_monitoring_combined = df_new_monitoring
else:
    df_monitoring_combined = pd.concat(
        [df_existing_monitoring, df_new_monitoring]
    )

df_monitoring_combined = df_monitoring_combined.sort_values(
    ["issue_time", "atcf_id"]
)
blob.upload_parquet_to_blob(blob_name, df_monitoring_combined)
```

```python
df_monitoring_combined
```
