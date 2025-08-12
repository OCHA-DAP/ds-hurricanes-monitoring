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

# Havana stats

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import numpy as np
import pandas as pd
import geopandas as gpd
import pytz
import matplotlib.pyplot as plt

from src.datasources import ibtracs, nhc, codab
from src.monitoring import monitoring_utils
from src.utils import blob
from src.constants import PROJ_CRS, CHD_GREEN
from src.email.plotting import convert_datetime_to_str
```

```python
from src.email.utils import (
    TEST_STORM,
    add_test_row_to_monitoring,
    TEST_FCAST_MONITOR_ID,
    TEST_OBSV_MONITOR_ID,
    TEST_ATCF_ID,
    open_static_image,
)
```

```python
# ibtracs.process_havana_distances()
```

```python
# ibtracs.calculate_havana_windstats()
```

```python
adm = codab.load_codab_from_blob("cub", aoi_only=True)
```

```python
wind_stats = ibtracs.load_havana_wind_stats()
```

```python
monitor_id = "TEST_FCAST_MONITOR_ID"
fcast_obsv = "fcast"
```

```python
df_monitoring = monitoring_utils.load_existing_monitoring_points(
    fcast_obsv, "cub"
)
if monitor_id in [TEST_FCAST_MONITOR_ID, TEST_OBSV_MONITOR_ID]:
    df_monitoring = add_test_row_to_monitoring(df_monitoring, fcast_obsv)
monitoring_point = df_monitoring.set_index("monitor_id").loc[monitor_id]
ny_tz = pytz.timezone("America/New_York")
cyclone_name = monitoring_point["name"]
atcf_id = monitoring_point["atcf_id"]
if atcf_id == TEST_ATCF_ID:
    atcf_id = "al042024"
issue_time = monitoring_point["issue_time"]
issue_time_ny = issue_time.astimezone(ny_tz)

if fcast_obsv == "fcast":
    df_tracks = nhc.load_recent_glb_forecasts()
    tracks_f = df_tracks[
        (df_tracks["id"] == atcf_id) & (df_tracks["issuance"] == issue_time)
    ].copy()
```

```python
issue_time_str = convert_datetime_to_str(issue_time_ny)
fcast_obsv_str = "observations" if fcast_obsv == "obsv" else "forecast"
no_pass_text = (
    "did not pass" if fcast_obsv == "obsv" else "not forecast to pass"
)
```

```python
df_interp = (
    tracks_f.set_index("validTime")[["latitude", "longitude", "maxwind"]]
    .resample("30min")
    .interpolate(method="linear")
)
```

```python
gdf = gpd.GeoDataFrame(
    df_interp,
    geometry=gpd.points_from_xy(df_interp.longitude, df_interp.latitude),
    crs="EPSG:4326",
)
gdf["havana_distance_km"] = (
    gdf.to_crs(PROJ_CRS).geometry.distance(
        adm.to_crs(PROJ_CRS).iloc[0].geometry
    )
    / 1000
)
```

```python
gdf
```

```python
dicts = []
for d_thresh in range(0, 501, 1):
    gdff = gdf[gdf["havana_distance_km"] <= d_thresh]
    dicts.append(
        {
            "max_wind": gdff["maxwind"].max(),
            "d": d_thresh,
        }
    )

current_wind_stats = pd.DataFrame(dicts).dropna()
```

```python
current_wind_stats["max_wind"].max()
```

```python
current_wind_stats["d"].min()
```

```python
monitoring_point
```

```python
x_cutoff = 100
y_cutoff = 10

ymax = 200

fig, ax = plt.subplots(figsize=(8, 8), dpi=300)

for nameyear, group in wind_stats.groupby("nameyear"):
    if group["d"].min() > ymax * 0.95:
        continue
    ax.plot(group["max_wind"], group["d"], alpha=0.5, color="grey")
    ax.annotate(
        nameyear + "  ",
        (group["max_wind"].min(), group["d"].min()),
        ha="center",
        va="bottom",
        fontsize=7,
        rotation=270,
    )

ax.plot(
    current_wind_stats["max_wind"],
    current_wind_stats["d"],
    linewidth=3,
    alpha=1,
    color=CHD_GREEN,
)
current_xy = (
    current_wind_stats["max_wind"].min(),
    current_wind_stats["d"].min(),
)
ax.annotate(
    f"  {cyclone_name}\n\n",
    current_xy,
    ha="left",
    va="center",
    fontsize=10,
    color=CHD_GREEN,
    fontweight="bold",
)
ax.annotate(
    f"\n  {fcast_obsv_str} issued\n  {issue_time_str}",
    current_xy,
    va="center",
    ha="left",
    fontsize=10,
    color=CHD_GREEN,
    fontstyle="italic",
)

ax.axvline(x=x_cutoff, color="lightgray", linestyle="--", linewidth=0.5)
ax.axhline(y=y_cutoff, color="lightgray", linestyle="--", linewidth=0.5)
ax.fill_between(
    np.arange(x_cutoff, 200, 1),
    0,
    y_cutoff,
    color="gold",
    alpha=0.2,
    zorder=-1,
)
ax.annotate(
    "Trigger zone ",
    (155, 1),
    va="bottom",
    ha="right",
    color="orange",
    fontsize=10,
    fontweight="bold",
)

if monitoring_point["min_dist"] >= ymax:
    rect = plt.Rectangle(
        (0, 0),
        1,
        1,
        transform=ax.transAxes,
        color="white",
        alpha=0.7,
        zorder=3,
    )
    ax.add_patch(rect)
    ax.text(
        0.5,
        0.5,
        f"{cyclone_name} {no_pass_text}\n" f"within 200 km of Havana",
        fontsize=20,
        color="grey",
        ha="center",
        va="center",
        transform=ax.transAxes,
    )

ax.set_xlim(right=155, left=0)
ax.set_ylim(top=ymax, bottom=0)

ax.set_xlabel("Maximum windspeed while within distance (knots)")
ax.set_ylabel("Distance to Havana (km)")
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.set_title("Comparison with historical wind speeds and distances")
```

```python

```
