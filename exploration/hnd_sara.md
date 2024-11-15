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

# Honduras - Sara 2024

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd
import geopandas as gpd
import xarray as xr
import matplotlib.pyplot as plt
from datetime import datetime

from src.datasources import chirps, codab, ibtracs, chirps_gefs
from src.constants import *
```

```python
adm = codab.load_codab_from_blob("hnd", admin_level=2)
adm1 = codab.load_codab_from_blob("hnd", admin_level=1)
```

```python
da_eta_iota = chirps.open_chirps_daily("hnd")["prcp"].rio.write_crs(4326)
da_eta_iota["T"] = pd.to_datetime(da_eta_iota["T"].values)
```

```python
datetime.combine(datetime.today().date(), datetime.min.time())
```

```python
issue_date = datetime.combine(datetime.today().date(), datetime.min.time())

das = []
for leadtime in range(16):
    valid_date = issue_date + pd.DateOffset(days=leadtime)
    da_in = chirps_gefs.open_chirps_gefs(issue_date, valid_date)
    da_in = da_in.squeeze(drop=True).persist()
    da_in["valid_date"] = valid_date
    da_in["issue_date"] = issue_date
    da_in = da_in.expand_dims(["valid_date", "issue_date"])
    das.append(da_in)
```

```python
da = xr.combine_by_coords(das, combine_attrs="drop_conflicts")
```

```python
da_clip0 = da.rio.clip(adm.geometry, all_touched=True)
```

```python
da_clip0.isel(valid_date=0).plot()
```

```python
adm.plot()
```

```python
dfs = []
for pcode, group in adm.groupby("ADM2_PCODE"):
    print(pcode)
    try:
        da_clip2 = da_clip0.rio.clip(group.geometry)
    except Exception as e:
        print(f"error for {pcode}: {e}")
        continue
    da_mean = da_clip2.mean(dim=["x", "y"])
    df_out = da_mean.to_dataframe("mean").reset_index()
    df_out["ADM2_PCODE"] = pcode
    dfs.append(df_out)
```

```python
df_gefs = pd.concat(dfs, ignore_index=True).drop(columns=["spatial_ref"])
```

```python
df_gefs
```

```python
da_sum = da_clip0.sum(dim=["valid_date"])
```

```python
da_sum
```

```python
start_sara = "2024-11-14"
end_sara = "2024-11-18"
```

```python
da_sum = (
    da_clip0.sel(valid_date=slice(start_sara, end_sara))
    .sum(dim=["valid_date"])
    .squeeze(drop=True)
)
```

```python
levels = [25, 50, 100, 150, 200, 300, 400, 500, 750]
colors = [
    "lawngreen",
    "limegreen",
    "yellow",
    "gold",
    "darkorange",
    "red",
    "firebrick",
    "magenta",
    "darkmagenta",
]
cbar_kwargs = {
    "label": "Precipitation (mm)",  # Set label for the colorbar
    "shrink": 0.8,  # Shrink the colorbar to 80% of its default size
}
```

```python
fig, ax = plt.subplots(dpi=300, figsize=(10, 5))

da_sum.rio.clip(adm1.geometry, all_touched=True).plot.contourf(
    ax=ax, levels=levels, colors=colors, extend="max", cbar_kwargs=cbar_kwargs
)
adm1.boundary.plot(ax=ax, linewidth=0.5, color="k")
for _, row in adm1.iterrows():
    ax.annotate(
        row["ADM1_ES"],
        (row.geometry.centroid.x, row.geometry.centroid.y),
        fontsize=7,
        va="center",
        ha="center",
    )

ax.axis("off")
ax.set_title(
    f"Total forecast rainfall for Sara in Honduras\n{start_sara} to {end_sara}"
)
```

```python
start_eta = pd.to_datetime("2020-11-02")
end_eta = pd.to_datetime("2020-11-07") + pd.DateOffset(days=1)

da_eta_sum = da_eta_iota.where(
    (da_eta_iota["T"] >= start_eta) & (da_eta_iota["T"] <= end_eta), drop=True
).sum(dim="T")
```

```python
fig, ax = plt.subplots(dpi=300, figsize=(10, 5))

da_eta_sum.rio.clip(adm1.geometry, all_touched=True).plot.contourf(
    ax=ax, levels=levels, colors=colors, extend="max", cbar_kwargs=cbar_kwargs
)
adm1.boundary.plot(ax=ax, linewidth=0.5, color="k")
ax.axis("off")

for _, row in adm1.iterrows():
    ax.annotate(
        row["ADM1_ES"],
        (row.geometry.centroid.x, row.geometry.centroid.y),
        fontsize=7,
        va="center",
        ha="center",
    )

ax.axis("off")
ax.set_title(
    f"Total rainfall for Eta 2020 in Honduras\n{start_eta.date()} to {end_eta.date()}"
)
```

```python
start_iota = pd.to_datetime("2020-11-16")
end_iota = pd.to_datetime("2020-11-18") + pd.DateOffset(days=1)

da_iota_sum = da_eta_iota.where(
    (da_eta_iota["T"] >= start_iota) & (da_eta_iota["T"] <= end_iota),
    drop=True,
).sum(dim="T")
```

```python
fig, ax = plt.subplots(dpi=300, figsize=(10, 5))

da_iota_sum.rio.clip(adm1.geometry, all_touched=True).plot.contourf(
    ax=ax, levels=levels, colors=colors, extend="max", cbar_kwargs=cbar_kwargs
)
adm1.boundary.plot(ax=ax, linewidth=0.5, color="k")
ax.axis("off")

for _, row in adm1.iterrows():
    ax.annotate(
        row["ADM1_ES"],
        (row.geometry.centroid.x, row.geometry.centroid.y),
        fontsize=7,
        va="center",
        ha="center",
    )

ax.axis("off")
ax.set_title(
    f"Total rainfall for Iota 2020 in Honduras\n{start_iota.date()} to {end_iota.date()}"
)
```

```python
stats = ibtracs.load_all_adm_wind_stats()
```

```python
stats
```

```python
stats[
    (stats["sid"] == ETA_SID) & (stats["ADM_PCODE"].str.contains("HN"))
].merge(
    adm1[["ADM1_PCODE", "ADM1_ES"]],
    left_on="ADM_PCODE",
    right_on="ADM1_PCODE",
).sort_values(
    "rp"
)
```

```python
stats[
    (stats["sid"] == IOTA_SID) & (stats["ADM_PCODE"].str.contains("HN"))
].merge(
    adm1[["ADM1_PCODE", "ADM1_ES"]],
    left_on="ADM_PCODE",
    right_on="ADM1_PCODE",
).sort_values(
    "rp"
)
```

```python

```
