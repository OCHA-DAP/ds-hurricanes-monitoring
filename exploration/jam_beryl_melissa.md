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

# Jamaica Beryl-Melissa

Comparing rainfall in Jamaica for Beryl and Melissa.

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
from datetime import datetime

import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
from dask.diagnostics import ProgressBar
from tqdm.auto import tqdm

from src.datasources import codab, chirps, chirps_gefs
```

```python
adm0 = codab.load_codab_from_blob("jam")
```

```python
adm0.plot()
```

```python
beryl_dates = pd.date_range("2024-07-01", "2024-07-07")
```

```python
beryl_dates
```

```python
das = []
for d in beryl_dates:
    da_in = chirps.open_chirps_cog(d)
    da_in["date"] = d
    das.append(da_in)
```

```python
da = xr.concat(das, dim="date").squeeze(drop=True)
```

```python
da_clip = da.rio.clip(adm0.geometry)
```

```python
with ProgressBar():
    da_clip_computed = da_clip.compute()
```

```python
da_mean = da_clip_computed.mean(dim=["x", "y"])
```

```python
da_clip_computed.isel(date=0).plot()
```

```python
da_mean.plot()
```

```python
rainfall_dates = pd.date_range("2024-07-02", "2024-07-05")
```

```python
da_sum = da_clip_computed.sum(dim="date")
```

```python
da_sum_three = da_clip_computed.sel(date=rainfall_dates).sum(dim="date")
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
num_days = len(rainfall_dates)
```

```python
fig, ax = plt.subplots(figsize=(8, 3))
adm0.boundary.plot(ax=ax, color="k")
da_sum_three.plot(
    ax=ax, levels=levels, colors=colors, extend="max", cbar_kwargs=cbar_kwargs
)
ax.axis("off")
ax.set_title(
    f"{num_days}-day rainfall from Beryl (mm) [CHIRPS]\n"
    f"Dates: {rainfall_dates.min().date()} to {rainfall_dates.max().date()}"
)
```

```python
issued_date = datetime(2025, 10, 28).date()

das = []
for lt in tqdm(range(16)):
    valid_date = issued_date + pd.Timedelta(days=lt)
    da_in = chirps_gefs.open_chirps_gefs(issued_date, valid_date)
    da_in["valid_date"] = valid_date
    das.append(da_in)
```

```python
da_gefs = xr.concat(das, dim="valid_date").squeeze(drop=True)
```

```python
da_gefs
```

```python
total_bounds = adm0.total_bounds
```

```python
total_bounds
```

```python
da_gefs_clip_box = da_gefs.rio.clip_box(*total_bounds)
```

```python
with ProgressBar():
    da_gefs_clip_box_computed = da_gefs_clip_box.compute()
```

```python
da_gefs_clip = da_gefs_clip_box_computed.rio.clip(adm0.geometry)
```

```python
da_gefs_clip
```

```python
da_gefs_mean_new = da_gefs_clip.mean(dim=["x", "y"])
```

```python
df_mean_new = da_gefs_mean_new.to_dataframe("mean")["mean"].reset_index()
df_mean_new["roll2_mean"] = df_mean_new["mean"].rolling(2).sum()
```

```python
df_mean_new["roll2_mean"].max()
```

```python
da_gefs_mean = da_gefs_clip.mean(dim=["x", "y"])
```

```python
da_gefs_mean.plot()
```

```python
df_mean = da_gefs_mean.to_dataframe("mean")["mean"].reset_index()
df_mean["roll2_mean"] = df_mean["mean"].rolling(2).sum()
```

```python
df_mean["roll2_mean"].max()
```

```python
gefs_dates = pd.date_range("2025-10-28", "2025-10-29")
```

```python
da_gefs_sel = da_gefs_clip.sel(valid_date=gefs_dates)
```

```python
da_gefs_sel.mean(dim=["x", "y"]).plot()
```

```python
da_gefs_sum = da_gefs_sel.sum(dim="valid_date")
```

```python
num_gefs_days = len(gefs_dates)
```

```python
fig, ax = plt.subplots(figsize=(8, 3))
adm0.boundary.plot(ax=ax, color="k")
da_gefs_sum.plot(
    ax=ax, levels=levels, colors=colors, extend="max", cbar_kwargs=cbar_kwargs
)
ax.axis("off")
ax.set_title(
    f"Forecasted {num_gefs_days}-day rainfall for Melissa (mm) [CHIRPS-GEFS]\n"
    f"Valid dates: {gefs_dates.min().date()} to {gefs_dates.max().date()}\n"
    f"Issued date: {issued_date}"
)
```

```python

```
