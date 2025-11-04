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

# Haiti Melissa

Comparing rainfall in Haiti for Melissa.

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
from datetime import datetime

import pandas as pd
import ocha_stratus as stratus
import xarray as xr
import matplotlib.pyplot as plt
from dask.diagnostics import ProgressBar
from tqdm.auto import tqdm

from src.datasources import codab, chirps, chirps_gefs
from src.constants import *
```

```python
adm0 = codab.load_codab_from_blob("hti")
```

```python
adm1 = codab.load_codab_from_blob("hti", admin_level=1)
```

```python
adm2 = codab.load_codab_from_blob("hti", admin_level=2)
```

```python
adm0.plot()
```

```python
sandy_dates = pd.date_range("2012-10-22", "2012-10-28")
```

```python
sandy_dates
```

```python
das = []
for d in sandy_dates:
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
rainfall_dates = pd.date_range("2012-10-23", "2012-10-26")
```

```python
da_sum = da_clip_computed.sum(dim="date")
```

```python
da_sum_sel = da_clip_computed.sel(date=rainfall_dates).sum(dim="date")
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
    "label": "Précipitations (mm)",  # Set label for the colorbar
    "shrink": 0.8,  # Shrink the colorbar to 80% of its default size
}
```

```python
num_days = len(rainfall_dates)
```

```python
fig, ax = plt.subplots(figsize=(8, 4))
adm0.boundary.plot(ax=ax, color="k")
adm1.boundary.plot(ax=ax, color="k", linewidth=0.5)
da_sum_sel.plot(
    ax=ax, levels=levels, colors=colors, extend="max", cbar_kwargs=cbar_kwargs
)
ax.axis("off")
ax.set_title(
    f"Précip. totales sur {num_days} jours de Sandy (mm) [CHIRPS]\n"
    f"Dates : {rainfall_dates.min().date()} à {rainfall_dates.max().date()}"
)
```

```python
issued_date = datetime(2025, 10, 30).date()

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
da_gefs_mean = da_gefs_clip.mean(dim=["x", "y"])
```

```python
da_gefs_mean.plot()
```

```python
da_gefs_mean
```

```python
gefs_dates = pd.date_range("2025-10-30", "2025-10-30")
```

```python
da_gefs_sel = da_gefs_clip.sel(valid_date=gefs_dates)
```

```python
da_gefs_sel
```

```python
da_gefs_sel.mean(dim=["x", "y"]).plot()
```

```python
da_gefs_sum_sel = da_gefs_sel.sum(dim="valid_date")
```

```python
num_gefs_dates = len(gefs_dates)
```

```python
fig, ax = plt.subplots(figsize=(8, 4), dpi=200)
adm0.boundary.plot(ax=ax, color="k")
adm1.boundary.plot(ax=ax, color="k", linewidth=0.5)
da_gefs_sum_sel.plot(
    ax=ax, levels=levels, colors=colors, extend="max", cbar_kwargs=cbar_kwargs
)
ax.axis("off")
ax.set_title(
    f"Précip. totales prévues sur {num_gefs_dates} jours pour Melissa (mm) [CHIRPS-GEFS]\n"
    f"Dates valides : {gefs_dates.min().date()} à {gefs_dates.max().date()}\n"
    f"Date d'émission : {issued_date}"
)
```

```python
GONAIVES2 = "HT0511"
```

```python
adm2[adm2["ADM2_PCODE"] == GONAIVES2]
```

```python
fig, ax = plt.subplots(figsize=(8, 4), dpi=200)
adm0.boundary.plot(ax=ax, color="k")
adm1.boundary.plot(ax=ax, color="k", linewidth=0.5)
# adm2[adm2["ADM2_PCODE"] == GONAIVES2].boundary.plot(
#     color="red", ax=ax, linestyle="--", linewidth=1
# )
da_gefs_sum_sel.plot(
    ax=ax, levels=levels, colors=colors, extend="max", cbar_kwargs=cbar_kwargs
)
ax.axis("off")
ax.set_title(
    f"Précip. totales prévues sur {num_gefs_dates} jours pour Melissa (mm) [CHIRPS-GEFS]\n"
    f"Dates valides : {gefs_dates.min().date()} à {gefs_dates.max().date()}\n"
    f"Date d'émission : {issued_date}"
)
```

```python
# get IMERG
query = """
SELECT *
FROM public.imerg
WHERE pcode = 'HT'
"""
with stratus.get_engine(stage="prod").connect() as con:
    df_imerg = pd.read_sql(query, con)
```

```python
df_imerg["valid_date"] = pd.to_datetime(df_imerg["valid_date"])
df_imerg = df_imerg.sort_values("valid_date")
```

```python
for window in [2, 3]:
    df_imerg[f"roll{window}_mean"] = (
        df_imerg["mean"].rolling(window=window).sum()
    )
```

```python
df_imerg.iloc[-10:]
```

```python
IMERG_BLOB_NAME = (
    "imerg/daily/late/v7/processed/imerg-daily-late-{date_str}.tif"
)
```

```python
imerg_dates = df_imerg.iloc[-3:]["valid_date"].dt.date
```

```python
imerg_dates
```

```python
das = []
for d in imerg_dates:
    blob_name = IMERG_BLOB_NAME.format(date_str=d)
    da_in = stratus.open_blob_cog(
        blob_name, stage="prod", container_name="raster"
    )
    da_in["date"] = d
    das.append(da_in)
```

```python
da_imerg = xr.concat(das, dim="date").squeeze(drop=True)
```

```python
da_imerg_clip_box = da_imerg.rio.clip_box(*total_bounds)
```

```python
with ProgressBar():
    da_imerg_clip_box_computed = da_imerg_clip_box.compute()
```

```python
da_imerg_clip = da_imerg_clip_box_computed.rio.clip(adm0.geometry)
```

```python
da_imerg_clip_sum = da_imerg_clip.where(da_imerg_clip > 0).sum(dim="date")
```

```python
da_imerg_clip_sum.where(da_imerg_clip_sum > 0).mean()
```

```python
da_imerg_clip_sum.where(da_imerg_clip_sum > 0).plot()
```

```python
num_imerg_dates = len(imerg_dates)
```

```python
fig, ax = plt.subplots(figsize=(8, 4), dpi=200)
adm0.boundary.plot(ax=ax, color="k")
adm1.boundary.plot(ax=ax, color="k", linewidth=0.5)
# adm2[adm2["ADM2_PCODE"] == GONAIVES2].boundary.plot(
#     color="red", ax=ax, linestyle="--", linewidth=1
# )
da_imerg_clip_sum.plot(
    ax=ax, levels=levels, colors=colors, extend="max", cbar_kwargs=cbar_kwargs
)
ax.axis("off")
ax.set_title(
    f"Précip. totales observées sur {num_imerg_dates} jours pour Melissa (mm) [IMERG]\n"
    f"Dates : {imerg_dates.min()} à {imerg_dates.max()}\n"
)
```

```python

```
