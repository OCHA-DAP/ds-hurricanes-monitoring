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

# CHIRPS-GEFS

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt

from tqdm.auto import tqdm

from src.datasources import codab, chirps_gefs
```

```python
adm = codab.load_combined_codab()
```

```python
adm.plot()
```

```python
issue_date_range = pd.date_range(
    start="2024-08-20", end="2024-08-21", freq="D"
)
```

```python
verbose = True

das = []
for issue_date in tqdm(issue_date_range):
    for leadtime in range(16):
        valid_date = issue_date + pd.Timedelta(days=leadtime)
        da_in = chirps_gefs.open_chirps_gefs(issue_date, valid_date)
        da_in = da_in.squeeze(drop=True)
        da_in["valid_date"] = valid_date
        da_in["issue_date"] = issue_date
        da_in = da_in.expand_dims(["valid_date", "issue_date"])
        # da_in = da_in.persist()
        das.append(da_in)
```

```python
da = xr.combine_by_coords(das, combine_attrs="drop_conflicts")
```

```python
da
```

```python
da.isel(valid_date=0, issue_date=0)
```

```python
pcode = adm.iloc[2]["ADM_PCODE"]
adm_f = adm[adm["ADM_PCODE"] == pcode]

fig, ax = plt.subplots()
adm_f.boundary.plot(ax=ax)
da.rio.clip(adm_f.geometry).isel(valid_date=0, issue_date=0).plot(ax=ax)
```

```python
df = da.rio.clip(adm_f.geometry).mean(dim=["x", "y"]).to_dataframe(name="mean")
```

```python
da.close()
```

```python

```
