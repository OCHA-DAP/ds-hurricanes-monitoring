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

# Return period for all

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
from tqdm.auto import tqdm

from src.datasources import ibtracs, codab
from src.utils import blob
from src.constants import *
from src.monitoring import monitoring_utils
```

```python
monitoring_utils.update_all_fcast_monitoring(
    clobber=True, disable_progress_bar=False
)
```

```python
ibtracs.process_sid_names()
```

```python
df_stats = ibtracs.load_all_adm_wind_stats()
```

```python
df_monitoring = monitoring_utils.load_existing_monitoring_points(
    "fcast", "all"
)
```

```python
df_monitoring[df_monitoring["adm_wind_rp"] > 10]
```

```python

```

```python
pcode = "HT"
thresh = 0
```

```python
df_adm = df_stats[df_stats["ADM_PCODE"] == pcode]
df_adm = df_adm.sort_values("rp")
```

```python
est_rp = np.interp(
    thresh, df_adm["adm_wind"], df_adm["rp"], right=np.inf, left=0.0
)
```

```python
f"{est_rp:0.1f}"
```

```python
df_sid_name = ibtracs.load_sid_names()
```

```python
annotate_thresh = 3

fig, ax = plt.subplots()
df_plot.plot(x="rp", y="adm_wind", ax=ax)
df_annotate = df_plot[df_plot["rp"] >= annotate_thresh]
for (
    nameyear,
    row,
) in df_annotate.set_index("nameyear").iterrows():
    ax.annotate(nameyear, (row["rp"], row["adm_wind"]))
```

```python
def calc_rp(group):
    group["rp"] = group["adm_wind"].apply(
        lambda x: (total_years + 1) / len(group[group["adm_wind"] >= x])
    )
    return group
```

```python
df_stats = (
    df_stats.groupby("ADM_PCODE")
    .apply(calc_rp, include_groups=False)
    .reset_index(level=0)
)
```

```python
df_stats
```

```python
blob_name = (
    f"{blob.PROJECT_PREFIX}/processed/ibtracs/all_adm_wind_stats.parquet"
)
blob.upload_parquet_to_blob(blob_name, df_stats)
```

```python
pcode = "CU09"
df_plot = df_stats[df_stats["ADM_PCODE"] == pcode]

annotate_thresh = 3

fig, ax = plt.subplots()
df_plot.plot(x="rp", y="adm_wind", ax=ax)
df_annotate = df_plot[df_plot["rp"] >= annotate_thresh]
for (
    nameyear,
    row,
) in df_annotate.set_index("nameyear").iterrows():
    ax.annotate(nameyear, (row["rp"], row["adm_wind"]))
```

```python
thresh = 10
(total_years + 1) / len(df_stats[df_stats["adm_wind"] > thresh])
```

```python

```
