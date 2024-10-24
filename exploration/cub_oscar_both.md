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

# Cuba- Oscar wind and rain comparison

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import matplotlib.pyplot as plt

from src.datasources import codab, ibtracs, nhc, imerg
from src.utils import blob
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
df_sid_names = ibtracs.load_sid_names()
```

```python
df_windstats = ibtracs.load_all_adm_wind_stats()
df_windstats = df_windstats[df_windstats["ADM_PCODE"].str.contains("CU")]
```

```python
df_windstats_cub_max = (
    df_windstats.groupby("sid")["adm_wind"]
    .max()
    .reset_index()
    .sort_values("adm_wind", ascending=False)
    .merge(df_sid_names)
)
df_windstats_cub_max["cerf"] = df_windstats_cub_max["sid"].isin(CERF_SIDS)
```

```python
# blob_name = (
#     f"{blob.PROJECT_PREFIX}/processed/imerg/cub_imerg_nobuffer_q9_agg.parquet"
# )
blob_name = f"{blob.PROJECT_PREFIX}/processed/imerg/cub_imerg_nobuffer_multi_agg.parquet"
df_rain = blob.load_parquet_from_blob(blob_name)
```

```python
df_rain
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/processed/imerg/cub_imerg_nobuffer_multi_sum_agg.parquet"
df_rain_sum = blob.load_parquet_from_blob(blob_name)
```

```python
df_rain_sum
```

```python
compare_var = "max"
compare_label = "maximum one-day rainfall anywhere in Cuba (mm)"
# compare_label = "maximum total rainfall anywhere in Cuba (mm)"
# compare_label = "2-day rolling sum of\n90th quantile rainfall over Cuba (mm)"
```

```python
df_rain_max = df_rain.groupby("sid")[compare_var].max().reset_index()
```

```python
df_stats_cub_max = df_windstats_cub_max.merge(df_rain_max)
# df_stats_cub_max = df_windstats_cub_max.merge(df_rain_sum)
```

```python
df_stats_cub_max
```

```python
# max sum rainfall
# oscar_values = (70, 676)

# max max rainfall
oscar_values = (70, 436)
```

```python
fig, ax = plt.subplots(dpi=200)
df_stats_cub_max[df_stats_cub_max["cerf"]].plot(
    x="adm_wind",
    y=compare_var,
    ax=ax,
    marker=".",
    linewidth=0,
    color="crimson",
    label="CERF allocation",
)
df_stats_cub_max[~df_stats_cub_max["cerf"]].plot(
    x="adm_wind",
    y=compare_var,
    ax=ax,
    marker=".",
    linewidth=0,
    color="dodgerblue",
    label="no CERF",
)
for nameyear, row in df_stats_cub_max.groupby("nameyear"):
    if (
        (row["adm_wind"].iloc[0] > df_stats_cub_max["adm_wind"].quantile(0.8))
        or (
            row[compare_var].iloc[0]
            > df_stats_cub_max[compare_var].quantile(0.9)
        )
        or (row["cerf"].iloc[0])
    ):
        ha, va = "left", "center"
        if nameyear == "Matthew 2016":
            va = "bottom"
        if nameyear == "Ivan 2004":
            va = "top"
        if nameyear == "Ian 2022":
            ha = "right"
        ax.annotate(
            f" {nameyear} ",
            (row["adm_wind"].iloc[0], row[compare_var].iloc[0]),
            fontsize=7,
            ha=ha,
            va=va,
        )
ax.plot(
    *oscar_values,
    marker=".",
    linewidth=0,
    color="green",
)
ax.annotate(
    " Oscar 2024 ",
    oscar_values,
    fontsize=10,
    ha="left",
    va="center",
    color="green",
)

ax.set_xlabel("Maximium wind in Cuba (knots)")
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.set_ylabel(compare_label)
```

```python

```

```python
float(row["adm_wind"].iloc[0])
```

```python
df_stats_cub_max["adm_wind"].quantile(0.9)
```
