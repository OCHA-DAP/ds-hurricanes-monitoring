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

# Historical CERF

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import geopandas as gpd
import ocha_stratus as stratus
import matplotlib.pyplot as plt
import pandas as pd
import xarray as xr
from tqdm.auto import tqdm
from dask.diagnostics import ProgressBar

from src.datasources import codab, ibtracs
from src.utils.blob import PROJECT_PREFIX
from src.constants import *
```

```python
adm_all = codab.load_combined_codab()
```

```python
adm0_all = adm_all.dissolve("ADM0_PCODE").reset_index()
```

```python
query = """
SELECT *
FROM storms.ibtracs_storms
WHERE genesis_basin IN ('NA', 'EP')
"""
with stratus.get_engine(stage="prod").connect() as con:
    df_storms = pd.read_sql(query, con)
```

```python
query = """
SELECT *
FROM storms.ibtracs_tracks_geo
WHERE basin IN ('NA', 'EP')
"""
with stratus.get_engine(stage="prod").connect() as conn:
    gdf_tracks = gpd.read_postgis(query, conn, geom_col="geometry")
```

```python
gdf_tracks = gdf_tracks.merge(df_storms)
```

```python
gdf_tracks_recent = gdf_tracks[
    (gdf_tracks["season"] >= 2016) & (gdf_tracks["season"] < 2025)
]
```

```python
dicts = []
for sid, group in tqdm(gdf_tracks_recent.groupby("sid")):
    dff = group.copy()
    for pcode, row in adm0_all.to_crs(3857).set_index("ADM0_PCODE").iterrows():
        dff["distance"] = dff.to_crs(3857).distance(row.geometry) / 1000
        dff["adm_wind"] = ibtracs.estimate_wind_at_distance(
            dff["wind_speed"], dff["distance"]
        )
        max_adm_wind = dff["adm_wind"].max()
        dff_max = dff[dff["adm_wind"] == max_adm_wind]
        max_time = dff_max["valid_time"].min()
        max_row = dff_max[dff_max["valid_time"] == max_time].iloc[0]
        dicts.append(
            {
                "sid": sid,
                "ADM0_PCODE": pcode,
                "max_adm_wind_time": max_time,
                "max_adm_wind": max_adm_wind,
                "max_adm_wind_wind": max_row["wind_speed"],
                "max_adm_wind_distance": max_row["distance"],
                "min_distance": dff["distance"].min(),
            }
        )
```

```python
df_stats = pd.DataFrame(dicts)
df_stats = df_stats.merge(df_storms)
```

```python
df_stats.sort_values("max_adm_wind", ascending=False)
```

```python
len(adm0_all)
```

```python
len(df_storms)
```

```python
len(df_storms) * len(adm0_all)
```

```python
len(df_stats)
```

```python
df_stats["ADM0_PCODE"].unique()
```

```python
data = [
    ("AG", IRMA_SID),
    ("BS", DORIAN_SID),
    ("CU", MATTHEW_SID),
    ("CU", IRMA_SID),
    ("CU", IAN_SID),
    ("CU", OSCAR_SID),
    ("CU", RAFAEL_SID),
    ("DM", MARIA_SID),
    ("SV", AMANDA_SID),
    ("GD", BERYL_SID),
    ("VC", BERYL_SID),
    ("GT", ETA_SID),
    ("GT", IOTA_SID),
    ("HT", MATTHEW_SID),
    ("HN", ETA_SID),
    ("HN", IOTA_SID),
    ("JM", BERYL_SID),
    ("NI", ETA_SID),
    ("NI", IOTA_SID),
]
df_cerf = pd.DataFrame(columns=["ADM0_PCODE", "sid"], data=data)
df_cerf["cerf"] = True
```

```python
len(df_cerf)
```

```python
df_stats = df_stats.merge(df_cerf, how="left")
```

```python
df_stats["cerf"] = df_stats["cerf"].fillna(False)
```

```python
df_stats["cerf"].sum()
```

```python
min_cerf_wind = df_stats[df_stats["cerf"]]["max_adm_wind"].min()
```

```python
min_cerf_wind
```

```python
jam_adm_wind = 160
```

```python
df_stats_high = df_stats[df_stats["max_adm_wind"] >= min_cerf_wind]
```

```python
df_stats[df_stats["max_adm_wind"] >= min_cerf_wind].sort_values(
    "max_adm_wind", ascending=False
)
```

```python
df_stats["name_season"] = (
    df_stats["name"].str.capitalize()
    + " "
    + df_stats["season"].astype(int).astype(str)
)
df_stats["name_season_pcode"] = (
    df_stats["name_season"] + " (" + df_stats["ADM0_PCODE"] + ")"
)
```

```python
blob_name = (
    f"{PROJECT_PREFIX}/processed/ep_na_since2016_adm0_wind_stats.parquet"
)
stratus.upload_parquet_to_blob(df_stats, blob_name)
```

```python
df_stats_cerf = df_stats[df_stats["cerf"]]
df_stats_no_cerf = df_stats[~df_stats["cerf"]]
```

```python
df_stats
```

```python
df_stats_cerf[df_stats_cerf.duplicated(subset=["max_adm_wind"], keep=False)]
```

```python
df_stats["sid"].nunique()
```

```python
df_stats_cerf["max_adm_wind"].mean()
```

```python
df_stats_no_cerf["max_adm_wind"].mean()
```

```python
fig, ax = plt.subplots(dpi=200)
ax.hist(
    [df_stats_no_cerf["max_adm_wind"], df_stats_cerf["max_adm_wind"]],
    bins=16,
    stacked=True,
    label=["No CERF", "CERF"],
    color=["grey", "crimson"],
    alpha=0.8,
)

shift = 1.3

for _, row in df_stats_cerf.iterrows():
    adj = 0
    if row["name_season_pcode"] in ["Irma 2017 (CU)", "Eta 2020 (NI)"]:
        adj = shift
    elif row["name_season_pcode"] in ["Maria 2017 (DM)", "Beryl 2024 (GD)"]:
        adj = -shift
    ax.text(
        row["max_adm_wind"] + adj,  # x position = wind speed
        -4,  # y position just below x-axis
        row.get("name_season_pcode"),
        rotation=90,
        ha="center",
        va="top",
        fontsize=6,
        color="crimson",
    )

ax.legend()
ax.set_ylim(top=70)
ax.set_xlim(left=0)
ax.grid(False)
[ax.spines[x].set_visible(False) for x in ["top", "right"]]
ax.set_ylabel("Count of storm-country combinations")
ax.set_xlabel("Estimated max. wind speed on land (knots)", labelpad=55)
ax.plot([jam_adm_wind, jam_adm_wind], [0, 10], color="darkorange")
ax.annotate(
    " Melissa 2025 (JM)",
    [jam_adm_wind, 10],
    color="darkorange",
    rotation=90,
    ha="center",
    va="bottom",
)

ax.annotate(
    "",  # no text, just arrow
    xy=(5, 1.05),  # arrow tip just above top of axes
    xytext=(5, 0.95),  # tail inside the plot
    xycoords=(
        "data",
        "axes fraction",
    ),  # x in data coords, y as fraction of axes
    arrowprops=dict(
        arrowstyle="->",
        color="dimgrey",
        lw=1.5,
    ),
    annotation_clip=False,  # ensure it’s always drawn
)

ax.annotate(
    "Actual value\n≈ 7,000",  # no text, just arrow
    xy=(5, 1.05),  # arrow tip just above top of axes
    xycoords=(
        "data",
        "axes fraction",
    ),  # x in data coords, y as fraction of axes
    ha="center",
    va="bottom",
    rotation=90,
    fontstyle="italic",
    fontsize=6,
    color="dimgrey",
    annotation_clip=False,  # ensure it’s always drawn
)

ax.set_title(
    "Maximum wind speed on land, per country,\nof EP and NA basin storms\n"
    "Seasons: 2016-2024"
)
```

```python
adm0_all.columns
```

```python
adm0_all["ADM0_NAME"] = (
    adm0_all["ADM0_EN"]
    .fillna(adm0_all["ADM0_ES"])
    .fillna(adm0_all["ADM0_FR"])
    .fillna(adm0_all["ADM0_HT"])
)
```

```python
adm0_all[["ADM0_PCODE", "ADM0_NAME"]].values
```

```python
df_out = df_stats.merge(adm0_all[["ADM0_PCODE", "ADM0_NAME"]])
```

```python
df_out
```

```python
out_path = "temp/ep_na_since2016_adm0_wind_stats.csv"
df_out.to_csv(out_path, index=False)
```

```python
cols = ["name_season_pcode", "max_adm_wind", "cerf"]
df_disp = df_stats.sort_values("max_adm_wind", ascending=False)[cols]
df_disp["max_adm_wind"] = df_disp["max_adm_wind"].astype(int)
df_disp.iloc[:20]
```

```python
df_stats.sort_values("max_adm_wind", ascending=False).iloc[:20]
```
