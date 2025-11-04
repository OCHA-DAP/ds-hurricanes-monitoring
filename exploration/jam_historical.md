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

# Jamaica historical plot

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

from src.datasources import codab, chirps, ibtracs
from src.utils.blob import PROJECT_PREFIX
from src.constants import *
```

```python
blob_name = f"{PROJECT_PREFIX}/processed/emdat/jam_emdat.parquet"
df_emdat = stratus.load_parquet_from_blob(blob_name)
```

```python
query = """
SELECT *
FROM storms.ibtracs_storms
WHERE genesis_basin = 'NA'
"""
with stratus.get_engine(stage="dev").connect() as con:
    df_storms = pd.read_sql(query, con)
```

```python
query = """
SELECT *
FROM storms.ibtracs_tracks_geo
WHERE basin = 'NA'
"""
with stratus.get_engine(stage="dev").connect() as conn:
    gdf_tracks = gpd.read_postgis(query, conn, geom_col="geometry")
```

```python
adm0 = codab.load_codab_from_blob("jam")
```

```python
adm0.plot()
```

```python
gdf_tracks = gdf_tracks.merge(df_storms)
```

```python
gdf_tracks = gdf_tracks.sort_values("valid_time")
```

```python
gdf_tracks_recent = gdf_tracks[
    (gdf_tracks["season"] >= 2000) & (gdf_tracks["season"] < 2025)
].copy()
```

```python
gdf_tracks_recent["distance"] = (
    gdf_tracks_recent.to_crs(3857).distance(adm0.to_crs(3857).geometry.iloc[0])
    / 1000
)
```

```python
gdf_tracks_recent["distance"].hist()
```

```python
d_thresh = 230
```

```python
gdf_tracks_close = gdf_tracks_recent[
    gdf_tracks_recent["distance"] <= d_thresh
].copy()
```

```python
min_date
```

```python
min_date - pd.Timedelta(days=1)
```

```python
# stack up CHIRPS
das = []
for sid, group in tqdm(gdf_tracks_close.groupby("sid")):
    min_date = group["valid_time"].dt.date.min()
    max_date = group["valid_time"].dt.date.max()
    dates = pd.date_range(
        min_date - pd.Timedelta(days=1), max_date + pd.Timedelta(days=1)
    )
    for d in dates:
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
df_rain_mean = da_mean.to_dataframe(name="mean")["mean"].reset_index()
```

```python
df_rain_mean = df_rain_mean.sort_values("date")
```

```python
df_rain_mean
```

```python
min_date - pd.Timedelta(days=1)
```

```python
gdf_tracks_close["adm_wind_speed"] = ibtracs.estimate_wind_at_distance(
    gdf_tracks_close["wind_speed"], gdf_tracks_close["distance"]
)
```

```python
# get IMERG
query = """
SELECT *
FROM public.imerg
WHERE pcode = 'JM'
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
df_imerg
```

```python
dicts = []
for sid, group in gdf_tracks_close.groupby("sid"):
    min_date = group["valid_time"].dt.date.min()
    max_date = group["valid_time"].dt.date.max()
    dff_rain = df_rain_mean[
        (df_rain_mean["date"].dt.date >= min_date - pd.Timedelta(days=1))
        & (df_rain_mean["date"].dt.date <= max_date + pd.Timedelta(days=1))
    ].copy()
    dff_imerg = df_imerg[
        (df_imerg["valid_date"].dt.date >= min_date - pd.Timedelta(days=1))
        & (df_imerg["valid_date"].dt.date <= max_date + pd.Timedelta(days=1))
    ].copy()
    for window in [2, 3]:
        dff_rain[f"roll{window}_mean"] = (
            dff_rain["mean"].rolling(window=window).sum()
        )
    dicts.append(
        {
            "sid": sid,
            "roll1_mean": dff_rain["mean"].max(),
            "roll2_mean": dff_rain["roll2_mean"].max(),
            "roll3_mean": dff_rain["roll3_mean"].max(),
            "roll1_mean_im": dff_imerg["mean"].max(),
            "roll2_mean_im": dff_imerg["roll2_mean"].max(),
            "roll3_mean_im": dff_imerg["roll3_mean"].max(),
            "wind": group["wind_speed"].max(),
            "adm_wind": group["adm_wind_speed"].max(),
        }
    )
```

```python
df_stats = pd.DataFrame(dicts)
```

```python
df_stats = df_stats.merge(df_storms[["sid", "season", "name"]])
```

```python
df_stats = df_stats.merge(df_emdat[["sid", "Total Affected"]], how="left")
```

```python
df_stats["Total Affected"] = df_stats["Total Affected"].fillna(0).astype(int)
df_stats["name"] = df_stats["name"].fillna("UNNAMED")
```

```python
cerf_sids = [BERYL_SID]
```

```python
df_stats["cerf"] = df_stats["sid"].isin(cerf_sids)
```

```python
for x in ["", "adm_"]:
    df_stats[f"{x}wind_kmh"] = df_stats[f"{x}wind"] * KNOTS_TO_KMH
```

```python
def plot_stats(
    wind_col="adm_wind_kmh",
    rain_col="roll2_mean",
    impact_col="Total Affected",
):
    cerf_color = "crimson"
    fig, ax = plt.subplots(figsize=(7, 7), dpi=200)

    ymax = df_stats[rain_col].max() * 1.1
    xmax = df_stats[wind_col].max() * 1.1

    bubble_sizes = df_stats[impact_col].fillna(0)
    bubble_sizes_scaled = bubble_sizes / bubble_sizes.max() * 5000

    ax.scatter(
        df_stats[wind_col],
        df_stats[rain_col],
        s=bubble_sizes_scaled,
        c=df_stats["cerf"].apply(lambda x: cerf_color if x else "k"),
        alpha=0.3,
        edgecolor="none",
        zorder=1,
    )

    for _, row in df_stats.iterrows():
        ax.annotate(
            row["name"].capitalize() + "\n" + str(row["season"]),
            (row[wind_col], row[rain_col]),
            ha="center",
            va="center",
            fontsize=6,
            color=cerf_color if row["cerf"] == True else "k",
            zorder=10 if row["cerf"] else 9,
            alpha=0.8,
        )

    ylabel = (
        "Two-day rainfall, mean over whole country (mm) [CHIRPS]"
        if rain_col == "roll2_mean"
        else ""
    )
    ax.set_ylabel(ylabel)
    ax.set_xlabel(
        "Estimated max. sustained wind speed on land in Jamaica (km/h) [IBTrACS]"
    )

    ax.set_xlim(left=0, right=xmax)
    ax.set_ylim(bottom=0, top=ymax)

    ax.set_title("Jamaica: historical rainfall and wind speed")

    ax.spines.top.set_visible(False)
    ax.spines.right.set_visible(False)
    return fig, ax
```

```python
df_stats
```

```python
df_imerg[df_imerg["valid_date"] >= "2025-10-26"]
```

```python
melissa_imerg = df_imerg[df_imerg["valid_date"] >= "2025-10-27"][
    "roll2_mean"
].max()
```

```python
melissa_imerg
```

```python
current_rain, current_wind = melissa_imerg, 269
fig, ax = plot_stats(rain_col="roll2_mean_im")
ax.set_ylabel("Two-day rainfall, mean over whole country (mm) [IMERG]")
ax.scatter(
    [current_wind],
    [current_rain],
    marker="x",
    color=CHD_GREEN,
    linewidths=3,
    s=100,
)
ax.annotate(
    "   Melissa  \n",
    (current_wind, current_rain),
    va="center",
    ha="right",
    color=CHD_GREEN,
    fontweight="bold",
)
ax.annotate(
    f"\n\n   observed wind speed   \n   and rainfall   ",
    (current_wind, current_rain),
    va="center",
    ha="right",
    color=CHD_GREEN,
    fontstyle="italic",
    fontsize=7,
)
legend_text = "\n    Red text indicates CERF allocation\n\n"

legend_text += (
    "    Size of bubble proportional to\n"
    "    total number of people affected [EM-DAT]"
)
ax.annotate(
    legend_text,
    (0, 360),
    va="top",
    fontsize=6,
    fontstyle="italic",
    color="grey",
)
# ax.set_ylim(top=current_rain * 1.1)
ax.set_xlim(right=current_wind * 1.1)
```

```python
current_rain, current_wind = 223.14158630371094, 269
fig, ax = plot_stats()
ax.scatter(
    [current_wind],
    [current_rain],
    marker="x",
    color=CHD_GREEN,
    linewidths=3,
    s=100,
)
ax.annotate(
    "   Melissa  \n",
    (current_wind, current_rain),
    va="center",
    ha="right",
    color=CHD_GREEN,
    fontweight="bold",
)
ax.annotate(
    f"\n\n   observed wind speed,   \n   rain forecast issued 2025-10-28   ",
    (current_wind, current_rain),
    va="center",
    ha="right",
    color=CHD_GREEN,
    fontstyle="italic",
    fontsize=7,
)
legend_text = "\n    Red text indicates CERF allocation\n\n"

legend_text += (
    "    Size of bubble proportional to\n"
    "    total number of people affected [EM-DAT]"
)
ax.annotate(
    legend_text,
    (0, 240),
    va="top",
    fontsize=6,
    fontstyle="italic",
    color="grey",
)
ax.set_ylim(top=current_rain * 1.1)
ax.set_xlim(right=current_wind * 1.1)
```

```python
cols = [
    "sid",
    "season",
    "name",
    "roll2_mean_im",
    "adm_wind_kmh",
    "cerf",
    "Total Affected",
]
df_melissa = pd.DataFrame(
    [
        {
            "sid": "2025291N11319",
            "season": 2025,
            "name": "MELISSA",
            "roll2_mean_im": current_rain,
            "adm_wind_kmh": current_wind,
            "cerf": False,
            "Total Affected": np.nan,
        }
    ]
)
df_out = pd.concat([df_stats[cols], df_melissa], ignore_index=True)
```

```python
df_out
```

```python
out_path = "temp/jam_ibtracs_chirps_gefs_stats.csv"
df_out.to_csv(out_path, index=False)
```
