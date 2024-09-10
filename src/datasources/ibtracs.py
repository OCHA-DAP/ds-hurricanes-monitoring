from typing import Literal

import geopandas as gpd
import pandas as pd

from src.constants import PROJ_CRS
from src.datasources import codab
from src.utils import blob


def speed2strcat(speed: float) -> str:
    """Convert knots to hurricane category using Saffir-Simpson scale."""
    if speed < 0:
        raise ValueError("Wind speed must be positive")
    elif speed < 18:
        return "Tropical Depression"
    elif speed < 33:
        return "Tropical Storm"
    elif speed < 64:
        return "Category 1"
    elif speed < 83:
        return "Category 2"
    elif speed < 96:
        return "Category 3"
    elif speed < 113:
        return "Category 4"
    else:
        return "Category 5"


def load_ibtracs_with_wind(wind_provider: Literal["usa", "wmo"] = "wmo"):
    """Load IBTrACS data with wind speed data from a specific provider."""
    blob_name = f"ibtracs/ibtracs_with_{wind_provider}_wind.parquet"
    df = blob.load_parquet_from_blob(
        blob_name, stage="dev", container_name="global"
    )
    return df


def process_havana_distances():
    adm = codab.load_codab_from_blob("cub", aoi_only=True)
    df = load_ibtracs_with_wind(wind_provider="usa")
    df_sp = df[df["basin"] == "NA"]

    def resample_and_interpolate(group):
        group = group.set_index("time")
        group = group.resample("30min").interpolate(method="linear")
        return group

    cols = ["usa_wind", "lat", "lon", "time"]
    df_interp = (
        df_sp.groupby(["sid", "name", "basin"])[cols]
        .apply(resample_and_interpolate, include_groups=False)
        .reset_index()
    )
    gdf = gpd.GeoDataFrame(
        df_interp,
        geometry=gpd.points_from_xy(df_interp.lon, df_interp.lat),
        crs="EPSG:4326",
    )
    gdf["havana_distance_km"] = (
        gdf.to_crs(PROJ_CRS).geometry.distance(
            adm.to_crs(PROJ_CRS).iloc[0].geometry
        )
        / 1000
    )
    blob_name = (
        f"{blob.PROJECT_PREFIX}/processed/ibtracs/havana_distances.parquet"
    )
    blob.upload_parquet_to_blob(
        blob_name, gdf.drop(columns="geometry"), stage="dev"
    )


def load_havana_distances():
    blob_name = (
        f"{blob.PROJECT_PREFIX}/processed/ibtracs/havana_distances.parquet"
    )
    return blob.load_parquet_from_blob(blob_name)


def calculate_havana_windstats():
    df = load_havana_distances()
    df = df[df["time"].dt.year >= 2000]
    dicts = []
    for d_thresh in range(0, 501, 1):
        tracks_f = df[df["havana_distance_km"] <= d_thresh]
        for (sid, name), group in tracks_f.groupby(["sid", "name"]):
            dicts.append(
                {
                    "sid": sid,
                    "name": name,
                    "max_wind": group["usa_wind"].max(),
                    "d": d_thresh,
                }
            )

    wind_stats = pd.DataFrame(dicts)
    wind_stats["year"] = wind_stats["sid"].str[:4].astype(int)
    wind_stats["nameyear"] = (
        wind_stats["name"].str.capitalize()
        + " "
        + wind_stats["year"].astype(str)
    )
    blob_name = (
        f"{blob.PROJECT_PREFIX}/processed/ibtracs/havana_wind_stats.parquet"
    )
    blob.upload_parquet_to_blob(blob_name, wind_stats, stage="dev")


def load_havana_wind_stats():
    blob_name = (
        f"{blob.PROJECT_PREFIX}/processed/ibtracs/havana_wind_stats.parquet"
    )
    return blob.load_parquet_from_blob(blob_name)
