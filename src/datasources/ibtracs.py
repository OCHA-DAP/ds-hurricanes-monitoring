from typing import Literal

import geopandas as gpd
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

from src.constants import KNOTS_TO_MS, MIN_YEAR, PROJ_CRS
from src.datasources import codab
from src.utils import blob


def estimate_rmax(
    vmax: np.ndarray, A: float = 46.6, B: float = 0.2
) -> np.ndarray:
    """Estimate the radius of maximum winds from the maximum wind speed.

    From Willoughby, H. E., Darling, R. W. R., & Rahn, M. E. (2006).
    Parametric Representation of the Primary Hurricane Vortex.
    Part II: A New Family of Sectionally Continuous Profiles.
    Monthly Weather Review, 134(4), 1102–1120.

    vmax: float
        Maximum wind speed in m/s.
    A: float
        Scaling factor.
    B: float
        Scaling exponent.

    Returns:
    --------
    float
        Radius of maximum winds in km.
    """
    return A * vmax**-B


def estimate_wind_at_distance(
    vmax: np.ndarray, distance: float, rmax: np.ndarray = None, B: float = 1.5
) -> np.ndarray:
    """Estimate the wind speed at a given distance from the center.

    From Vickery, P. J., & Wadhera, D. (2008).
    Statistical models of hurricane wind and storm surge: Update.
    Journal of Applied Meteorology and Climatology, 47(6), 1741-1751.

    vmax: float
        Maximum wind speed in knots.
    distance: float
        Distance from the center in km.
    rmax: float
        Radius of maximum winds in km.
    B: float
        Scaling exponent.

    Returns:
    --------
    float
        Wind speed in knots.
    """
    vmax = vmax * KNOTS_TO_MS
    if rmax is None:
        rmax = estimate_rmax(vmax)
    wind_speed = np.where(
        distance < rmax,
        vmax / KNOTS_TO_MS,
        (vmax * (rmax / distance) ** B * np.exp(1 - (rmax / distance) ** B))
        / KNOTS_TO_MS,
    )
    return wind_speed


def speed2strcat(speed: float) -> str:
    """Convert knots to hurricane category using Saffir-Simpson scale."""
    if speed < 0:
        raise ValueError("Wind speed must be positive")
    elif speed < 18:
        return "Trop. Dep."
    elif speed < 33:
        return "Trop. Storm"
    elif speed < 64:
        return "Cat. 1"
    elif speed < 83:
        return "Cat. 2"
    elif speed < 96:
        return "Cat. 3"
    elif speed < 113:
        return "Cat. 4"
    else:
        return "Cat. 5"


def load_ibtracs_with_wind(wind_provider: Literal["usa", "wmo"] = "wmo"):
    """Load IBTrACS data with wind speed data from a specific provider."""
    blob_name = f"ibtracs/ibtracs_with_{wind_provider}_wind.parquet"
    df = blob.load_parquet_from_blob(
        blob_name, stage="dev", container_name="global"
    )
    return df


def load_all_adm_wind_stats():
    blob_name = (
        f"{blob.PROJECT_PREFIX}/processed/ibtracs/all_adm_wind_stats.parquet"
    )
    return blob.load_parquet_from_blob(blob_name)


def estimate_current_rp(pcodes, adm_winds):
    if not len(pcodes) == len(adm_winds):
        raise ValueError("Length of pcodes and adm_winds must be equal")
    df_stats = load_all_adm_wind_stats()
    est_rps = []
    for pcode, adm_wind in zip(pcodes, adm_winds):
        df_adm = df_stats[df_stats["ADM_PCODE"] == pcode]
        df_adm = df_adm.sort_values("rp")
        est_rps.append(
            np.interp(
                adm_wind,
                df_adm["adm_wind"],
                df_adm["rp"],
                right=np.inf,
                left=0,
            )
        )
    return est_rps


def get_similar_storms(pcodes, adm_winds, n_similar_storms: int = 3):
    if not len(pcodes) == len(adm_winds):
        raise ValueError("Length of pcodes and adm_winds must be equal")
    df_sid_name = load_sid_names()
    df_stats = load_all_adm_wind_stats()
    df_stats = df_stats.merge(df_sid_name)
    similar_storms = []
    for pcode, adm_wind in zip(pcodes, adm_winds):
        df_adm = df_stats[df_stats["ADM_PCODE"] == pcode].copy()
        df_adm["wind_dif"] = df_adm["adm_wind"] - adm_wind
        df_adm["wind_dif_abs"] = df_adm["wind_dif"].abs()
        df_adm = df_adm.sort_values("wind_dif_abs")
        df_similar = df_adm.iloc[:3]
        cols = ["sid", "nameyear", "adm_wind"]
        dicts_s = df_similar[cols].to_dict("records")
        similar_storms.append(dicts_s)
    return similar_storms


def load_sid_names():
    blob_name = f"{blob.PROJECT_PREFIX}/processed/ibtracs/sid_name.parquet"
    return blob.load_parquet_from_blob(blob_name)


def process_sid_names():
    df_tracks = load_ibtracs_with_wind(wind_provider="usa")
    df_out = df_tracks.groupby("sid")["name"].first().reset_index()
    df_out["year"] = df_out["sid"].str[:4].astype(int)
    df_out["nameyear"] = (
        df_out["name"].str.capitalize() + " " + df_out["year"].astype(str)
    )
    blob_name = f"{blob.PROJECT_PREFIX}/processed/ibtracs/sid_name.parquet"
    blob.upload_parquet_to_blob(blob_name, df_out)


def calculate_all_adm_wind_stats():
    adm = codab.load_combined_codab().to_crs(PROJ_CRS)
    df = load_ibtracs_with_wind(wind_provider="usa")
    df = df[df["time"].dt.year >= MIN_YEAR]
    df = df[df["basin"] == "NA"]
    total_years = df["time"].dt.year.nunique()
    gdf = gpd.GeoDataFrame(
        data=df, geometry=gpd.points_from_xy(df["lon"], df["lat"]), crs=4326
    ).to_crs(PROJ_CRS)
    dfs = []
    for pcode, adm_row in tqdm(
        adm.set_index("ADM_PCODE").iterrows(), total=len(adm)
    ):
        gdf["distance"] = gdf.geometry.distance(adm_row.geometry) / 1000
        gdf["adm_wind"] = estimate_wind_at_distance(
            gdf["usa_wind"], gdf["distance"]
        )
        df_in = gdf.groupby("sid")["adm_wind"].max().reset_index()
        df_in["ADM_PCODE"] = pcode
        dfs.append(df_in)

    df_stats = pd.concat(dfs, ignore_index=True)

    def calc_rp(group):
        group["rp"] = group["adm_wind"].apply(
            lambda x: (total_years + 1) / len(group[group["adm_wind"] >= x])
        )
        return group

    df_stats = (
        df_stats.groupby("ADM_PCODE")
        .apply(calc_rp, include_groups=False)
        .reset_index(level=0)
    )
    blob_name = (
        f"{blob.PROJECT_PREFIX}/processed/ibtracs/all_adm_wind_stats.parquet"
    )
    blob.upload_parquet_to_blob(blob_name, df_stats)


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
