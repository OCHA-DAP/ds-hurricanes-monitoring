from typing import Literal

import geopandas as gpd
import pandas as pd
from tqdm.auto import tqdm

from src.datasources import codab, ibtracs, nhc
from src.datasources.ibtracs import estimate_wind_at_distance
from src.utils import blob


def get_blob_name(
    fcast_obsv: Literal["fcast", "obsv"], geography: Literal["cub", "all"]
):
    return (
        f"{blob.PROJECT_PREFIX}/monitoring/"
        f"{geography}_{fcast_obsv}_monitoring.parquet"
    )


def load_existing_monitoring_points(
    fcast_obsv: Literal["fcast", "obsv"], geography: Literal["cub", "all"]
):
    blob_name = get_blob_name(fcast_obsv, geography)
    return blob.load_parquet_from_blob(blob_name)


def update_fcast_monitoring(
    geography: Literal["cub", "all"],
    clobber: bool = False,
    verbose: bool = False,
    disable_progress_bar: bool = True,
):
    if geography == "cub":
        update_cub_fcast_monitoring(clobber=clobber, verbose=verbose)
    elif geography == "all":
        update_all_fcast_monitoring(
            clobber=clobber,
            verbose=verbose,
            disable_progress_bar=disable_progress_bar,
        )
    else:
        raise ValueError(f"geography {geography} not recognized")


def update_all_fcast_monitoring(
    clobber: bool = False,
    verbose: bool = False,
    thorough_check: bool = False,
    disable_progress_bar: bool = True,
):
    adm = codab.load_combined_codab().to_crs(3857)
    blob_name = get_blob_name("fcast", "all")
    df_existing_monitoring = blob.load_parquet_from_blob(blob_name)
    df_tracks = nhc.load_recent_glb_forecasts()
    df_tracks = df_tracks[df_tracks["basin"] == "al"]
    dicts = []
    for issue_time, issue_group in tqdm(
        df_tracks.groupby("issuance"), disable=disable_progress_bar
    ):
        if not thorough_check and not clobber:
            if issue_time in df_existing_monitoring["issue_time"].to_list():
                if verbose:
                    print(
                        f"not doing thorough check, {issue_time} "
                        "already monitored"
                    )
                continue
        for atcf_id, group in issue_group.groupby("id"):
            cols = ["latitude", "longitude", "maxwind"]
            df_interp = (
                group.set_index("validTime")[cols]
                .resample("30min")
                .interpolate()
                .reset_index()
            )
            gdf = gpd.GeoDataFrame(
                df_interp,
                geometry=gpd.points_from_xy(
                    df_interp.longitude, df_interp.latitude
                ),
                crs="EPSG:4326",
            ).to_crs(3857)
            gdf["leadtime"] = gdf["validTime"] - issue_time
            for pcode, row in adm.set_index("ADM_PCODE").iterrows():
                monitor_id = (
                    f"{atcf_id}_fcast_"
                    f"{issue_time.isoformat().split('+')[0]}_{pcode}"
                )
                if (
                    monitor_id in df_existing_monitoring["monitor_id"].unique()
                    and not clobber
                ):
                    if verbose:
                        print(f"already monitored for {monitor_id}")
                    continue
                else:
                    if verbose:
                        print(f"monitoring for {monitor_id}")
                gdf["distance"] = gdf.geometry.distance(row.geometry) / 1000
                gdf["adm_wind"] = estimate_wind_at_distance(
                    gdf["maxwind"], gdf["distance"]
                )

                landfall_row = gdf.loc[gdf["distance"].idxmin()]
                time_to_landfall = landfall_row["leadtime"]
                landfall_s = landfall_row["maxwind"]

                maxwind_row = gdf.loc[gdf["adm_wind"].idxmax()]
                time_to_maxwind = maxwind_row["leadtime"]
                max_adm_wind = maxwind_row["adm_wind"]

                dicts.append(
                    {
                        "monitor_id": monitor_id,
                        "atcf_id": atcf_id,
                        "ADM_PCODE": pcode,
                        "name": group["name"].iloc[0],
                        "issue_time": issue_time,
                        "time_to_closest": time_to_landfall,
                        "closest_s": landfall_s,
                        "min_dist": gdf["distance"].min(),
                        "time_to_maxwind": time_to_maxwind,
                        "max_adm_wind": max_adm_wind,
                    }
                )
    df_new_monitoring = pd.DataFrame(dicts)
    if not df_new_monitoring.empty:
        df_new_monitoring["adm_wind_rp"] = ibtracs.estimate_current_rp(
            df_new_monitoring["ADM_PCODE"], df_new_monitoring["max_adm_wind"]
        )
    if clobber:
        df_monitoring_combined = df_new_monitoring
    else:
        df_monitoring_combined = pd.concat(
            [df_existing_monitoring, df_new_monitoring]
        )

    df_monitoring_combined = df_monitoring_combined.sort_values(
        ["issue_time", "atcf_id"]
    )
    blob.upload_parquet_to_blob(blob_name, df_monitoring_combined)


def update_cub_fcast_monitoring(clobber: bool = False, verbose: bool = False):
    adm1 = codab.load_codab_from_blob("cub", aoi_only=True).to_crs(3857)
    blob_name = get_blob_name("fcast", "cub")
    df_existing_monitoring = blob.load_parquet_from_blob(blob_name)
    df_tracks = nhc.load_recent_glb_forecasts()
    df_tracks = df_tracks[df_tracks["basin"] == "al"]

    dicts = []
    for issue_time, issue_group in df_tracks.groupby("issuance"):
        for atcf_id, group in issue_group.groupby("id"):
            monitor_id = (
                f"{atcf_id}_fcast_{issue_time.isoformat().split('+')[0]}"
            )
            if (
                monitor_id in df_existing_monitoring["monitor_id"].unique()
                and not clobber
            ):
                if verbose:
                    print(f"already monitored for {monitor_id}")
                continue
            else:
                print(f"monitoring for {monitor_id}")

            cols = ["latitude", "longitude", "maxwind"]
            df_interp = (
                group.set_index("validTime")[cols]
                .resample("30min")
                .interpolate()
                .reset_index()
            )
            gdf = gpd.GeoDataFrame(
                df_interp,
                geometry=gpd.points_from_xy(
                    df_interp.longitude, df_interp.latitude
                ),
                crs="EPSG:4326",
            ).to_crs(3857)
            gdf["distance"] = (
                gdf.geometry.distance(adm1.iloc[0].geometry) / 1000
            )
            gdf["leadtime"] = gdf["validTime"] - issue_time

            landfall_row = gdf.loc[gdf["distance"].idxmin()]
            time_to_landfall = landfall_row["leadtime"]
            landfall_s = landfall_row["maxwind"]

            dicts.append(
                {
                    "monitor_id": monitor_id,
                    "atcf_id": atcf_id,
                    "name": group["name"].iloc[0],
                    "issue_time": issue_time,
                    "time_to_closest": time_to_landfall,
                    "closest_s": landfall_s,
                    "min_dist": gdf["distance"].min(),
                }
            )

    df_new_monitoring = pd.DataFrame(dicts)

    if clobber:
        df_monitoring_combined = df_new_monitoring
    else:
        df_monitoring_combined = pd.concat(
            [df_existing_monitoring, df_new_monitoring]
        )

    df_monitoring_combined = df_monitoring_combined.sort_values(
        ["issue_time", "atcf_id"]
    )
    blob.upload_parquet_to_blob(blob_name, df_monitoring_combined)
