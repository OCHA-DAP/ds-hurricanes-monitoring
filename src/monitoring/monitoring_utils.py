from typing import Literal

import geopandas as gpd
import pandas as pd

from src.datasources import codab, nhc
from src.utils import blob


def get_blob_name(
    fcast_obsv: Literal["fcast", "obsv"], geography: Literal["cub", "all"]
):
    return f"{blob.PROJECT_PREFIX}/monitoring/{geography}_{fcast_obsv}_monitoring.parquet"


def load_existing_monitoring_points(
    fcast_obsv: Literal["fcast", "obsv"], geography: Literal["cub", "all"]
):
    blob_name = get_blob_name(fcast_obsv, geography)
    return blob.load_parquet_from_blob(blob_name)


def update_fcast_monitoring(
    geography: Literal["cub", "all"],
    clobber: bool = False,
    verbose: bool = False,
):
    if geography != "cub":
        raise NotImplementedError("Only Cuba is supported for now")
    else:
        update_cub_fcast_monitoring(clobber=clobber, verbose=verbose)


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
