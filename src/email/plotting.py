import io
from typing import Literal

import geopandas as gpd
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pytz
from matplotlib import pyplot as plt

from src.constants import CHD_GREEN, LON_ZOOM_RANGE, PROJ_CRS
from src.datasources import codab, ibtracs, nhc
from src.email.utils import (
    TEST_ATCF_ID,
    TEST_FCAST_MONITOR_ID,
    TEST_OBSV_MONITOR_ID,
    TEST_STORM,
    add_test_row_to_monitoring,
    open_static_image,
)
from src.monitoring import monitoring_utils
from src.utils import blob


def get_plot_blob_name(
    monitor_id,
    plot_type: Literal["map", "scatter"],
    geography: Literal["cub", "all"],
):
    fcast_obsv = "fcast" if "fcast" in monitor_id.lower() else "obsv"
    return (
        f"{blob.PROJECT_PREFIX}/plots/{fcast_obsv}/"
        f"{monitor_id}_{geography}_{plot_type}.png"
    )


def convert_datetime_to_str(x: pd.Timestamp) -> str:
    return x.strftime("%H:%M, %-d %b")


def update_plots(
    fcast_obsv: Literal["fcast", "obsv"],
    geography: Literal["cub", "all"],
    clobber: list = None,
    verbose: bool = False,
):
    if clobber is None:
        clobber = []
    df_monitoring = monitoring_utils.load_existing_monitoring_points(
        fcast_obsv, geography
    )
    if TEST_STORM:
        df_monitoring = add_test_row_to_monitoring(
            df_monitoring, fcast_obsv, geography
        )
    existing_plot_blobs = blob.list_container_blobs(
        name_starts_with=f"{blob.PROJECT_PREFIX}/plots/{fcast_obsv}/"
    )
    existing_plot_blobs = [
        x for x in existing_plot_blobs if f"_{geography}_" in x
    ]
    if geography == "cub":
        for monitor_id, row in df_monitoring.set_index(
            "monitor_id"
        ).iterrows():
            for plot_type in ["map", "scatter"]:
                blob_name = get_plot_blob_name(
                    monitor_id, plot_type, geography
                )
                if (
                    blob_name in existing_plot_blobs
                    and plot_type not in clobber
                ):
                    if verbose:
                        print(f"Skipping {blob_name}, already exists")
                    continue
                print(f"Creating {blob_name}")
                create_plot(monitor_id, plot_type, fcast_obsv, geography)
    elif geography == "all":
        df_monitoring["email_monitor_id"] = df_monitoring["monitor_id"].apply(
            lambda x: "_".join(x.split("_")[:-1])
        )
        for monitor_id, group in df_monitoring.groupby("email_monitor_id"):
            for plot_type in ["map"]:
                blob_name = get_plot_blob_name(
                    monitor_id, plot_type, geography
                )
                if (
                    blob_name in existing_plot_blobs
                    and plot_type not in clobber
                ):
                    if verbose:
                        print(f"Skipping {blob_name}, already exists")
                    continue
                print(f"Creating {blob_name}")
                create_plot(monitor_id, plot_type, fcast_obsv, geography)


def create_plot(
    monitor_id: str,
    plot_type: Literal["map", "scatter"],
    fcast_obsv: Literal["fcast", "obsv"],
    geography: Literal["cub", "all"],
    debug: bool = False,
):
    if plot_type == "map":
        return create_map_plot(monitor_id, fcast_obsv, geography, debug=debug)
    elif plot_type == "scatter":
        return create_scatter_plot(
            monitor_id, fcast_obsv, geography, debug=debug
        )
    else:
        raise ValueError(f"Unknown plot type: {plot_type}")


def create_scatter_plot(
    monitor_id: str,
    fcast_obsv: Literal["fcast", "obsv"],
    geography: Literal["cub", "all"],
    debug: bool = False,
):
    if geography != "cub":
        raise NotImplementedError("Only Cuba is supported for now")
    return create_cub_scatter_plot(monitor_id, fcast_obsv, debug=debug)


def create_cub_scatter_plot(
    monitor_id: str, fcast_obsv: Literal["fcast", "obsv"], debug: bool = False
):
    adm = codab.load_codab_from_blob("cub", aoi_only=True)
    wind_stats = ibtracs.load_havana_wind_stats()
    df_monitoring = monitoring_utils.load_existing_monitoring_points(
        fcast_obsv, "cub"
    )
    if monitor_id in [TEST_FCAST_MONITOR_ID, TEST_OBSV_MONITOR_ID]:
        df_monitoring = add_test_row_to_monitoring(df_monitoring, fcast_obsv)
    monitoring_point = df_monitoring.set_index("monitor_id").loc[monitor_id]
    ny_tz = pytz.timezone("America/New_York")
    cyclone_name = monitoring_point["name"]
    atcf_id = monitoring_point["atcf_id"]
    if atcf_id == TEST_ATCF_ID:
        atcf_id = "al042024"
    issue_time = monitoring_point["issue_time"]
    issue_time_ny = issue_time.astimezone(ny_tz)
    issue_time_str = convert_datetime_to_str(issue_time_ny)
    fcast_obsv_str = "observations" if fcast_obsv == "obsv" else "forecast"
    no_pass_text = (
        "did not pass" if fcast_obsv == "obsv" else "not forecast to pass"
    )

    if fcast_obsv == "fcast":
        df_tracks = nhc.load_recent_glb_forecasts()
        tracks_f = df_tracks[
            (df_tracks["id"] == atcf_id)
            & (df_tracks["issuance"] == issue_time)
        ].copy()
    else:
        df_tracks = nhc.load_recent_glb_obsv()
        tracks_f = df_tracks[
            (df_tracks["id"] == atcf_id)
            & (df_tracks["lastUpdate"] <= issue_time)
        ].copy()
        tracks_f = tracks_f.rename(
            columns={"lastUpdate": "validTime", "intensity": "maxwind"}
        )
        tracks_f["issuance"] = tracks_f["validTime"]

    df_interp = (
        tracks_f.set_index("validTime")[["latitude", "longitude", "maxwind"]]
        .resample("30min")
        .interpolate(method="linear")
    )

    gdf = gpd.GeoDataFrame(
        df_interp,
        geometry=gpd.points_from_xy(df_interp.longitude, df_interp.latitude),
        crs="EPSG:4326",
    )
    gdf["havana_distance_km"] = (
        gdf.to_crs(PROJ_CRS).geometry.distance(
            adm.to_crs(PROJ_CRS).iloc[0].geometry
        )
        / 1000
    )
    dicts = []
    for d_thresh in range(0, 501, 1):
        gdff = gdf[gdf["havana_distance_km"] <= d_thresh]
        dicts.append(
            {
                "max_wind": gdff["maxwind"].max(),
                "d": d_thresh,
            }
        )

    current_wind_stats = pd.DataFrame(dicts).dropna()

    x_cutoff = 100
    y_cutoff = 10

    ymax = 200

    fig, ax = plt.subplots(figsize=(8, 8), dpi=300)

    for nameyear, group in wind_stats.groupby("nameyear"):
        if group["d"].min() > ymax * 0.95:
            continue
        ax.plot(group["max_wind"], group["d"], alpha=0.5, color="grey")
        ax.annotate(
            nameyear + "  ",
            (group["max_wind"].min(), group["d"].min()),
            ha="center",
            va="bottom",
            fontsize=7,
            rotation=270,
        )

    ax.plot(
        current_wind_stats["max_wind"],
        current_wind_stats["d"],
        linewidth=3,
        alpha=1,
        color=CHD_GREEN,
    )
    current_xy = (
        current_wind_stats["max_wind"].min(),
        current_wind_stats["d"].min(),
    )
    ax.annotate(
        f"  {cyclone_name}\n\n",
        current_xy,
        ha="left",
        va="center",
        fontsize=10,
        color=CHD_GREEN,
        fontweight="bold",
    )
    ax.annotate(
        f"\n  {fcast_obsv_str} issued\n  {issue_time_str}",
        current_xy,
        va="center",
        ha="left",
        fontsize=10,
        color=CHD_GREEN,
        fontstyle="italic",
    )

    ax.axvline(x=x_cutoff, color="lightgray", linestyle="--", linewidth=0.5)
    ax.axhline(y=y_cutoff, color="lightgray", linestyle="--", linewidth=0.5)
    ax.fill_between(
        np.arange(x_cutoff, 200, 1),
        0,
        y_cutoff,
        color="gold",
        alpha=0.2,
        zorder=-1,
    )
    ax.annotate(
        "Trigger zone ",
        (155, 1),
        va="bottom",
        ha="right",
        color="orange",
        fontsize=10,
        fontweight="bold",
    )

    if monitoring_point["min_dist"] >= ymax:
        rect = plt.Rectangle(
            (0, 0),
            1,
            1,
            transform=ax.transAxes,
            color="white",
            alpha=0.7,
            zorder=3,
        )
        ax.add_patch(rect)
        ax.text(
            0.5,
            0.5,
            f"{cyclone_name} {no_pass_text}\n" f"within 200 km of Havana",
            fontsize=20,
            color="grey",
            ha="center",
            va="center",
            transform=ax.transAxes,
        )

    ax.set_xlim(right=155, left=0)
    ax.set_ylim(top=ymax, bottom=0)

    ax.set_xlabel("Maximum windspeed while within distance (knots)")
    ax.set_ylabel("Distance to Havana (km)")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_title("Comparison with historical wind speeds and distances")
    if debug:
        return fig, ax
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    buffer.seek(0)
    blob_name = get_plot_blob_name(monitor_id, "scatter", "cub")
    blob.upload_blob_data(blob_name, buffer)
    plt.close(fig)


def create_map_plot(
    monitor_id: str,
    fcast_obsv: Literal["fcast", "obsv"],
    geography: Literal["cub", "all"],
    debug: bool = False,
):
    if geography == "cub":
        return create_cub_map_plot(monitor_id, fcast_obsv, debug=debug)
    elif geography == "all":
        return create_all_map_plot(monitor_id, fcast_obsv, debug=debug)
    else:
        raise ValueError(f"Unknown geography: {geography}")


def create_all_map_plot(
    monitor_id: str, fcast_obsv: Literal["fcast", "obsv"], debug: bool = False
):
    lts = {
        "action": {
            "color": "darkorange",
            "plot_color": "black",
            "dash": "solid",
            "label": "Action",
            "zorder": 2,
            "lt_max": pd.Timedelta(days=3),
            "lt_min": pd.Timedelta(days=-1),
            "threshs": {
                "wind_dist": 100,
                "dist_min": 0,
            },
        },
        "readiness": {
            "color": "dodgerblue",
            "plot_color": "grey",
            "dash": "dot",
            "label": "Mobilisation",
            "zorder": 1,
            "lt_max": pd.Timedelta(days=5),
            "lt_min": pd.Timedelta(days=2),
            "threshs": {
                "wind_dist": 100,
                "dist_min": 0,
            },
        },
    }
    df_monitoring = monitoring_utils.load_existing_monitoring_points(
        fcast_obsv, "all"
    )
    if monitor_id in [TEST_FCAST_MONITOR_ID, TEST_OBSV_MONITOR_ID]:
        df_monitoring = add_test_row_to_monitoring(
            df_monitoring, fcast_obsv, "all"
        )
    df_monitoring["email_monitor_id"] = df_monitoring["monitor_id"].apply(
        lambda x: "_".join(x.split("_")[:-1])
    )
    monitoring_group = df_monitoring[
        (df_monitoring["email_monitor_id"] == monitor_id)
    ]
    monitoring_point = monitoring_group.iloc[0]
    ny_tz = pytz.timezone("America/New_York")
    cyclone_name = monitoring_point["name"]
    atcf_id = monitoring_point["atcf_id"]
    if atcf_id == TEST_ATCF_ID:
        atcf_id = "al022024"
    issue_time = monitoring_point["issue_time"]
    issue_time_ny = issue_time.astimezone(ny_tz)

    if fcast_obsv == "fcast":
        df_tracks = nhc.load_recent_glb_forecasts()
        tracks_f = df_tracks[
            (df_tracks["id"] == atcf_id)
            & (df_tracks["issuance"] == issue_time)
        ].copy()
    else:
        df_tracks = nhc.load_recent_glb_obsv()
        tracks_f = df_tracks[
            (df_tracks["id"] == atcf_id)
            & (df_tracks["lastUpdate"] <= issue_time)
        ].copy()
        tracks_f = tracks_f.rename(
            columns={"lastUpdate": "validTime", "intensity": "maxwind"}
        )
        tracks_f["issuance"] = tracks_f["validTime"]

    tracks_f["validTime_hti"] = tracks_f["validTime"].apply(
        lambda x: x.astimezone(ny_tz)
    )
    tracks_f["valid_time_str"] = tracks_f["validTime_hti"].apply(
        convert_datetime_to_str
    )
    tracks_f["lt"] = tracks_f["validTime"] - tracks_f["issuance"]
    tracks_f["windspeed_validtime_str"] = (
        tracks_f["maxwind"].astype(str)
        + " kt<br>"
        + tracks_f["valid_time_str"].apply(
            lambda x: f"{x.split(', ')[0]},<br>{x.split(', ')[1]}"
        )
    )

    fig = go.Figure()

    relevant_lts = (
        ["readiness", "action"] if fcast_obsv == "fcast" else ["obsv"]
    )
    for lt_name in relevant_lts:
        lt_params = lts[lt_name]
        if lt_name == "obsv":
            dff = tracks_f.copy()
        else:
            dff = tracks_f[
                (tracks_f["lt"] <= lt_params["lt_max"])
                & (tracks_f["lt"] >= lt_params["lt_min"])
            ]
        # all points
        fig.add_trace(
            go.Scattermapbox(
                lon=dff["longitude"],
                lat=dff["latitude"],
                mode="markers+text+lines",
                marker=dict(size=50, color=lt_params["plot_color"]),
                text=dff["windspeed_validtime_str"].astype(str),
                line=dict(width=2, color=lt_params["plot_color"]),
                textfont=dict(size=12, color="white"),
                customdata=dff["valid_time_str"],
                hovertemplate=("Valid time: %{customdata}<extra></extra>"),
            )
        )

    lat_max = max(tracks_f["latitude"])
    lat_min = min(tracks_f["latitude"])
    lon_max = max(tracks_f["longitude"])
    lon_min = min(tracks_f["longitude"])
    width_to_height = 1
    margin = 1.7
    height = (lat_max - lat_min) * margin * width_to_height
    width = (lon_max - lon_min) * margin
    lon_zoom = np.interp(width, LON_ZOOM_RANGE, range(20, 0, -1))
    lat_zoom = np.interp(height, LON_ZOOM_RANGE, range(20, 0, -1))
    zoom = round(min(lon_zoom, lat_zoom), 2)
    center_lat = (lat_max + lat_min) / 2
    center_lon = (lon_max + lon_min) / 2

    issue_time_str = convert_datetime_to_str(issue_time_ny)
    fcast_obsv_str = "observations" if fcast_obsv == "obsv" else "forecast"
    plot_title = (
        f"NOAA {fcast_obsv_str} for {cyclone_name}<br>"
        f"<sup>Issued {issue_time_str} (all times are New York time)</sup>"
    )

    if fcast_obsv == "fcast":
        legend_filename = "map_legend_nothresh.png"
        aspect = 1
    else:
        legend_filename = "map_legend_obsv.png"
        aspect = 1.3

    encoded_legend = open_static_image(legend_filename)

    fig.update_layout(
        title=plot_title,
        mapbox_style="open-street-map",
        mapbox_zoom=zoom,
        mapbox_center_lat=center_lat,
        mapbox_center_lon=center_lon,
        margin={"r": 0, "t": 50, "l": 0, "b": 0},
        height=850,
        width=800,
        showlegend=False,
        images=[
            dict(
                source=f"data:image/png;base64,{encoded_legend}",
                xref="paper",
                yref="paper",
                x=0.01,
                y=0.01,
                sizex=0.25,
                sizey=0.25 / aspect,
                xanchor="left",
                yanchor="bottom",
                opacity=0.7,
            )
        ],
    )
    if debug:
        return fig

    buffer = io.BytesIO()
    blob_name = get_plot_blob_name(monitor_id, "map", "all")
    # scale corresponds to 150 dpi
    try:
        fig.write_image(buffer, format="png", scale=2.08)
        buffer.seek(0)
        blob.upload_blob_data(blob_name, buffer)
    except Exception as e:
        print(f"Error uploading {blob_name}: {e}")


def create_cub_map_plot(
    monitor_id: str, fcast_obsv: Literal["fcast", "obsv"], debug: bool = False
):
    adm = codab.load_codab_from_blob("cub", aoi_only=True)
    lts = {
        "action": {
            "color": "darkorange",
            "plot_color": "black",
            "dash": "solid",
            "label": "Action",
            "zorder": 2,
            "lt_max": pd.Timedelta(days=3),
            "lt_min": pd.Timedelta(days=-1),
            "threshs": {
                "wind_dist": 100,
                "dist_min": 0,
            },
        },
        "readiness": {
            "color": "dodgerblue",
            "plot_color": "grey",
            "dash": "dot",
            "label": "Mobilisation",
            "zorder": 1,
            "lt_max": pd.Timedelta(days=5),
            "lt_min": pd.Timedelta(days=2),
            "threshs": {
                "wind_dist": 100,
                "dist_min": 0,
            },
        },
    }
    df_monitoring = monitoring_utils.load_existing_monitoring_points(
        fcast_obsv, "cub"
    )
    if monitor_id in [TEST_FCAST_MONITOR_ID, TEST_OBSV_MONITOR_ID]:
        df_monitoring = add_test_row_to_monitoring(
            df_monitoring, fcast_obsv, "cub"
        )
    monitoring_point = df_monitoring.set_index("monitor_id").loc[monitor_id]
    ny_tz = pytz.timezone("America/New_York")
    cyclone_name = monitoring_point["name"]
    atcf_id = monitoring_point["atcf_id"]
    if atcf_id == TEST_ATCF_ID:
        atcf_id = "al042024"
    issue_time = monitoring_point["issue_time"]
    issue_time_ny = issue_time.astimezone(ny_tz)

    if fcast_obsv == "fcast":
        df_tracks = nhc.load_recent_glb_forecasts()
        tracks_f = df_tracks[
            (df_tracks["id"] == atcf_id)
            & (df_tracks["issuance"] == issue_time)
        ].copy()
    else:
        df_tracks = nhc.load_recent_glb_obsv()
        tracks_f = df_tracks[
            (df_tracks["id"] == atcf_id)
            & (df_tracks["lastUpdate"] <= issue_time)
        ].copy()
        tracks_f = tracks_f.rename(
            columns={"lastUpdate": "validTime", "intensity": "maxwind"}
        )
        tracks_f["issuance"] = tracks_f["validTime"]

    tracks_f["validTime_hti"] = tracks_f["validTime"].apply(
        lambda x: x.astimezone(ny_tz)
    )
    tracks_f["valid_time_str"] = tracks_f["validTime_hti"].apply(
        convert_datetime_to_str
    )
    tracks_f["lt"] = tracks_f["validTime"] - tracks_f["issuance"]
    tracks_f["windspeed_validtime_str"] = (
        tracks_f["maxwind"].astype(str)
        + " kt<br>"
        + tracks_f["valid_time_str"].apply(
            lambda x: f"{x.split(', ')[0]},<br>{x.split(', ')[1]}"
        )
    )

    fig = go.Figure()

    # adm outline
    x, y = adm.geometry[0].exterior.coords.xy
    fig.add_trace(
        go.Scattermapbox(
            lon=list(x),
            lat=list(y),
            mode="lines",
            line_color="purple",
            showlegend=False,
        )
    )

    relevant_lts = (
        ["readiness", "action"] if fcast_obsv == "fcast" else ["obsv"]
    )
    for lt_name in relevant_lts:
        lt_params = lts[lt_name]
        if lt_name == "obsv":
            dff = tracks_f.copy()
        else:
            dff = tracks_f[
                (tracks_f["lt"] <= lt_params["lt_max"])
                & (tracks_f["lt"] >= lt_params["lt_min"])
            ]
        # triggered points
        dff_trig = dff[
            (dff["maxwind"] >= lt_params["threshs"]["wind_dist"])
            & (dff["lt"] >= lt_params["lt_min"])
        ]
        fig.add_trace(
            go.Scattermapbox(
                lon=dff_trig["longitude"],
                lat=dff_trig["latitude"],
                mode="markers",
                marker=dict(size=50, color="red"),
            )
        )
        # all points
        fig.add_trace(
            go.Scattermapbox(
                lon=dff["longitude"],
                lat=dff["latitude"],
                mode="markers+text+lines",
                marker=dict(size=50, color=lt_params["plot_color"]),
                text=dff["windspeed_validtime_str"].astype(str),
                line=dict(width=2, color=lt_params["plot_color"]),
                textfont=dict(size=12, color="white"),
                customdata=dff["valid_time_str"],
                hovertemplate=("Valid time: %{customdata}<extra></extra>"),
            )
        )
    print(dff)
    adm_centroid = adm.to_crs(3857).centroid.to_crs(4326)[0]
    centroid_lat, centroid_lon = adm_centroid.y, adm_centroid.x

    if fcast_obsv == "fcast":
        lat_max = max(tracks_f["latitude"])
        lat_max = max(lat_max, centroid_lat)
        lat_min = min(tracks_f["latitude"])
        lat_min = min(lat_min, centroid_lat)
        lon_max = max(tracks_f["longitude"])
        lon_max = max(lon_max, centroid_lon)
        lon_min = min(tracks_f["longitude"])
        lon_min = min(lon_min, centroid_lon)
        width_to_height = 1
        margin = 1.7
        height = (lat_max - lat_min) * margin * width_to_height
        width = (lon_max - lon_min) * margin
        lon_zoom = np.interp(width, LON_ZOOM_RANGE, range(20, 0, -1))
        lat_zoom = np.interp(height, LON_ZOOM_RANGE, range(20, 0, -1))
        zoom = round(min(lon_zoom, lat_zoom), 2)
        center_lat = (lat_max + lat_min) / 2
        center_lon = (lon_max + lon_min) / 2
    else:
        zoom = 5.8
        center_lat = centroid_lat
        center_lon = centroid_lon

    issue_time_str = convert_datetime_to_str(issue_time_ny)
    fcast_obsv_str = "observations" if fcast_obsv == "obsv" else "forecast"
    plot_title = (
        f"NOAA {fcast_obsv_str} for {cyclone_name}<br>"
        f"<sup>Issued {issue_time_str} (all times are New York time)</sup>"
    )

    if fcast_obsv == "fcast":
        legend_filename = "map_legend.png"
        aspect = 1
    else:
        legend_filename = "map_legend_obsv.png"
        aspect = 1.3

    encoded_legend = open_static_image(legend_filename)

    fig.update_layout(
        title=plot_title,
        mapbox_style="open-street-map",
        mapbox_zoom=zoom,
        mapbox_center_lat=center_lat,
        mapbox_center_lon=center_lon,
        margin={"r": 0, "t": 50, "l": 0, "b": 0},
        height=850,
        width=800,
        showlegend=False,
        images=[
            dict(
                source=f"data:image/png;base64,{encoded_legend}",
                xref="paper",
                yref="paper",
                x=0.01,
                y=0.01,
                sizex=0.25,
                sizey=0.25 / aspect,
                xanchor="left",
                yanchor="bottom",
                opacity=0.7,
            )
        ],
    )
    if debug:
        return fig

    buffer = io.BytesIO()
    # scale corresponds to 150 dpi
    fig.write_image(buffer, format="png", scale=2.08)
    buffer.seek(0)

    blob_name = get_plot_blob_name(monitor_id, "map", "cub")
    blob.upload_blob_data(blob_name, buffer)
