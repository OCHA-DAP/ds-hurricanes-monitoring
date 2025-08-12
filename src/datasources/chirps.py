import calendar
import datetime
import os
from pathlib import Path

import cftime
import requests
import xarray as xr
from tqdm import tqdm

DATA_DIR = Path(os.environ["AA_DATA_DIR_NEW"])
CHIRPS_RAW_DIR = DATA_DIR / "public" / "raw" / "glb" / "chirps" / "daily"
CHIRPS_PROC_DIR = DATA_DIR / "public" / "processed" / "glb" / "chirps"
CHIRPS_BASE_URL = (
    "https://iridl.ldeo.columbia.edu/SOURCES/.UCSB/.CHIRPS/.v2p0/"
)


def download_chirps_daily(
    d: datetime.datetime, total_bounds, iso3: str, clobber: bool = False
):
    """
    Download CHIRPS daily data for a specific date.
    :param d: date
    :param total_bounds: total_bounds from CODAB
    :param clobber:
    :return:
    """
    if not CHIRPS_RAW_DIR.exists():
        os.makedirs(CHIRPS_RAW_DIR, exist_ok=True)

    lon_min, lat_min, lon_max, lat_max = total_bounds
    location_url = (
        f"X/%28{lon_min}%29%28{lon_max}"
        f"%29RANGEEDGES/"
        f"Y/%28{lat_max}%29%28{lat_min}"
        f"%29RANGEEDGES/"
    )

    resolution = 0.05

    year = str(d.year)
    month = f"{d.month:02d}"
    day = f"{d.day:02d}"

    month_name = calendar.month_abbr[int(month)]

    filepath = CHIRPS_RAW_DIR / f"chirps-daily-{iso3}-{year}-{month}-{day}.nc"
    if filepath.exists() and not clobber:
        return

    url = (
        f"{CHIRPS_BASE_URL}"
        ".daily-improved/.global/."
        f"{str(resolution).replace('.', 'p')}/.prcp/"
        f"{location_url}"
        f"T/%28{day}%20{month_name}%20{year}%29%28{day}"
        f"%20{month_name}%20{year}"
        "%29RANGEEDGES/data.nc"
    )
    try:
        response = requests.get(url)
        with open(filepath, "wb") as out_file:
            out_file.write(response.content)
    except Exception as e:
        print(f"Failed to download {d.date()}")
        print(e)
        return


def process_chirps_daily(iso3: str):
    """Cycle over individual CHIRPS daily files and concatenate them into
    one file.
    """
    if not CHIRPS_PROC_DIR.exists():
        os.makedirs(CHIRPS_PROC_DIR, exist_ok=True)

    filenames = os.listdir(CHIRPS_RAW_DIR)
    filenames = [f for f in filenames if f.startswith(f"chirps-daily-{iso3}")]
    ds_ins = []
    for filename in tqdm(filenames):
        ds_in = xr.load_dataset(CHIRPS_RAW_DIR / filename)
        ds_ins.append(ds_in)

    ds_concat = xr.concat(ds_ins, dim="T")
    ds_concat = ds_concat.assign_coords(
        T=cftime.datetime.fromordinal(
            ds_concat.T.values, calendar="standard", has_year_zero=False
        )
    )
    # ds_concat.T.attrs["calendar"] = "360_day"
    # ds_concat = xr.decode_cf(ds_concat)
    filename = f"chirps-daily-{iso3}.nc"
    ds_concat.to_netcdf(CHIRPS_PROC_DIR / filename)


def open_chirps_daily(iso3: str) -> xr.Dataset:
    filename = f"chirps-daily-{iso3}.nc"
    return xr.open_dataset(CHIRPS_PROC_DIR / filename)
