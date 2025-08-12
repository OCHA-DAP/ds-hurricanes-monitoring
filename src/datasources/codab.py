import numpy as np
import pandas as pd
import requests
from tqdm.auto import tqdm

from src.constants import ADMIN1_ISO3S, HAVANA1, ISO3S
from src.utils import blob

FIELDMAPS_BASE_URL = "https://data.fieldmaps.io/cod/originals/{iso3}.shp.zip"


def get_blob_name(iso3: str):
    iso3 = iso3.lower()
    return f"{blob.PROJECT_PREFIX}/raw/codab/{iso3}.shp.zip"


def download_codab_to_blob(iso3: str, clobber: bool = False):
    iso3 = iso3.lower()
    blob_name = get_blob_name(iso3)
    if not clobber and blob_name in blob.list_container_blobs(
        name_starts_with=f"{blob.PROJECT_PREFIX}/raw/codab/"
    ):
        print(f"{blob_name} already exists in blob storage")
        return
    url = FIELDMAPS_BASE_URL.format(iso3=iso3)
    response = requests.get(url)
    response.raise_for_status()
    blob.upload_blob_data(blob_name, response.content, stage="dev")


def load_codab_from_blob(
    iso3: str, admin_level: int = 0, aoi_only: bool = False
):
    iso3 = iso3.lower()
    if aoi_only:
        if iso3 != "cub":
            raise ValueError("AOI only available for Cuba")
        if admin_level == 0:
            admin_level = 1
    shapefile = f"{iso3}_adm{admin_level}.shp"
    gdf = blob.load_gdf_from_blob(
        blob_name=get_blob_name(iso3),
        shapefile=shapefile,
        stage="dev",
    )
    if aoi_only:
        gdf = gdf[gdf["ADM1_PCODE"] == HAVANA1]
    return gdf


def combine_codabs():
    """Combine all CODABs into a single GeoDataFrame."""
    gdfs = []
    for iso3 in tqdm(ISO3S):
        admin_level = 1 if iso3 in ADMIN1_ISO3S else 0
        gdfs.append(load_codab_from_blob(iso3, admin_level=admin_level))
    gdf = pd.concat(gdfs, ignore_index=True)

    def get_admin1_name(row):
        col_names = [
            "ADM1_EN",
            "ADM1_ES",
        ]
        for col_name in col_names:
            if not pd.isnull(row[col_name]):
                return row[col_name]
        return np.nan

    def get_admin0_name(row):
        col_names = [
            "ADM0_EN",
            "ADM0_FR",
            "ADM0_ES",
            "ADM0_HT",
        ]
        for col_name in col_names:
            if not pd.isnull(row[col_name]):
                return row[col_name].split(" (")[0]
        if row["ADM0_PCODE"] == "TT":
            return "Trinidad and Tobago"
        else:
            raise ValueError("could not find name")

    def get_pcode(row):
        if not pd.isnull(row["ADM1_PCODE"]):
            return row["ADM1_PCODE"]
        elif not pd.isnull(row["ADM0_PCODE"]):
            return row["ADM0_PCODE"]
        else:
            raise ValueError("coudn't find pcode")

    def get_admin_full_name(row):
        if not pd.isnull(row["ADM1_NAME"]):
            return f'{row["ADM1_NAME"]} ({row["ADM0_NAME"]})'
        elif not pd.isnull(row["ADM0_NAME"]):
            return row["ADM0_NAME"]
        else:
            raise ValueError("couldn't get full name")

    gdf["ADM1_NAME"] = gdf.apply(get_admin1_name, axis=1)
    gdf["ADM0_NAME"] = gdf.apply(get_admin0_name, axis=1)
    gdf["ADM_PCODE"] = gdf.apply(get_pcode, axis=1)
    gdf["ADM_NAME"] = gdf.apply(get_admin_full_name, axis=1)

    blob_name = f"{blob.PROJECT_PREFIX}/processed/codab/combined_codab.shp.zip"
    blob.upload_gdf_to_blob(gdf, blob_name)


def load_combined_codab():
    blob_name = f"{blob.PROJECT_PREFIX}/processed/codab/combined_codab.shp.zip"
    return blob.load_gdf_from_blob(blob_name)
