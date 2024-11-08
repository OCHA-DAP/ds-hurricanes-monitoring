import pandas as pd

from src.utils import blob


def get_blob_name(date: pd.Timestamp):
    return f"imerg/daily/late/v7/processed/imerg-daily-late-{date.date()}.tif"


def open_imerg_raster(date: pd.Timestamp):
    blob_name = get_blob_name(date)
    return blob.open_blob_cog(blob_name, container_name="raster", stage="prod")
