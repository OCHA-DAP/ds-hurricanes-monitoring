import rioxarray as rxr

CHIRPS_GEFS_URL = (
    "https://data.chc.ucsb.edu/products/EWX/data/forecasts/"
    "CHIRPS-GEFS_precip_v12/daily_16day/"
    "{issue_date:%Y/%m/%d}/data.{valid_date:%Y.%m%d}.tif"
)


def open_chirps_gefs(issue_date, valid_date):
    url = CHIRPS_GEFS_URL.format(issue_date=issue_date, valid_date=valid_date)
    da_in = rxr.open_rasterio(url, masked=True, chunks=True)
    return da_in
