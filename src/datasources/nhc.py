from src.utils import blob


def load_recent_glb_forecasts():
    return blob.load_csv_from_blob(
        "noaa/nhc/forecasted_tracks.csv",
        stage="dev",
        container_name="global",
        parse_dates=["issuance", "validTime"],
        sep=";",
    )