from typing import Literal

import pandas as pd

from src.constants import MIN_EMAIL_DISTANCE
from src.email.send_emails import send_info_email
from src.email.utils import (
    TEST_ATCF_ID,
    TEST_STORM,
    add_test_row_to_monitoring,
    load_email_record,
)
from src.monitoring import monitoring_utils
from src.utils import blob


def update_fcast_info_emails(
    geography: Literal["cub", "all"], verbose: bool = False
):
    df_monitoring = monitoring_utils.load_existing_monitoring_points(
        "fcast", geography
    )
    try:
        df_existing_email_record = load_email_record()
    except Exception as e:
        print(f"could not load email record: {e}")
        df_existing_email_record = pd.DataFrame(
            columns=["monitor_id", "atcf_id", "geography", "email_type"]
        )
    if TEST_STORM:
        df_monitoring = add_test_row_to_monitoring(df_monitoring, "fcast")
        df_existing_email_record = df_existing_email_record[
            ~(
                (df_existing_email_record["atcf_id"] == TEST_ATCF_ID)
                & (df_existing_email_record["email_type"] == "info")
            )
        ]

    dicts = []
    for monitor_id, row in df_monitoring.set_index("monitor_id").iterrows():
        if row["min_dist"] > MIN_EMAIL_DISTANCE:
            if verbose:
                print(
                    f"min_dist is {row['min_dist']}, "
                    f"skipping info email for {monitor_id}"
                )
            continue
        if (
            monitor_id
            in df_existing_email_record[
                df_existing_email_record["email_type"] == "info"
            ]["monitor_id"].unique()
        ):
            if verbose:
                print(f"already sent info email for {monitor_id}")
        else:
            try:
                print(f"sending info email for {monitor_id}")
                send_info_email(
                    monitor_id=monitor_id,
                    fcast_obsv="fcast",
                    geography=geography,
                )
                dicts.append(
                    {
                        "monitor_id": monitor_id,
                        "atcf_id": row["atcf_id"],
                        "geography": geography,
                        "email_type": "info",
                    }
                )
            except Exception as e:
                print(f"could not send info email for {monitor_id}: {e}")

    df_new_email_record = pd.DataFrame(dicts)
    df_combined_email_record = pd.concat(
        [df_existing_email_record, df_new_email_record], ignore_index=True
    )
    blob_name = f"{blob.PROJECT_PREFIX}/email/email_record.csv"
    blob.upload_csv_to_blob(blob_name, df_combined_email_record)
