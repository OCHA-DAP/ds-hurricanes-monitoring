import smtplib
import ssl
from email.headerregistry import Address
from email.message import EmailMessage
from email.utils import make_msgid
from typing import Literal

import pytz
from html2text import html2text
from jinja2 import Environment, FileSystemLoader

from src.email.utils import (
    TEST_FCAST_MONITOR_ID,
    TEST_OBSV_MONITOR_ID,
    add_test_row_to_monitoring,
    get_distribution_list,
    TEST_STORM,
    TEMPLATES_DIR,
    EMAIL_ADDRESS,
    EMAIL_HOST,
    EMAIL_PORT,
    EMAIL_USERNAME,
    EMAIL_PASSWORD,
    STATIC_DIR,
)
from src.monitoring import monitoring_utils


def send_info_email(
    monitor_id: str,
    fcast_obsv: Literal["fcast", "obsv"],
    geography: Literal["cub", "all"],
):
    if geography == "cub":
        send_cub_info_email(monitor_id, fcast_obsv)
    else:
        raise NotImplementedError("Only Cuba is supported for now")


def send_cub_info_email(monitor_id: str, fcast_obsv: Literal["fcast", "obsv"]):
    df_monitoring = monitoring_utils.load_existing_monitoring_points(
        fcast_obsv, geography="cub"
    )
    if monitor_id in [TEST_FCAST_MONITOR_ID, TEST_OBSV_MONITOR_ID]:
        df_monitoring = add_test_row_to_monitoring(df_monitoring, fcast_obsv)
    monitoring_point = df_monitoring.set_index("monitor_id").loc[monitor_id]
    ny_tz = pytz.timezone("America/New_York")
    cyclone_name = monitoring_point["name"]
    issue_time = monitoring_point["issue_time"]
    issue_time_ny = issue_time.astimezone(ny_tz)
    pub_time = issue_time_ny.strftime("%H:%M")
    pub_date = issue_time_ny.strftime("%-d %b %Y")
    fcast_obsv_str = "observation" if fcast_obsv == "obsv" else "forecast"

    min_dist = monitoring_point["min_dist"]
    closest_s = monitoring_point["closest_s"]

    distribution_list = get_distribution_list()
    to_list = distribution_list[distribution_list["cub"] == "to"]
    cc_list = distribution_list[distribution_list["cub"] == "cc"]

    test_subject = "TEST: " if TEST_STORM else ""

    environment = Environment(loader=FileSystemLoader(TEMPLATES_DIR))

    template_name = "informational"
    template = environment.get_template(f"{template_name}.html")
    msg = EmailMessage()
    msg.set_charset("utf-8")
    msg["Subject"] = (
        f"{test_subject}Cuba – {cyclone_name} "
        f"forecast issued {pub_time}, {pub_date} "
    )
    msg["From"] = Address(
        "OCHA Centre for Humanitarian Data",
        EMAIL_ADDRESS.split("@")[0],
        EMAIL_ADDRESS.split("@")[1],
    )
    msg["To"] = [
        Address(
            row["name"],
            row["email"].split("@")[0],
            row["email"].split("@")[1],
        )
        for _, row in to_list.iterrows()
    ]
    msg["Cc"] = [
        Address(
            row["name"],
            row["email"].split("@")[0],
            row["email"].split("@")[1],
        )
        for _, row in cc_list.iterrows()
    ]
    # map_cid = make_msgid(domain="humdata.org")
    # scatter_cid = make_msgid(domain="humdata.org")
    chd_banner_cid = make_msgid(domain="humdata.org")
    ocha_logo_cid = make_msgid(domain="humdata.org")

    html_str = template.render(
        name=cyclone_name,
        pub_time=pub_time,
        pub_date=pub_date,
        fcast_obsv=fcast_obsv_str,
        test_email=TEST_STORM,
        min_dist=int(min_dist),
        closest_s=int(closest_s),
        # map_cid=map_cid[1:-1],
        # scatter_cid=scatter_cid[1:-1],
        chd_banner_cid=chd_banner_cid[1:-1],
        ocha_logo_cid=ocha_logo_cid[1:-1],
    )
    text_str = html2text(html_str)
    msg.set_content(text_str)
    msg.add_alternative(html_str, subtype="html")

    # for plot_type, cid in zip(["map", "scatter"], [map_cid, scatter_cid]):
    #     blob_name = get_plot_blob_name(monitor_id, plot_type)
    #     image_data = io.BytesIO()
    #     blob_client = blob.get_container_client().get_blob_client(blob_name)
    #     blob_client.download_blob().download_to_stream(image_data)
    #     image_data.seek(0)
    #     msg.get_payload()[1].add_related(
    #         image_data.read(), "image", "png", cid=cid
    #     )

    for filename, cid in zip(
        ["centre_banner.png", "ocha_logo_wide.png"],
        [chd_banner_cid, ocha_logo_cid],
    ):
        img_path = STATIC_DIR / filename
        with open(img_path, "rb") as img:
            msg.get_payload()[1].add_related(
                img.read(), "image", "png", cid=cid
            )

    context = ssl.create_default_context()

    with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT, context=context) as server:
        server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
        server.sendmail(
            EMAIL_ADDRESS,
            to_list["email"].tolist() + cc_list["email"].tolist(),
            msg.as_string(),
        )
