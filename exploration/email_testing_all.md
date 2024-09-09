---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.1
  kernelspec:
    display_name: ds-hurricanes-monitoring
    language: python
    name: ds-hurricanes-monitoring
---

# Email testing - all

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import traceback

import geopandas as gpd
import pandas as pd
from tqdm.auto import tqdm

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
from src.datasources import codab
```

```python
geography = "all"
verbose = True
```

```python
adm = codab.load_combined_codab()
```

```python
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
# if TEST_STORM:
#     df_monitoring = add_test_row_to_monitoring(
#         df_monitoring, "fcast", geography
#     )
#     df_existing_email_record = df_existing_email_record[
#         ~(
#             (df_existing_email_record["atcf_id"] == TEST_ATCF_ID)
#             & (df_existing_email_record["email_type"] == "info")
#         )
#     ]
```

```python
df_existing_email_record
```

```python
df_monitoring["email_monitor_id"] = df_monitoring["monitor_id"].apply(
    lambda x: "_".join(x.split("_")[:-1])
)
```

```python
dicts = []
for email_monitor_id, email_group in df_monitoring.groupby("email_monitor_id"):
    if email_group["min_dist"].min() > MIN_EMAIL_DISTANCE:
        if verbose:
            print(
                f"min of min_dist is {email_group['min_dist'].min()}, "
                f"skipping info email for {email_monitor_id}"
            )
        continue
    if (
        email_monitor_id
        in df_existing_email_record[
            (df_existing_email_record["email_type"] == "info")
            & (df_existing_email_record["geography"] == geography)
        ]["monitor_id"].unique()
    ):
        if verbose:
            print(f"already sent info email for {monitor_id}")
        continue

    try:
        print(f"sending info email for {email_monitor_id}")
        send_info_email(
            monitor_id=email_monitor_id,
            fcast_obsv="fcast",
            geography=geography,
        )
        dicts.append(
            {
                "monitor_id": email_monitor_id,
                "atcf_id": email_group.iloc[0]["atcf_id"],
                "geography": geography,
                "email_type": "info",
            }
        )
    except Exception as e:
        print(f"could not send info email for {email_monitor_id}: {e}")
        traceback.print_exc()
```

```python
pd.DataFrame([{"ADM_NAME": "SADF"}, {"ADM_NAME": "fds"}]).to_dict("records")
```
