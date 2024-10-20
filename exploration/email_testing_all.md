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

from src.constants import *
from src.email.send_emails import send_info_email
from src.email.utils import (
    TEST_ATCF_ID,
    TEST_STORM,
    TEST_FCAST_MONITOR_ID,
    add_test_row_to_monitoring,
    load_email_record,
)
from src.monitoring import monitoring_utils
from src.utils import blob
from src.datasources import codab
```

```python
geography = "all"
```

```python
df_existing_email_record = load_email_record()
df_existing_email_record
```

```python
send_info_email(TEST_FCAST_MONITOR_ID, "fcast", "all")
```

```python
MONITOR_ID_TO_DROP = "al022024_fcast_2024-07-02T21:00:00"
```

```python
df_existing_email_record = df_existing_email_record[
    (df_existing_email_record["monitor_id"] != MONITOR_ID_TO_DROP)
    | (df_existing_email_record["geography"] != geography)
]
df_existing_email_record
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/email/email_record.csv"
blob.upload_csv_to_blob(blob_name, df_existing_email_record)
```

```python
len(df_existing_email_record)
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
df_monitoring["monitor_id"].str.contains("al02")
```

```python
df_relevant = df_monitoring[df_monitoring["min_dist"] < 250]
```

```python
df_relevant.groupby("email_monitor_id")["closest_s"].max()
```

```python
df_relevant.groupby("email_monitor_id").size()
```

```python

```
