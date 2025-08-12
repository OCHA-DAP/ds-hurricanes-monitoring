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

# Wind speed decay

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import matplotlib.pyplot as plt
import numpy as np

from src.datasources import ibtracs, nhc
from src.monitoring import monitoring_utils
from src.constants import *
from src.datasources.ibtracs import get_similar_storms
```

```python
df_sid_name = ibtracs.load_sid_names()
```

```python
df_stats = ibtracs.load_all_adm_wind_stats()
```

```python
df_stats = df_stats.merge(df_sid_name)
```

```python
df_stats
```

```python
df_monitoring = monitoring_utils.load_existing_monitoring_points(
    "fcast", "all"
)
```

```python
df_monitoring
```

```python
pcodes = ["HT", "VC"]
adm_winds = [80, 20]
similar_storms = []
n_similar_storms = 3
for pcode, adm_wind in zip(pcodes, adm_winds):
    df_adm = df_stats[df_stats["ADM_PCODE"] == pcode].copy()
    df_adm["wind_dif"] = df_adm["adm_wind"] - adm_wind
    df_adm["wind_dif_abs"] = df_adm["wind_dif"].abs()
    df_adm = df_adm.sort_values("wind_dif_abs")
    df_similar = df_adm.iloc[:3]
    cols = ["sid", "nameyear", "adm_wind"]
    dicts_s = df_similar[cols].to_dict("records")
    similar_storms.append(dicts_s)
```

```python
similar_storms
```

```python
monitor_id = "al022024_fcast_2024-06-29T09:00:00"
```

```python
df_monitoring["email_monitor_id"] = df_monitoring["monitor_id"].apply(
    lambda x: "_".join(x.split("_")[:-1])
)
monitoring_group = df_monitoring[
    (df_monitoring["email_monitor_id"] == monitor_id)
]
```

```python
adm_email_content = monitoring_group[
    monitoring_group["min_dist"] < ALL_MIN_EMAIL_DISTANCE
].copy()
```

```python
adm_email_content["similar_storms"] = get_similar_storms(
    adm_email_content["ADM_PCODE"], adm_email_content["max_adm_wind"]
)
```

```python
def get_similar_storms_str(x_list):
    return "\n".join(
        [
            f'{x.get("nameyear")} ({int(x.get("adm_wind"))} knots)'
            for x in x_list
        ]
    )


get_similar_storms_str(adm_email_content["similar_storms"].iloc[0])
```

```python
adm_email_content["similar_storms_str"] = adm_email_content[
    "similar_storms"
].apply(get_similar_storms_str)
```

```python
adm_email_content
```

```python

```
