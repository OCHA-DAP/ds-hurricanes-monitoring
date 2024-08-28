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

# Email testing

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd

from src.utils import blob
from src.monitoring import monitoring_utils
```

```python

```

```python
blob_name = f"{blob.PROJECT_PREFIX}/monitoring/cub_fcast_monitoring.parquet"
blob.upload_parquet_to_blob(blob_name, pd.DataFrame(columns=["monitor_id"]))
```

```python
test = blob.load_parquet_from_blob(
    monitoring_utils.get_blob_name("fcast", "cub")
)
```

```python
test
```

```python
monitoring_utils.update_fcast_monitoring("cub", verbose=True, clobber=True)
```

```python

```
