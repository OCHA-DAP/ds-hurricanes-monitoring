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

# Mailing lists

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd

from src.utils import blob
```

## Test list

```python
df = pd.DataFrame(
    columns=["name", "email", "cub", "all"],
    data=[
        ["TEST_NAME", "tristan.downing@un.org", "to", "to"],
        ["TEST_NAME", "downing.tristan@gmail.com", "to", "to"],
    ],
)
blob_name = f"{blob.PROJECT_PREFIX}/email/test_distribution_list.csv"
blob.upload_csv_to_blob(blob_name, df)
df
```

## Actual list

```python
df = pd.DataFrame(
    columns=["name", "email", "cub", "all"],
    data=[
        # OCHA HQ
        ["Jacopo Damelio", "jacopo.damelio@un.org", "to", "to"],
        ["Nicolas Rost", "rostn@un.org", "to", "to"],
        # CHD DS
        ["Tristan Downing", "tristan.downing@un.org", "cc", "cc"],
        ["Zachary Arno", "zachary.arno@un.org", "cc", "cc"],
        ["Pauline Ndirangu", "pauline.ndirangu@un.org", "cc", "cc"],
    ],
)
df
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/email/distribution_list.csv"
blob.upload_csv_to_blob(blob_name, df)
df
```

```python

```
