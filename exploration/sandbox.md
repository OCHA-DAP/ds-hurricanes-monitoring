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

# Sandbox

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
from src.utils import blob
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/monitoring/all_fcast_monitoring.parquet"
```

```python
df = blob.load_parquet_from_blob(blob_name)
```

```python
atcf_id = "al052025"
```

```python
dff = df[df["atcf_id"] == atcf_id]
```

```python
max_issued_time = dff["issue_time"].max()
```

```python
max_issued_time
```

```python
dff[(dff["issue_time"] == max_issued_time) & (dff["min_dist"] <= 500)]
```
