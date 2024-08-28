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

# CODAB

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
from src.datasources import codab
```

```python
codab.download_codab_to_blob("cub")
```

```python
adm = codab.load_codab_from_blob("cub")
```

```python
adm.plot()
```

```python
adm_aoi = codab.load_codab_from_blob("cub", aoi_only=True)
```

```python
adm_aoi.plot()
```
