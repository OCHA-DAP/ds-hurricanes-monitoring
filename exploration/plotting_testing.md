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

# Plotting testing

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
from src.email import plotting
```

```python
plotting.update_plots("fcast", "cub", verbose=True)
```
