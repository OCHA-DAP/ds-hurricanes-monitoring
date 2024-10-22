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
from src.email.utils import TEST_FCAST_MONITOR_ID
```

```python
plotting.update_plots("fcast", "all", verbose=True)
```

```python
test = plotting.create_plot(
    "al022024_fcast_2024-07-11T03:00:00", "map", "fcast", "all", debug=True
)
```

```python
test
```

```python
plotting.create_plot(
    "al162024_fcast_2024-10-20T21:00:00", "map", "fcast", "all", debug=False
)
```

```python
test
```

```python
test[test["issuance"] > "2024-06-28"]
```
