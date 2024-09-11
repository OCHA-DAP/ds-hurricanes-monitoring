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
```

```python
df = nhc.load_recent_glb_forecasts()
```

```python
test = monitoring_utils.update_all_fcast_monitoring(
    clobber=True, disable_progress_bar=False
)
```

```python
X = range(0, 251)
fig, ax = plt.subplots()
ax.scatter(x=X, y=[ibtracs.estimate_wind_at_distance(80, x) for x in X])
```

```python
pd.DataFrame()
```

```python
ibtracs.estimate_wind_at_distance(
    np.array([80, 90, 80, 90]), np.array([1, 2, 100, 250])
)
```

```python
test.plot.scatter(x="min_dist", y="max_adm_wind")
```

```python
test
```
