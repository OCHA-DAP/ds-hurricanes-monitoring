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
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from tqdm.auto import tqdm

from src.datasources import codab
from src.utils import blob
from src.constants import ISO3S, ADMIN1_ISO3S
```

```python
# for iso3 in tqdm(ISO3S):
#     codab.download_codab_to_blob(iso3)
```

```python
test = codab.load_combined_codab()
```

```python
fig, ax = plt.subplots(dpi=300)
test.plot(column="ADM_NAME", ax=ax)
ax.axis("off")
```

```python
test
```

```python
test["ADM_PCODE"].nunique()
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
