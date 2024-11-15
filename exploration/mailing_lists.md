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
from src.email.utils import is_valid_email
```

## Test list

```python
df_test = pd.DataFrame(
    columns=["name", "email", "cub", "all"],
    data=[
        ["TEST_NAME", "tristan.downing@un.org", "to", "to"],
        ["TEST_NAME", "downing.tristan@gmail.com", "to", "to"],
    ],
)
blob_name = f"{blob.PROJECT_PREFIX}/email/test_distribution_list.csv"
blob.upload_csv_to_blob(blob_name, df_test)
df_test
```

## Actual list

```python
df = pd.DataFrame(
    columns=["name", "email", "cub", "all"],
    data=[
        # OCHA HQ
        ["Jacopo Damelio", "jacopo.damelio@un.org", "to", "to"],
        ["Nicolas Rost", "rostn@un.org", "to", "to"],
        ["Regina Omlor", "regina.omlor@un.org", "to", "to"],
        # JEU
        ["Charlotta Benedek", "benedek@un.org", "to", "to"],
        ["Diego Reyes", "diego.reyes1@un.org", "to", "to"],
        # OCHA LAC
        ["Dario Alvarez", "alvarez6@un.org", None, "to"],
        ["Erlin Palma Garcia", "palmae@un.org", None, "to"],
        ["Vera Goldschmidt Ferreira", "goldschmidtv@un.org", None, "to"],
        ["Milena Montano", "milena.montano@un.org", None, "to"],
        ["Joel Cruz", "cruz23@un.org", None, "to"],
        ["Himshem Him", "himh@un.org", None, "to"],
        ["Gianni Morelli", "morelli@un.org", None, "to"],
        ["Veronique Durroux", "durroux@un.org", None, "to"],
        ["Brenda Eriksen", "eriksenb@un.org", None, "to"],
        ["Marc Belanger", "belanger2@un.org", None, "to"],
        # CHD DS
        ["Tristan Downing", "tristan.downing@un.org", "cc", "cc"],
        ["Zachary Arno", "zachary.arno@un.org", "cc", "cc"],
        ["Pauline Ndirangu", "pauline.ndirangu@un.org", "cc", "cc"],
    ],
)
df
```

```python
print("invalid emails: ")
display(df[~df["email"].apply(is_valid_email)])
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/email/distribution_list.csv"
blob.upload_csv_to_blob(blob_name, df)
df
```

```python

```
