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

# Jamaica EM-DAT

Matching `sid` to EM-DAT

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import duckdb
import ocha_stratus as stratus

from src.utils.blob import PROJECT_PREFIX
```

```python
# get blob URL - note that this contains the SAS so be careful
blob_name = "emdat/processed/emdat_all.parquet"
url = (
    stratus.get_container_client(container_name="global")
    .get_blob_client(blob_name)
    .url
)

# load using duckdb (query is for flooding in Cameroon)
con = duckdb.connect()
df_in = con.execute(
    f"""
    SELECT *
    FROM read_parquet('{url}')
    WHERE ISO = 'JAM' AND "Disaster Subtype" = 'Tropical cyclone' AND Historic = 'No'
"""
).df()
```

```python
df_in.columns
```

```python
cols = ["DisNo.", "Event Name", "Start Year", "Start Month", "Start Day"]
```

```python
df_in[cols]
```

```python
dis2sid = {
    "2001-0615-JAM": "2001303N13276",
    "2002-0627-JAM": "2002265N10315",
    "2002-0656-JAM": "2002258N10300",
    "2004-0415-JAM": "2004223N11301",
    "2004-0462-JAM": "2004247N10332",
    "2005-0351-JAM": "2005186N12299",
    "2005-0382-JAM": "2005192N11318",
    "2005-0585-JAM": "2005289N18282",
    "2007-0360-JAM": "2007225N12331",
    "2007-0523-JAM": "2007297N18300",
    "2008-0338-JAM": "2008229N18293",
    "2008-0338-JAM": "2008238N13293",
    "2010-0501-JAM": "2010271N19276",
    "2012-0410-JAM": "2012296N14283",
    "2016-0355-JAM": "2016273N13300",
    "2024-0422-JAM": "2024181N09320",
    "2024-0822-JAM": "2024309N13283",
}
```

```python
df_in["sid"] = df_in["DisNo."].replace(dis2sid)
```

```python
blob_name = f"{PROJECT_PREFIX}/processed/emdat/jam_emdat.parquet"
stratus.upload_parquet_to_blob(df_in, blob_name)
```
