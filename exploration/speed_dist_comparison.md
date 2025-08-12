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

# Wind / distance comparison

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import matplotlib.pyplot as plt
import numpy as np

from src.datasources.ibtracs import estimate_wind_at_distance
```

```python
ONE_TO_TEN_FACTOR = 0.87
THREE_TO_TEN_FACTOR = 0.95
```

```python
framework_combos = [
    ("FJI 1", 86, 250),
    ("FJI 2, \n MOZ 1 \n", 64, 0),
    ("\n BGD", 64 * THREE_TO_TEN_FACTOR, 0),
    ("HTI fcast", 64 * ONE_TO_TEN_FACTOR, 230),
    ("HTI obsv", 50 * ONE_TO_TEN_FACTOR, 230),
    ("MOZ 2", 48, 0),
    ("MDG", 90, 100),
]
```

```python
distances = np.linspace(0, 260, 100)
windspeeds = np.linspace(0, 140, 100)
levels = np.linspace(0, 140, 15)

# Create meshgrid
X, Y = np.meshgrid(distances, windspeeds)

# Calculate wind speeds for the meshgrid
Z = np.zeros_like(X)  # Initialize a 2D array to store wind speed values

# Loop over the wind speeds (vmax values) from the windspeeds list
for i in range(X.shape[0]):
    for j in range(X.shape[1]):
        vmax = Y[
            i, j
        ]  # Get the corresponding vmax value from windspeeds (Y in meshgrid)
        Z[i, j] = estimate_wind_at_distance(
            vmax, X[i, j]
        )  # Calculate wind speed at given distance and vmax

# Plot the contour
fig, ax = plt.subplots(figsize=(8, 6), dpi=200)
cp = ax.contourf(
    X, Y, Z, levels=levels, cmap="Spectral_r"
)  # Set your contour levels

cbar = fig.colorbar(cp)
cbar.set_label("Wind speed at distance from center of storm (knots)")

# Plot labels as annotations
for label, wind_speed, distance in framework_combos:
    ha = "left" if distance < 200 else "right"
    ax.plot(distance, wind_speed, marker=".", color="k")
    ax.annotate(
        " " + label + " ",
        (distance, wind_speed),
        ha=ha,
        va="center",
        fontsize=8,
        color="k",
    )

# Set labels and title
ax.set_xlabel("Distance from center of storm (km)")
ax.set_ylabel("Wind speed at center of storm (knots)")
ax.set_title(
    "Cyclone framework wind speed / distance triggers\n"
    "(all wind speeds translated into 10-minute average)"
)

plt.show()
```

```python

```

```python
estimate_wind_at_distance(20, 100)
```

```python

```
