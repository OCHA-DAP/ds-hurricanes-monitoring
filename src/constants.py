import numpy as np

HAVANA1 = "CU09"

MIN_EMAIL_DISTANCE = 1000

LON_ZOOM_RANGE = np.array(
    [
        0.0007,
        0.0014,
        0.003,
        0.006,
        0.012,
        0.024,
        0.048,
        0.096,
        0.192,
        0.3712,
        0.768,
        1.536,
        3.072,
        6.144,
        11.8784,
        23.7568,
        47.5136,
        98.304,
        190.0544,
        360.0,
    ]
)

CHD_GREEN = "#1bb580"

PROJ_CRS = "EPSG:3857"

ISO3S = [
    # Cuba
    "cub",
    # Jamaica
    "jam",
    # Haiti
    "hti",
    # Dominican Republic
    "dom",
    # Bahamas
    "bhs",
    # Dominica
    "dma",
    # Saint Lucia
    "lca",
    # Saint Vincent and the Grenadines
    "vct",
    # Grenada
    "grd",
    # Trinidad and Tobago
    "tto",
    # Barbados
    "brb",
    # Saint Kitts and Nevis
    "kna",
    # Antigua and Barbuda
    "atg",
    # Honduras
    "hnd",
    # Belize
    "blz",
    # Nicaragua
    "nic",
    # Costa Rica
    "cri",
    # Guatemala
    "gtm",
    # El Salvador
    "slv",
    # Panama
    "pan",
]

ADMIN1_ISO3S = [
    # Cuba
    "cub",
    # Bahamas
    "bhs",
    # Honduras
    "hnd",
    # Guatemala
    "gtm",
    # Nicaragua
    "nic",
    # Costa Rica
    "cri",
    # Panama
    "pan",
]
