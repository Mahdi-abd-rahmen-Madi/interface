# config.py

# Input/output paths
INPUT_SHAPEFILE_1 = "/home/mahdi/interface/data/shapefiles/reference.shp"
INPUT_SHAPEFILE_2 = "/home/mahdi/interface/data/shapefiles/roof.shp"
OUTPUT_FOLDER = "data/"

# CRS settings
TARGET_CRS = "EPSG:2154"

# Alignment thresholds
MAX_DISTANCE = 25
MIN_OVERLAP_RATIO = 0.5

# Merge thresholds
MIN_AREA_THRESHOLD = 1000
MAX_MERGE_DISTANCE = 50

# Dissolve buffer distance
BUFFER_DISTANCE = 0.1

# PostGIS connection string
POSTGIS_CONNECTION_STRING = "postgresql://mahdi:mahdi@localhost:5432/roofs"
POSTGIS_SCHEMA = "public"