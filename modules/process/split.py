# split.py
import os
import geopandas as gpd
from sqlalchemy import create_engine
from config import OUTPUT_FOLDER, POSTGIS_CONNECTION_STRING, POSTGIS_SCHEMA

def split_and_save(gdf, attribute="alignment"):
    """
    Split a GeoDataFrame into smaller subsets based on the specified attribute.
    """
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    for value, subset in gdf.groupby(attribute):
        output_path = os.path.join(OUTPUT_FOLDER, f"{value}.fgb")
        subset.to_file(output_path, driver="FlatGeobuf")

def upload_to_postgis(gdf, table_name):
    """
    Upload a GeoDataFrame to PostGIS.
    """
    engine = create_engine(POSTGIS_CONNECTION_STRING)
    gdf.to_postgis(table_name, engine, schema=POSTGIS_SCHEMA, if_exists="replace", index=False)