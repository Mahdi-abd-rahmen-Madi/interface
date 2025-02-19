import geopandas as gpd
from sqlalchemy import create_engine
import psycopg2
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def validate_and_repair_geometries(gdf):
    """
    Validate and repair geometries in a GeoDataFrame.

    Parameters:
        gdf (GeoDataFrame): Input GeoDataFrame.

    Returns:
        GeoDataFrame: GeoDataFrame with repaired geometries.
    """
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.buffer(0) if geom.is_valid else None)
    gdf = gdf[gdf.geometry.notnull()]  # Drop invalid geometries
    return gdf


def drop_unnecessary_columns(gdf, columns_to_drop=['reference_', 'overlap_ra']):
    """
    Drop unnecessary columns from a GeoDataFrame.

    Parameters:
        gdf (GeoDataFrame): Input GeoDataFrame.
        columns_to_drop (list): List of column names to drop.

    Returns:
        GeoDataFrame: GeoDataFrame with specified columns dropped.
    """
    return gdf.drop(columns=columns_to_drop, errors='ignore')


def round_numeric_columns(gdf, columns_to_round={'surface_ut': 2, 'production': 3, 'PROD_EURO': 4}):
    """
    Round numeric columns in a GeoDataFrame to a specified number of decimal places.

    Parameters:
        gdf (GeoDataFrame): Input GeoDataFrame.
        columns_to_round (dict): Dictionary mapping column names to the number of decimal places.

    Returns:
        GeoDataFrame: GeoDataFrame with numeric columns rounded.
    """
    for column, decimals in columns_to_round.items():
        if column in gdf.columns and gdf[column].dtype in ['float64', 'float32']:
            gdf[column] = gdf[column].round(decimals)
    return gdf


def split_by_attribute(gdf, attribute="nom", output_folder="split_data"):
    """
    Split a GeoDataFrame into smaller subsets based on the specified attribute.

    Parameters:
        gdf (GeoDataFrame): Input GeoDataFrame.
        attribute (str): Attribute to split the data by.
        output_folder (str): Folder to save the output files.

    Returns:
        list: List of paths to the exported FlatGeobuf files.
    """
    if attribute not in gdf.columns:
        raise ValueError(f"Attribute '{attribute}' not found in the dataset.")

    os.makedirs(output_folder, exist_ok=True)
    fgb_paths = []

    for nom_value, subset_gdf in gdf.groupby(attribute):
        # Export subset to FlatGeobuf
        fgb_path = os.path.join(output_folder, f"{nom_value}.fgb")
        subset_gdf.to_file(fgb_path, driver="FlatGeobuf")
        fgb_paths.append(fgb_path)

    print(f"Data split into {len(fgb_paths)} subsets based on '{attribute}'.")
    return fgb_paths


def upload_to_postgis(gdf, table_name, connection_string, schema="public"):
    """
    Upload a GeoDataFrame to PostGIS.

    Parameters:
        gdf (GeoDataFrame): Input GeoDataFrame.
        table_name (str): Name of the table in PostGIS.
        connection_string (str): Connection string for PostGIS.
        schema (str): Schema name in PostGIS.
    """
    engine = create_engine(connection_string)
    gdf.to_postgis(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
        dtype={"geometry": "Geometry"}
    )
    print(f"Uploaded data to PostGIS table: {schema}.{table_name}")


def upload_split_data_to_postgis(fgb_paths, connection_string, schema="public"):
    """
    Upload split data to PostGIS.

    Parameters:
        fgb_paths (list): List of paths to FlatGeobuf files.
        connection_string (str): Connection string for PostGIS.
        schema (str): Schema name in PostGIS.
    """
    for fgb_path in fgb_paths:
        # Read FlatGeobuf file
        subset_gdf = gpd.read_file(fgb_path)

        # Derive table name from the filename
        table_name = os.path.splitext(os.path.basename(fgb_path))[0]

        # Upload to PostGIS
        upload_to_postgis(subset_gdf, table_name, connection_string, schema)


def generate_vector_tiles_from_postgis(zoom, x, y, connection_string, schema="public"):
    """
    Generate vector tiles from PostGIS for a specific zoom level and tile.

    Parameters:
        zoom (int): Zoom level.
        x (int): X coordinate of the tile.
        y (int): Y coordinate of the tile.
        connection_string (str): Connection string for PostGIS.
        schema (str): Schema name in PostGIS.

    Returns:
        bytes: Binary MVT data for the specified tile.
    """
    try:
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()

        # Get all table names in the schema
        cursor.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE';
        """, (schema,))
        table_names = [row[0] for row in cursor.fetchall()]

        mvt_data = b""

        for table_name in table_names:
            # Generate MVT for each table
            cursor.execute(f"""
                SELECT ST_AsMVT(q, %s, 4096, 'geom')
                FROM (
                    SELECT id, PROD_EURO, ST_AsMVTGeom(geometry, TileBBox(%s, %s, %s, 4096)) AS geom
                    FROM {schema}.%s
                    WHERE ST_Intersects(geometry, TileBBox(%s, %s, %s, 4096))
                ) q;
            """, (table_name, zoom, x, y, table_name, zoom, x, y))

            tile_data = cursor.fetchone()[0]
            if tile_data:
                mvt_data += tile_data

        cursor.close()
        conn.close()

        return mvt_data

    except Exception as e:
        logging.error(f"An error occurred while generating vector tiles: {e}")
        return None


def convert_shapefile_to_postgis_tiles(input_shapefile, output_folder, connection_string, schema="public", attribute="nom"):
    """
    Converts a shapefile to vector tiles using PostGIS.

    Parameters:
        input_shapefile (str): Path to the input shapefile.
        output_folder (str): Folder to save intermediate FlatGeobuf files.
        connection_string (str): Connection string for PostGIS.
        schema (str): Schema name in PostGIS.
        attribute (str): Attribute to split the data by.
    """
    try:
        print("Reading shapefile...")
        gdf = gpd.read_file(input_shapefile)

        # Step 1: Validate and repair geometries
        print("Validating and repairing geometries...")
        gdf = validate_and_repair_geometries(gdf)

        if len(gdf) == 0:
            logging.error("No valid geometries found in the input shapefile.")
            return

        # Step 2: Ensure CRS is EPSG:2154
        print("Ensuring CRS is EPSG:2154...")
        gdf = gdf.to_crs(epsg=2154)

        # Step 3: Drop unnecessary columns
        print("Dropping unnecessary attributes (reference_, overlap_ra)...")
        gdf = drop_unnecessary_columns(gdf, columns_to_drop=['reference_', 'overlap_ra'])

        # Step 4: Round numeric columns
        print("Rounding numeric attributes (surface_ut, production, PROD_EURO)...")
        gdf = round_numeric_columns(gdf, columns_to_round={
            'surface_ut': 2,       # Round to 2 decimal places
            'production': 3,       # Round to 3 decimal places
            'PROD_EURO': 4         # Round to 4 decimal places
        })

        # Step 5: Split data by the "nom" attribute
        print(f"Splitting data by attribute '{attribute}'...")
        fgb_paths = split_by_attribute(gdf, attribute=attribute, output_folder=output_folder)

        # Step 6: Upload split data to PostGIS
        print("Uploading split data to PostGIS...")
        upload_split_data_to_postgis(fgb_paths, connection_string, schema)

        print("Data uploaded to PostGIS successfully.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    # Example usage
    input_shapefile = "/home/mahdi/interface/data/output/aligned_results_20250217_093030.shp"  # Replace with your shapefile path
    output_folder = "/home/mahdi/interface/data/output/Vector_tiles/"  # Replace with desired output folder
    connection_string = "postgresql://mahdi:mahdi@localhost:5432/roof" #"postgresql://user:password@localhost:5432/mydb"   Replace with your PostGIS connection string

    convert_shapefile_to_postgis_tiles(input_shapefile, output_folder, connection_string, schema="public", attribute="nom")