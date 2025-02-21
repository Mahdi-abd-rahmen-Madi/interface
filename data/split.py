# split.py
import geopandas as gpd
import os
import logging

# split.py
def split_by_attribute(gdf, attribute="nom", output_folder="split_data"):
    """
    Split a GeoDataFrame into smaller subsets based on the specified attribute.
    
    Args:
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

    logging.info(f"Data split into {len(fgb_paths)} subsets based on '{attribute}'.")
    return fgb_paths


# split.py
def upload_split_data_to_postgis(fgb_paths, connection_string, schema="public"):
    """
    Upload split data to PostGIS.
    
    Args:
        fgb_paths (list): List of paths to FlatGeobuf files.
        connection_string (str): Connection string for PostGIS.
        schema (str): Schema name in PostGIS.
    """
    from sqlalchemy import create_engine

    for fgb_path in fgb_paths:
        # Read FlatGeobuf file
        subset_gdf = gpd.read_file(fgb_path)

        # Derive table name from the filename
        table_name = os.path.splitext(os.path.basename(fgb_path))[0]

        engine = create_engine(connection_string)
        subset_gdf.to_postgis(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists="replace",
            index=False,
            dtype={"geometry": "Geometry"}
        )

        # Log successful upload
        logging.info(f"Uploaded data to PostGIS table: {schema}.{table_name}")

        # Validate column names after upload
        validate_columns_after_upload(engine, schema, table_name)


def validate_columns_after_upload(engine, schema, table_name):
    """
    Validate column names in the uploaded PostGIS table.
    
    Args:
        engine: SQLAlchemy engine for database connection.
        schema (str): Schema name in PostGIS.
        table_name (str): Table name in PostGIS.
    """
    query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s;
    """
    with engine.connect() as conn:
        result = conn.execute(query, (schema, table_name)).fetchall()
        columns = [col[0] for col in result]
        logging.info(f"Columns in table {schema}.{table_name}: {columns}")

        if "nom" not in columns:
            raise ValueError(f"Column 'nom' not found in uploaded table: {schema}.{table_name}")