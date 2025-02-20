
# Extended Documentation for the Code

## Overview

This script provides a comprehensive pipeline for processing geospatial data from a shapefile, repairing geometries, splitting data based on attributes, uploading to a PostGIS database, and generating vector tiles. The code is modular, allowing each step of the process to be reused independently or as part of a larger workflow.

---

## Table of Contents

1. Dependencies
2. Logging Configuration
3. Functions
    - validate_and_repair_geometries
    - drop_unnecessary_columns
    - round_numeric_columns
    - split_by_attribute
    - upload_to_postgis
    - upload_split_data_to_postgis
    - generate_vector_tiles_from_postgis
    - convert_shapefile_to_postgis_tiles
    - check_dependencies
4. Main Execution
5. Example Usage

---

## Dependencies

The script relies on the following Python libraries:

- **geopandas** : For handling geospatial data.
- **sqlalchemy** : For interacting with databases.
- **psycopg2** : For connecting to PostgreSQL/PostGIS.
- **geoalchemy2** : For working with spatial data in SQLAlchemy.
- **os** : For file system operations.
- **logging** : For logging messages.

Before running the script, ensure these dependencies are installed. If any dependency is missing, the `check_dependencies` function will raise an error.

---

## Logging Configuration

The logging module is configured to log messages at the `INFO` level or higher. The format includes the timestamp, log level, and message content.

```python
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
```

This ensures that all important events during execution are logged for debugging and monitoring purposes.

---

## Functions

### validate_and_repair_geometries

**Purpose** : Validates and repairs invalid geometries in a GeoDataFrame.

**Parameters** :

- `gdf (GeoDataFrame)`: Input GeoDataFrame containing geometries.

**Returns** :

- `GeoDataFrame`: A new GeoDataFrame with repaired geometries.

**Logic** :

- Uses the `.buffer(0)` method to attempt repair of invalid geometries.
- Drops rows with null geometries after repair.

```python
def validate_and_repair_geometries(gdf):
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.buffer(0) if geom.is_valid else None)
    gdf = gdf[gdf.geometry.notnull()]  # Drop invalid geometries
    return gdf
```

---

### drop_unnecessary_columns

**Purpose** : Removes specified columns from a GeoDataFrame.

**Parameters** :

- `gdf (GeoDataFrame)`: Input GeoDataFrame.
- `columns_to_drop (list)`: List of column names to remove.

**Returns** :

- `GeoDataFrame`: A new GeoDataFrame with specified columns removed.

**Logic** :

- Attempts to drop the specified columns, ignoring errors if the columns do not exist.

```python
def drop_unnecessary_columns(gdf, columns_to_drop=['reference_', 'overlap_ra']):
    return gdf.drop(columns=columns_to_drop, errors='ignore')
```

---

### round_numeric_columns

**Purpose** : Rounds numeric columns in a GeoDataFrame to a specified number of decimal places.

**Parameters** :

- `gdf (GeoDataFrame)`: Input GeoDataFrame.
- `columns_to_round (dict)`: Dictionary mapping column names to the number of decimal places.

**Returns** :

- `GeoDataFrame`: A new GeoDataFrame with numeric columns rounded.

**Logic** :

- Iterates through the dictionary of columns and their rounding specifications.
- Rounds only numeric columns present in the GeoDataFrame.

```python
def round_numeric_columns(gdf, columns_to_round={'surface_ut': 2, 'production': 3, 'PROD_EURO': 4}):
    for column, decimals in columns_to_round.items():
        if column in gdf.columns and gdf[column].dtype in ['float64', 'float32']:
            gdf[column] = gdf[column].round(decimals)
    return gdf
```
---

### split_by_attribute

**Purpose** : Splits a GeoDataFrame into smaller subsets based on a specified attribute and exports them as FlatGeobuf files.

**Parameters** :

- `gdf (GeoDataFrame)`: Input GeoDataFrame.
- `attribute (str)`: Attribute to split the data by.
- `output_folder (str)`: Folder to save the output files.

**Returns** :

- `list`: List of paths to the exported FlatGeobuf files.

**Logic** :

- Groups the GeoDataFrame by the specified attribute.
- Exports each group as a separate FlatGeobuf file.

```python
def split_by_attribute(gdf, attribute="nom", output_folder="split_data"):
    if attribute not in gdf.columns:
        raise ValueError(f"Attribute '{attribute}' not found in the dataset.")
    os.makedirs(output_folder, exist_ok=True)
    fgb_paths = []
    for nom_value, subset_gdf in gdf.groupby(attribute):
        fgb_path = os.path.join(output_folder, f"{nom_value}.fgb")
        subset_gdf.to_file(fgb_path, driver="FlatGeobuf")
        fgb_paths.append(fgb_path)
    print(f"Data split into {len(fgb_paths)} subsets based on '{attribute}'.")
    return fgb_paths
```
---

### upload_to_postgis

**Purpose** : Uploads a GeoDataFrame to a PostGIS table.

**Parameters** :

- `gdf (GeoDataFrame)`: Input GeoDataFrame.
- `table_name (str)`: Name of the table in PostGIS.
- `connection_string (str)`: Connection string for PostGIS.
- `schema (str)`: Schema name in PostGIS.

**Logic** :

- Creates an SQLAlchemy engine using the connection string.
- Uploads the GeoDataFrame to the specified table in PostGIS.

```python
def upload_to_postgis(gdf, table_name, connection_string, schema="public"):
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
```
---

### upload_split_data_to_postgis

**Purpose** : Uploads multiple FlatGeobuf files to PostGIS.

**Parameters** :

- `fgb_paths (list)`: List of paths to FlatGeobuf files.
- `connection_string (str)`: Connection string for PostGIS.
- `schema (str)`: Schema name in PostGIS.

**Logic** :

- Reads each FlatGeobuf file as a GeoDataFrame.
- Derives the table name from the filename.
- Calls `upload_to_postgis` to upload the GeoDataFrame.

```python
def upload_split_data_to_postgis(fgb_paths, connection_string, schema="public"):
    for fgb_path in fgb_paths:
        subset_gdf = gpd.read_file(fgb_path)
        table_name = os.path.splitext(os.path.basename(fgb_path))[0]
        upload_to_postgis(subset_gdf, table_name, connection_string, schema)
```


---

### generate_vector_tiles_from_postgis

**Purpose** : Generates vector tiles from PostGIS for a specific zoom level and tile.

**Parameters** :

- `zoom (int)`: Zoom level.
- `x (int)`: X coordinate of the tile.
- `y (int)`: Y coordinate of the tile.
- `connection_string (str)`: Connection string for PostGIS.
- `schema (str)`: Schema name in PostGIS.

**Returns** :

- `bytes`: Binary MVT data for the specified tile.

**Logic** :

- Connects to the PostGIS database.
- Queries all tables in the specified schema.
- Generates MVT for each table and concatenates the results.

```python
def generate_vector_tiles_from_postgis(zoom, x, y, connection_string, schema="public"):
    try:
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE';
        """, (schema,))
        table_names = [row[0] for row in cursor.fetchall()]
        mvt_data = b""
        for table_name in table_names:
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
```

---

### convert_shapefile_to_postgis_tiles

**Purpose** : Converts a shapefile to vector tiles using PostGIS.

**Parameters** :

- `input_shapefile (str)`: Path to the input shapefile.
- `output_folder (str)`: Folder to save intermediate FlatGeobuf files.
- `connection_string (str)`: Connection string for PostGIS.
- `schema (str)`: Schema name in PostGIS.
- `attribute (str)`: Attribute to split the data by.

**Logic** :

- Reads the shapefile into a GeoDataFrame.
- Validates and repairs geometries.
- Ensures the CRS is EPSG:2154.
- Drops unnecessary columns.
- Rounds numeric columns.
- Splits the data by the specified attribute.
- Uploads the split data to PostGIS.

```python
def convert_shapefile_to_postgis_tiles(input_shapefile, output_folder, connection_string, schema="public", attribute="nom"):
    try:
        print("Reading shapefile...")
        gdf = gpd.read_file(input_shapefile)
        gdf = validate_and_repair_geometries(gdf)
        if len(gdf) == 0:
            logging.error("No valid geometries found in the input shapefile.")
            return
        gdf = gdf.to_crs(epsg=2154)
        gdf = drop_unnecessary_columns(gdf, columns_to_drop=['ref_area', 'overlap'])
        gdf = round_numeric_columns(gdf, columns_to_round={
            'surface_ut': 2, 'production': 2, 'PROD_EURO/': 2
        })
        fgb_paths = split_by_attribute(gdf, attribute=attribute, output_folder=output_folder)
        upload_split_data_to_postgis(fgb_paths, connection_string, schema)
        print("Data uploaded to PostGIS successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
```

---

### check_dependencies

**Purpose** : Checks if required dependencies are installed.

**Logic** :

- Imports necessary libraries and raises an error if any are missing.

```python
def check_dependencies():
    try:
        import geopandas
        import sqlalchemy
        import psycopg2
        import geoalchemy2
        print("All required dependencies are installed.")
    except ImportError as e:
        raise ImportError(f"Missing dependency: {e}")
```
---

## Main Execution

The script checks dependencies and calls the `convert_shapefile_to_postgis_tiles` function with example parameters.

```python
if __name__ == "__main__":
    from utils import check_dependencies
    check_dependencies()
    input_shapefile = "/home/mahdi/interface/data/output/aligned_results_20250219_165037.shp"
    output_folder = "/home/mahdi/interface/data/output/Vector_tiles/"
    connection_string = "postgresql://mahdi:mahdi@localhost:5432/roofs"
    convert_shapefile_to_postgis_tiles(input_shapefile, output_folder, connection_string, schema="public", attribute="nom")
```
---
## result : 

DBeaver:
![[dbeaver_YYweeLokPi.png]]

DB Manager in qgis :

![[qgis-ltr-bin_HZFcVruiRo.png]]