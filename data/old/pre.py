import subprocess
import os
import logging
import csv
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def run_command(command):
    """Run a command using subprocess and handle errors."""
    try:
        logging.debug(f"Running command: {' '.join(command)}")
        result = subprocess.run(command, check=True, text=True, capture_output=True)
        logging.debug(result.stdout)
        return result.stdout
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed with return code {e.returncode}")
        logging.error(e.stderr)
        raise

def validate_file(file_path):
    """Check if the file exists."""
    if not os.path.isfile(file_path):
        logging.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")

def read_gcps_from_csv(csv_file):
    """Read GCPs from a CSV file and return a list of GCP arguments."""
    gcps = []
    try:
        with open(csv_file, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                gcps.extend([
                    "-gcp", row['source_x'], row['source_y'],
                    row['target_x'], row['target_y']
                ])
        return gcps
    except Exception as e:
        logging.error(f"Failed to read GCPs from CSV file: {e}")
        raise

def get_layer_name(shapefile):
    """Get the layer name from a shapefile using ogrinfo."""
    try:
        cmd = ["ogrinfo", "-so", shapefile]
        output = run_command(cmd)
        for line in output.splitlines():
            if line.strip().startswith("1:"):
                # Extract the layer name from the line
                layer_name = line.split(":")[1].split()[0].strip()
                logging.info(f"Determined layer name: {layer_name}")
                return layer_name
        logging.error("Could not determine layer name from shapefile.")
        logging.debug(f"Raw ogrinfo output:\n{output}")
        raise ValueError("Could not determine layer name from shapefile.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to get layer name: {e.stderr}")
        raise

def validate_geometries(shapefile):
    """Validate geometries in the shapefile."""
    try:
        cmd = ["ogrinfo", "-al", "-so", shapefile]
        output = run_command(cmd)
        if "Invalid geometry" in output:
            logging.error("Invalid geometries detected in the shapefile.")
            raise ValueError("Invalid geometries detected in the shapefile.")
        logging.info("All geometries are valid.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to validate geometries: {e.stderr}")
        raise

def load_to_postgis(shapefile, layer_name, postgis_config):
    """
    Load a shapefile into a PostGIS database with spatial indexing, CRS validation,
    field length checks, reserved word avoidance, and geometry validation.
    """
    host = postgis_config.get("host", "localhost")
    port = postgis_config.get("port", "5432")
    dbname = postgis_config.get("dbname", "roofs")  # Ensure this matches the database name
    user = postgis_config.get("user", "postgres")  # Ensure this matches the username
    password = postgis_config.get("password", "mahdi987456")  # Ensure this matches the password
    schema = postgis_config.get("schema", "public")  # Default schema
    table_name = os.path.splitext(os.path.basename(shapefile))[0]

    # Construct the connection string
    pg_connection = f"PG:host={host} port={port} dbname={dbname} user={user} password={password}"

    # Perform CRS transformation directly in ogr2ogr
    load_cmd = [
        "ogr2ogr",
        "-f", "PostgreSQL",
        "-overwrite",
        "-nln", f"{schema}.{table_name}",  # Table name in PostGIS
        "-lco", "GEOMETRY_NAME=geom",  # Name of the geometry column
        "-lco", f"SCHEMA={schema}",  # Specify the schema explicitly
        "-nlt", "PROMOTE_TO_MULTI",  # Ensure all geometries are multipart
        "-t_srs", "EPSG:2154",  # Perform CRS transformation here
        pg_connection,
        shapefile
    ]

    logging.info(f"Loading {shapefile} into PostGIS...")
    try:
        run_command(load_cmd)
        logging.info(f"Successfully loaded {shapefile} into PostGIS as table {table_name}.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to load {shapefile} into PostGIS: {e.stderr}")
        logging.error(f"Connection details: host={host}, port={port}, dbname={dbname}, user={user}, password=<hidden>")
        raise

    # Step 1: Create spatial index on the geometry column
    create_index_cmd = [
        "psql",  # Command
        "-h", host,  # Host (no extra space)
        "-p", str(port),  # Port
        "-d", dbname,  # Database name
        "-U", user,  # Username
        "-c",  # SQL command
        f'CREATE INDEX idx_{table_name}_geom ON "{schema}"."{table_name}" USING GIST (geom);'
    ]
    logging.info(f"Creating spatial index on table {table_name}...")
    try:
        run_command(create_index_cmd)
        logging.info(f"Spatial index created successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to create spatial index: {e.stderr}")
        raise

    # Step 2: Validate CRS is set to EPSG:2154
    validate_crs_cmd = [
        "psql",  # Command
        "-h", host,  # Host (no extra space)
        "-p", str(port),  # Port
        "-d", dbname,  # Database name
        "-U", user,  # Username
        "-c",  # SQL command
        f"SELECT Find_SRID('{schema}', '{table_name}', 'geom');"
    ]
    output = run_command(validate_crs_cmd)
    if "2154" not in output:
        logging.error(f"CRS is not set to EPSG:2154 for table {table_name}.")
        raise ValueError(f"CRS is not set to EPSG:2154 for table {table_name}.")
    logging.info(f"CRS validated as EPSG:2154 for table {table_name}.")

    # Step 3: Check field lengths and avoid reserved words
    check_fields_cmd = [
        "psql",  # Command
        "-h", host,  # Host (no extra space)
        "-p", str(port),  # Port
        "-d", dbname,  # Database name
        "-U", user,  # Username
        "-c",  # SQL command
        rf'\d "{schema}"."{table_name}";'
    ]
    output = run_command(check_fields_cmd)
    for line in output.splitlines():
        if "reserved" in line.lower() or "invalid" in line.lower():
            logging.error(f"Reserved word or invalid field name detected in table {table_name}.")
            raise ValueError(f"Reserved word or invalid field name detected in table {table_name}.")
    logging.info(f"Field names validated successfully for table {table_name}.")

def process_dataset(input_shp, tps_transformed_shp, adjusted_shp, csv_file, postgis_config):
    """
    Process a single dataset by running TPS transformation, translation adjustment,
    and loading into PostGIS with all validations and enhancements.
    """
    # Validate input files
    validate_file(input_shp)
    validate_file(csv_file)

    # Read GCPs from CSV
    gcps = read_gcps_from_csv(csv_file)

    # Step 1: Run the TPS transformation
    warp_cmd = [
        "ogr2ogr",
        *gcps,  # Insert GCPs here
        "-tps",
        "-t_srs", "EPSG:2154",
        tps_transformed_shp,
        input_shp
    ]
    logging.info("Running TPS transformation...")
    run_command(warp_cmd)
    logging.info("TPS transformation complete.")

    # Get the layer name from the TPS-transformed shapefile
    layer_name = get_layer_name(tps_transformed_shp)

    # Step 2: Apply the translate adjustment
    adjust_cmd = [
        "ogr2ogr",
        adjusted_shp,  # Output adjusted shapefile
        tps_transformed_shp,  # Input: TPS-transformed shapefile
        "-dialect", "Sqlite",
        "-sql", f"SELECT ST_Translate(Geometry, 0.733, -5.179, 0 ) AS geometry, * FROM {layer_name}"
    ]
    logging.info("Running translation adjustment...")
    run_command(adjust_cmd)
    logging.info("Translation adjustment complete.")

    # Step 3: Validate geometries in the adjusted shapefile
    validate_geometries(adjusted_shp)

    # Step 4: Load the adjusted shapefile into PostGIS with all checks
    load_to_postgis(adjusted_shp, layer_name, postgis_config)

def main():
    # PostGIS configuration
    postgis_config = {
        "host": "localhost",
        "port": "5432",
        "dbname": "roofs",  # Ensure this matches the database name
        "user": "mahdi",  # Ensure this matches the username
        "password": "mahdi",  # Ensure this matches the password
        "schema": "public"  # Default schema
    }

    # List of datasets to process
    datasets = [
        {"input_shp": "roof.shp", "tps_transformed_shp": "roof1_warped5_tps.shp", "adjusted_shp": "roof1_warped_adjusted_tps.shp", "csv_file": "gcps.csv"},
        # Add more datasets as needed
    ]

    # Use ThreadPoolExecutor to run datasets in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_dataset, ds["input_shp"], ds["tps_transformed_shp"], ds["adjusted_shp"], ds["csv_file"], postgis_config) for ds in datasets]
        
        for future in futures:
            try:
                future.result()  # This will raise an exception if the task failed
            except Exception as e:
                logging.error(f"Dataset processing failed: {e}")

if __name__ == "__main__":
    main()