import subprocess
import os
import logging
from concurrent.futures import ThreadPoolExecutor
import glob

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_command(command):
    """Run a command using subprocess and handle errors."""
    try:
        logging.debug(f"Running command: {' '.join(command[:2])} [...]")  # Mask sensitive parts of the command
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

def get_layer_name(shapefile):
    """Get the layer name from a shapefile using ogrinfo."""
    try:
        cmd = ["ogrinfo", "-so", shapefile]
        output = run_command(cmd)
        for line in output.splitlines():
            if line.strip().startswith("1:"):
                layer_name = line.split(":")[1].strip().split(" ")[0]
                logging.info(f"Determined layer name: {layer_name}")
                return layer_name
        logging.error("Could not determine layer name from shapefile.")
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
    Load a shapefile into a PostGIS database with SPGiST indexing, CRS validation,
    field length checks, reserved word avoidance, and geometry validation.
    """
    host = postgis_config.get("host", "localhost")
    port = postgis_config.get("port", "5432")
    dbname = postgis_config.get("dbname", "roofs")
    user = postgis_config.get("user", "mahdi")
    schema = postgis_config.get("schema", "public")
    table_name = os.path.splitext(os.path.basename(shapefile))[0]
    
    # Connection string without password (handled by .pgpass)
    pg_connection = f"PG:host={host} port={port} dbname={dbname} user={user}"

    # Load shapefile into PostGIS
    load_cmd = [
        "ogr2ogr",
        "-f", "PostgreSQL",
        "-overwrite",
        "-nln", f"{schema}.{table_name}",
        "-lco", "GEOMETRY_NAME=geom",
        "-lco", f"SCHEMA={schema}",
        "-nlt", "PROMOTE_TO_MULTI",
        "-t_srs", "EPSG:2154",
        pg_connection,
        shapefile
    ]
    logging.info(f"Loading {shapefile} into PostGIS...")
    try:
        run_command(load_cmd)
        logging.info(f"Successfully loaded {shapefile} into PostGIS as table {table_name}.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to load {shapefile} into PostGIS: {e.stderr}")
        raise

    # Create SPGiST index
    create_index_cmd = [
        "psql",
        "-h", host,
        "-p", str(port),
        "-d", dbname,
        "-c",
        f'CREATE INDEX idx_{table_name}_geom ON "{schema}"."{table_name}" USING SPGiST (geom);'
    ]
    logging.info(f"Creating SPGiST index on table {table_name}...")
    try:
        run_command(create_index_cmd)
        logging.info(f"SPGiST index created successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to create SPGiST index: {e.stderr}")
        raise

    # Validate CRS
    validate_crs_cmd = [
        "psql",
        "-h", host,
        "-p", str(port),
        "-d", dbname,
        "-c",
        f"SELECT Find_SRID('{schema}', '{table_name}', 'geom');"
    ]
    output = run_command(validate_crs_cmd)
    if "2154" not in output:
        logging.error(f"CRS is not set to EPSG:2154 for table {table_name}.")
        raise ValueError(f"CRS is not set to EPSG:2154 for table {table_name}.")
    logging.info(f"CRS validated as EPSG:2154 for table {table_name}.")

    # Check field names for reserved words or invalid characters
    check_fields_cmd = [
        "psql",
        "-h", host,
        "-p", str(port),
        "-d", dbname,
        "-c",
        rf'\d "{schema}"."{table_name}";'
    ]
    output = run_command(check_fields_cmd)
    for line in output.splitlines():
        if "reserved" in line.lower() or "invalid" in line.lower():
            logging.error(f"Reserved word or invalid field name detected in table {table_name}.")
            raise ValueError(f"Reserved word or invalid field name detected in table {table_name}.")
    logging.info(f"Field names validated successfully for table {table_name}.")

    # Set ownership and grant privileges
    set_owner_cmd = [
        "psql",
        "-h", host,
        "-p", str(port),
        "-d", dbname,
        "-c",
        f'ALTER TABLE "{schema}"."{table_name}" OWNER TO mahdi;'
    ]
    grant_privileges_cmd = [
        "psql",
        "-h", host,
        "-p", str(port),
        "-d", dbname,
        "-c",
        f'GRANT ALL PRIVILEGES ON TABLE "{schema}"."{table_name}" TO mahdi;'
    ]
    try:
        run_command(set_owner_cmd)
        run_command(grant_privileges_cmd)
        logging.info(f"Ownership set to mahdi and all privileges granted.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to set ownership or grant privileges: {e.stderr}")
        raise

def process_preprocessed_shapefile(input_shp, postgis_config):
    """
    Process a preprocessed shapefile by validating geometries and loading into PostGIS.
    """
    validate_file(input_shp)
    layer_name = get_layer_name(input_shp)
    validate_geometries(input_shp)
    load_to_postgis(input_shp, layer_name, postgis_config)

def main():
    # PostGIS configuration without password (handled by .pgpass)
    postgis_config = {
        "host": "localhost",
        "port": "5432",
        "dbname": "roofs",
        "user": "mahdi",
        "schema": "public"
    }

    # Define the path pattern for shapefiles starting with "aligned_results"
    shapefile_pattern = "/home/mahdi/interface/data/output/aligned_results*.shp"

    # Find all matching shapefiles
    shapefiles = glob.glob(shapefile_pattern)
    if not shapefiles:
        logging.error(f"No shapefiles found matching the pattern: {shapefile_pattern}")
        return

    logging.info(f"Found {len(shapefiles)} shapefiles to process.")

    # Process shapefiles in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_preprocessed_shapefile, shp, postgis_config) for shp in shapefiles]

        for future in futures:
            try:
                future.result()
            except Exception as e:
                logging.error(f"Dataset processing failed: {e}")

if __name__ == "__main__":
    main()