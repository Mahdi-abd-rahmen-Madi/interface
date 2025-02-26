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

def process_dataset(input_shp, tps_transformed_shp, adjusted_shp, csv_file):
    """Process a single dataset by running TPS transformation and translation adjustment."""
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

def main():
    # List of datasets to process
    datasets = [
        {"input_shp": "roof.shp", "tps_transformed_shp": "roof1_warped5_tps.shp", "adjusted_shp": "roof1_warped_adjusted_tps.shp", "csv_file": "gcps.csv"},
        #{"input_shp": "roof2.shp", "tps_transformed_shp": "roof2_warped5_tps.shp", "adjusted_shp": "roof2_warped_adjusted_tps.shp", "csv_file": "gcps.csv"},
        # Add more datasets as needed
    ]

    # Use ThreadPoolExecutor to run datasets in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_dataset, ds["input_shp"], ds["tps_transformed_shp"], ds["adjusted_shp"], ds["csv_file"]) for ds in datasets]
        
        for future in futures:
            try:
                future.result()  # This will raise an exception if the task failed
            except Exception as e:
                logging.error(f"Dataset processing failed: {e}")

if __name__ == "__main__":
    main()