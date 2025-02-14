import subprocess
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_command(command):
    """Run a command using subprocess and handle errors."""
    try:
        logging.info(f"Running command: {' '.join(command)}")
        result = subprocess.run(command, check=True, text=True, capture_output=True)
        logging.info(result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed with return code {e.returncode}")
        logging.error(e.stderr)
        raise

def validate_file(file_path):
    """Check if the file exists."""
    if not os.path.isfile(file_path):
        logging.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")

def main():
    # Configuration
    input_shp = "roof.shp"
    tps_transformed_shp = "roof_warped5_tps.shp"
    adjusted_shp = "roof_warped_adjusted_tps.shp"

    # Validate input file
    validate_file(input_shp)

    # Step 1: Run the TPS transformation
    warp_cmd = [
        "ogr2ogr",
        "-gcp", "900159.193", "6271206.336", "900159.705", "6271215.24", 
        "-gcp", "900215.699", "6271199.372", "900216.939", "6271207.755",
        "-gcp", "900943.123", "6271289.268", "900942.807", "6271292.411",
        "-gcp", "900673.195", "6271805.313", "900673.177", "6271807.047",
        "-gcp", "900457.698", "6271776.301", "900457.606", "6271776.292",
        "-gcp", "900393.661", "6271805.32",  "900393.736", "6271805.318",
        "-gcp", "900304.688", "6271783.813", "900304.677", "6271783.831",
        "-gcp", "900991.148", "6271715.381", "900991.621", "6271712.406",
        "-gcp", "900990.244", "6271698.861", "900990.552", "6271695.048",
        "-gcp", "900906.71",  "6271502.431", "900906.902", "6271501.143",
        "-gcp", "900956.675", "6271503.865", "900956.557", "6271502.59",
        "-gcp", "900862.792", "6271045.237", "900861.398", "6271046.981",
        "-gcp", "900717.106", "6271026.91",  "900718.721", "6271033.458",
        "-gcp", "900696.69",  "6271017.824", "900696.802", "6271022.766",
        "-gcp", "900693.153", "6271055.367", "900693.406", "6271060.755",
        "-gcp", "900710.265", "6271055.741", "900709.821", "6271058.994",
        "-gcp", "900741.683", "6271090.807", "900741.52",  "6271094.969",
        "-gcp", "900755.647", "6271098.319", "900756.206", "6271103.492",
        "-gcp", "900570.283", "6271099.753", "900569.378", "6271105.598",
        "-gcp", "900581.628", "6271098.849", "900580.699", "6271103.963",
        "-gcp", "900515.175", "6271074.84",  "900516.389", "6271080.897",
        "-gcp", "900514.24",  "6271079.337", "900515.084", "6271084.482",
        "-gcp", "900516.687", "6271080.35",  "900518.559", "6271086.777",
        "-gcp", "900488.167", "6271055.726", "900488.495", "6271062.814",
        "-gcp", "900475.73",  "6271056.318", "900476.356", "6271063.946",
        "-gcp", "900051.186", "6271849.317", "900051.911", "6271848.982",
        "-gcp", "900048.183", "6271847.312", "900048.672", "6271847.205",
        "-gcp", "900036.184", "6271900.777", "900036.722", "6271900.572",
        "-gcp", "900016.192", "6271905.8",   "900016.455", "6271905.43",
        "-gcp", "900406.724", "6271421.319", "900408.399", "6271425.371",
        "-gcp", "900421.785", "6271393.895", "900423.808", "6271396.848",
        "-gcp", "900794.185", "6271950.8",   "900794.902", "6271951.501",
        "-gcp", "900754.143", "6271928.857", "900754.775", "6271928.67",
        "-gcp", "900748.723", "6271928.844", "900748.8",   "6271928.921",
        "-gcp", "900335.235", "6271961.253", "900335.519", "6271960.243",
        "-gcp", "900336.231", "6271955.833", "900336.494", "6271955.148",
        "-gcp", "900339.197", "6271940.312", "900340.284", "6271941.61",
        "-gcp", "900370.182", "6271962.835", "900371.527", "6271962.444",
        "-gcp", "900016.201", "6271002.826", "900016.895", "6271008.961",
        "-gcp", "900013.179", "6270999.804", "900013.027", "6271005.965",
        "-gcp", "900003.215", "6270998.337", "900001.894", "6271003.489",
        "-gcp", "900076.723", "6271004.863", "900076.142", "6271011.319",
        "-gcp", "900084.135", "6271004.877", "900084.208", "6271010.517",
        "-gcp", "900093.248", "6270999.328", "900092.463", "6271005.564",
        "-gcp", "900000.722", "6271275.405", "900000.243", "6271279.793",
        "-gcp", "900057.204", "6271701.829", "900057.674", "6271703.578",
        "-tps",
        "-t_srs", "EPSG:2154",
        tps_transformed_shp,
        input_shp
    ]
    logging.info("Running TPS transformation...")
    run_command(warp_cmd)
    logging.info("TPS transformation complete.")

    # Step 2: Apply the translate adjustment
    adjust_cmd = [
        "ogr2ogr",
        adjusted_shp,  # Output adjusted shapefile
        tps_transformed_shp,  # Input: TPS-transformed shapefile
        "-dialect", "Sqlite",
        "-sql", "SELECT ST_Translate(Geometry, 0.733, -5.179, 0 ) AS geometry, * FROM roof_warped5_tps"
    ]
    logging.info("Running translation adjustment...")
    run_command(adjust_cmd)
    logging.info("Translation adjustment complete.")

if __name__ == "__main__":
    main()