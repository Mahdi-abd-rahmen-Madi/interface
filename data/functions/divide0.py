import geopandas as gpd
from shapely.geometry import GeometryCollection
from pathlib import Path
import multiprocessing as mp
from functools import partial
import pandas as pd
import time  # Import the time module for timing

def validate_and_reproject(gdf, target_crs=2154):
    """
    Validates the CRS of a GeoDataFrame and reprojects it if necessary.
    Parameters:
        gdf (GeoDataFrame): The GeoDataFrame to validate and reproject.
        target_crs (int): The target EPSG code (default is 2154).
    Returns:
        GeoDataFrame: The GeoDataFrame with the correct CRS.
    """
    start_time = time.time()  # Start timing
    current_crs = gdf.crs.to_epsg() if gdf.crs and gdf.crs.is_projected else None
    if current_crs != target_crs:
        print(f"Reprojecting from {current_crs or 'unknown'} to EPSG:{target_crs}...")
        gdf = gdf.to_crs(epsg=target_crs)
    end_time = time.time()  # End timing
    print(f"CRS validation and reprojection completed in {end_time - start_time:.2f} seconds.")
    return gdf


def process_intersection(parcelle_chunk, communes):
    """
    Processes the intersection for a single chunk of parcels.
    Parameters:
        parcelle_chunk (GeoDataFrame): A subset of the PARCELLE.shp data.
        communes (GeoDataFrame): The COMMUNES.shp data.
    Returns:
        GeoDataFrame: The result of the intersection.
    """
    start_time = time.time()  # Start timing
    # Perform intersection
    result = gpd.overlay(parcelle_chunk, communes, how='intersection')
    # Drop any invalid geometries
    result = result[~result.geometry.is_empty]
    result.geometry = result.geometry.apply(lambda geom: geom if not isinstance(geom, GeometryCollection) else None)
    result = result[result.geometry.notnull()]
    end_time = time.time()  # End timing
    print(f"Intersection for chunk completed in {end_time - start_time:.2f} seconds.")
    return result


def divide_parcelles_by_communes(parcelle_path, communes_path, output_path, num_processes=None):
    """
    Divides the PARCELLE.shp file using the communes-20220101.shp boundaries.
    Parameters:
        parcelle_path (str): Path to the PARCELLE.shp file.
        communes_path (str): Path to the communes-20220101.shp file.
        output_path (str): Path to save the resulting divided shapefile.
        num_processes (int): Number of CPU cores to use for parallel processing. Defaults to all available cores.
    Returns:
        None
    """
    try:
        start_time = time.time()  # Start overall timing

        # Step 1: Load the shapefiles
        load_start = time.time()
        print("Loading shapefiles...")
        parcelles = gpd.read_file(parcelle_path)
        communes = gpd.read_file(communes_path)
        load_end = time.time()
        print(f"Shapefile loading completed in {load_end - load_start:.2f} seconds.")

        # Step 2: Validate and reproject CRS
        reprojection_start = time.time()
        print("Validating and reprojecting CRS if necessary...")
        parcelles = validate_and_reproject(parcelles)
        communes = validate_and_reproject(communes)
        reprojection_end = time.time()
        print(f"CRS validation and reprojection completed in {reprojection_end - reprojection_start:.2f} seconds.")

        # Step 3: Prepare for parallel processing
        prep_start = time.time()
        print("Preparing for parallel processing...")
        if num_processes is None:
            num_processes = mp.cpu_count()
        # Split the PARCELLE.shp into chunks
        chunk_size = len(parcelles) // num_processes + 1
        parcelle_chunks = [parcelles[i:i + chunk_size] for i in range(0, len(parcelles), chunk_size)]
        # Define a partial function for parallel processing
        func = partial(process_intersection, communes=communes)
        prep_end = time.time()
        print(f"Parallel processing preparation completed in {prep_end - prep_start:.2f} seconds.")

        # Step 4: Perform the intersection in parallel
        intersection_start = time.time()
        print(f"Performing intersection using {num_processes} cores...")
        with mp.Pool(num_processes) as pool:
            results = pool.map(func, parcelle_chunks)
        intersection_end = time.time()
        print(f"Intersection completed in {intersection_end - intersection_start:.2f} seconds.")

        # Combine the results
        combine_start = time.time()
        result = gpd.GeoDataFrame(pd.concat(results, ignore_index=True), crs=parcelles.crs)
        combine_end = time.time()
        print(f"Combining results completed in {combine_end - combine_start:.2f} seconds.")

        # Step 5: Save the result to a new shapefile
        save_start = time.time()
        output_dir = Path(output_path).parent
        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)
        print(f"Saving result to {output_path}...")
        result.to_file(output_path, driver="ESRI Shapefile")
        save_end = time.time()
        print(f"File saving completed in {save_end - save_start:.2f} seconds.")

        end_time = time.time()  # End overall timing
        print(f"Total execution time: {end_time - start_time:.2f} seconds.")
        print("Division completed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    # Define file paths
    parcelle_path = "/home/mahdi/interface/data/shapefiles/pq2/PARCELLE.SHP"
    communes_path = "/home/mahdi/interface/data/shapefiles/pq2/communes-20220101.shp"
    output_path = "/home/mahdi/interface/data/shapefiles/pq2/divided_parcelles_basic_8.shp"

    # Run the division process
    divide_parcelles_by_communes(parcelle_path, communes_path, output_path, num_processes=4)