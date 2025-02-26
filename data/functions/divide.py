import geopandas as gpd
from shapely.geometry import GeometryCollection
from pathlib import Path
import multiprocessing as mp
from functools import partial
import pandas as pd
from tqdm import tqdm
import time
import tempfile

def validate_and_reproject(gdf, target_crs=2154):
    """
    Validates the CRS of a GeoDataFrame and reprojects it if necessary.
    """
    current_crs = gdf.crs.to_epsg() if gdf.crs and gdf.crs.is_projected else None
    if current_crs != target_crs:
        print(f"Reprojecting from {current_crs or 'unknown'} to EPSG:{target_crs}...")
        gdf = gdf.to_crs(epsg=target_crs)
    return gdf


def drop_unnecessary_attributes(parcelles_gdf, communes_gdf):
    """
    Drops unnecessary attributes from the GeoDataFrames with case-insensitive matching.
    """
    print("Dropping unnecessary attributes...")
    
    # Case-insensitive column dropping
    for col in parcelles_gdf.columns:
        if col.lower() == "numero" or col.lower() == "feuille":
            parcelles_gdf = parcelles_gdf.drop(columns=[col])
            
    for col in communes_gdf.columns:
        if col.lower() == "wikipedia":
            communes_gdf = communes_gdf.drop(columns=[col])
            
    return parcelles_gdf, communes_gdf


def save_chunk_to_file(chunk, temp_dir, index):
    """
    Saves a GeoDataFrame chunk to a temporary file.
    """
    temp_path = f"{temp_dir}/chunk_{index}.gpkg"
    chunk.to_file(temp_path, driver="GPKG")
    return temp_path


def process_intersection_from_file(temp_path, communes_gdf):
    """
    Processes the intersection for a single chunk loaded from a file.
    """
    parcelle_chunk = gpd.read_file(temp_path)
    result = gpd.overlay(parcelle_chunk, communes_gdf, how='intersection')
    result = result[~result.geometry.is_empty]
    result.geometry = result.geometry.apply(lambda geom: geom if not isinstance(geom, GeometryCollection) else None)
    result = result[result.geometry.notnull()]
    return result


def divide_parcelles_by_communes(parcelle_path, communes_path, output_path, num_processes=None):
    """
    Divides the PARCELLE.shp file using the communes-20220101.shp boundaries.
    """
    try:
        start_time = time.time()

        # Step 1: Load the shapefiles
        print("Loading shapefiles...")
        load_start = time.time()
        parcelles = gpd.read_file(parcelle_path)
        communes = gpd.read_file(communes_path)
        load_end = time.time()
        print(f"Shapefile loading completed in {load_end - load_start:.2f} seconds.")

        # Step 2: Validate and reproject CRS
        print("Validating and reprojecting CRS if necessary...")
        reprojection_start = time.time()
        parcelles = validate_and_reproject(parcelles)
        communes = validate_and_reproject(communes)
        reprojection_end = time.time()
        print(f"CRS reprojection completed in {reprojection_end - reprojection_start:.2f} seconds.")

        # Step 3: Drop unnecessary attributes
        attribute_drop_start = time.time()
        parcelles, communes = drop_unnecessary_attributes(parcelles, communes)
        attribute_drop_end = time.time()
        print(f"Attribute dropping completed in {attribute_drop_end - attribute_drop_start:.2f} seconds.")

        # Step 4: Create spatial indexes
        print("Creating spatial indexes...")
        parcelles.sindex
        communes.sindex

        # Step 5: Prepare for parallel processing
        print("Preparing for parallel processing...")
        if num_processes is None:
            num_processes = mp.cpu_count()

        # Split the PARCELLE.shp into chunks
        chunk_size = max(len(parcelles) // (num_processes * 2), 1)  # Reduce chunk size
        parcelle_chunks = [parcelles[i:i + chunk_size] for i in range(0, len(parcelles), chunk_size)]

        # Save chunks to temporary files
        with tempfile.TemporaryDirectory() as temp_dir:
            print("Saving chunks to temporary files...")
            temp_files = [save_chunk_to_file(chunk, temp_dir, i) for i, chunk in enumerate(parcelle_chunks)]

            # Define a partial function for parallel processing
            func = partial(process_intersection_from_file, communes_gdf=communes)

            # Step 6: Perform the intersection in parallel
            print(f"Performing intersection using {num_processes} cores...")
            intersection_start = time.time()
            with mp.Pool(num_processes) as pool:
                results = list(tqdm(pool.imap(func, temp_files), total=len(temp_files), desc="Processing chunks"))
            intersection_end = time.time()
            print(f"Intersection completed in {intersection_end - intersection_start:.2f} seconds.")

            # Step 7: Combine the results
            print("Combining results...")
            combine_start = time.time()
            result = pd.concat(results, ignore_index=True)
            combine_end = time.time()
            print(f"Result combination completed in {combine_end - combine_start:.2f} seconds.")

        # Step 8: Save the result to a new shapefile
        output_dir = Path(output_path).parent
        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)

        print(f"Saving result to {output_path}...")
        save_start = time.time()
        result.to_file(output_path, driver="ESRI Shapefile")  # Output as Shapefile
        save_end = time.time()
        print(f"File saving completed in {save_end - save_start:.2f} seconds.")

        end_time = time.time()
        print(f"Total execution time: {end_time - start_time:.2f} seconds.")
        print("Division completed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    parcelle_path = "/home/mahdi/interface/data/raw/pq2/PARCELLE.SHP"
    communes_path = "/home/mahdi/interface/data/raw/pq2/communes-20220101.shp"
    output_path = "/home/mahdi/interface/data/output/divide/divide.shp"  # Output as Shapefile

    # Run the division process with multiprocessing
    divide_parcelles_by_communes(
        parcelle_path,
        communes_path,
        output_path,
        num_processes=4  # Reduce parallelism to avoid memory issues
    )