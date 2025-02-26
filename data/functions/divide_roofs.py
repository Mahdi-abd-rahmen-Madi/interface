import geopandas as gpd
from shapely.geometry import GeometryCollection
from pathlib import Path
import multiprocessing as mp
from functools import partial
import pandas as pd
from tqdm import tqdm
import time
import tempfile
import fiona

def validate_and_reproject(gdf, target_crs=2154):
    """
    Validates the CRS of a GeoDataFrame and reprojects it if necessary.
    """
    current_crs = gdf.crs.to_epsg() if gdf.crs and gdf.crs.is_projected else None
    if current_crs != target_crs:
        print(f"Reprojecting from {current_crs or 'unknown'} to EPSG:{target_crs}...")
        gdf = gdf.to_crs(epsg=target_crs)  # Reproject in memory
    return gdf

def drop_unnecessary_attributes(toits_gdf, communes_gdf):
    """
    Drops unnecessary attributes from the GeoDataFrames with case-insensitive matching.
    """
    print("Dropping unnecessary attributes...")

    # Print column names for debugging
    print(f"Toits columns: {toits_gdf.columns.tolist()}")
    print(f"Communes columns: {communes_gdf.columns.tolist()}")

    # Rename 'fid' column to avoid conflicts with GeoPackage
    if 'fid' in toits_gdf.columns:
        print("Renaming 'fid' column to 'original_fid' to avoid conflicts")
        toits_gdf = toits_gdf.rename(columns={'fid': 'ofid'})

    # Case-insensitive column dropping for communes
    for col in communes_gdf.columns:
        if col.lower() in ["wikipedia", "surf_ha"]:  # Adjust based on actual commune attributes
            communes_gdf = communes_gdf.drop(columns=[col])

    return toits_gdf, communes_gdf

def save_chunk_to_file(chunk, temp_dir, index):
    """
    Saves a GeoDataFrame chunk to a temporary file, using GeoPackage if available, otherwise Shapefile.
    """
    # Check if GeoPackage driver is supported
    if "GPKG" in fiona.supported_drivers:
        temp_path = f"{temp_dir}/chunk_{index}.gpkg"
        driver = "GPKG"
    else:
        temp_path = f"{temp_dir}/chunk_{index}.shp"
        driver = "ESRI Shapefile"

    try:
        chunk.to_file(temp_path, driver=driver)
    except Exception as e:
        print(f"Error saving chunk {index} to {driver}: {e}")
        raise

    return temp_path

def process_intersection_from_file(temp_path, communes_gdf):
    """
    Processes the intersection for a single chunk loaded from a file.
    """
    try:
        toits_chunk = gpd.read_file(temp_path)
        result = gpd.overlay(toits_chunk, communes_gdf, how='intersection')
        result = result[~result.geometry.is_empty]
        result.geometry = result.geometry.apply(lambda geom: geom if not isinstance(geom, GeometryCollection) else None)
        result = result[result.geometry.notnull()]
        return result
    except Exception as e:
        print(f"Error processing chunk {temp_path}: {e}")
        return gpd.GeoDataFrame(geometry=[])  # Return empty GeoDataFrame on error

def divide_toits_by_communes(toits_path, communes_path, output_path, num_processes=None):
    """
    Divides the TOITS_PQ2_filtered.shp file using the COMMUNES shapefile boundaries.
    """
    try:
        start_time = time.time()
        # Step 1: Load the shapefiles
        print("Loading shapefiles...")
        load_start = time.time()
        toits = gpd.read_file(toits_path)
        communes = gpd.read_file(communes_path)
        load_end = time.time()
        print(f"Shapefile loading completed in {load_end - load_start:.2f} seconds.")
        print(f"Toits shapefile has {len(toits)} features.")
        print(f"Communes shapefile has {len(communes)} features.")

        # Step 2: Validate and reproject CRS
        print("Validating and reprojecting CRS if necessary...")
        reprojection_start = time.time()
        toits = validate_and_reproject(toits)
        communes = validate_and_reproject(communes)
        reprojection_end = time.time()
        print(f"CRS reprojection completed in {reprojection_end - reprojection_start:.2f} seconds.")

        # Step 3: Drop unnecessary attributes
        attribute_drop_start = time.time()
        toits, communes = drop_unnecessary_attributes(toits, communes)
        attribute_drop_end = time.time()
        print(f"Attribute dropping completed in {attribute_drop_end - attribute_drop_start:.2f} seconds.")

        # Step 4: Create spatial indexes
        print("Creating spatial indexes...")
        index_start = time.time()
        toits.sindex
        communes.sindex
        index_end = time.time()
        print(f"Spatial index creation completed in {index_end - index_start:.2f} seconds.")

        # Step 5: Prepare for parallel processing
        print("Preparing for parallel processing...")
        if num_processes is None:
            num_processes = mp.cpu_count()

        print(f"Using {num_processes} processes for parallel computation.")
        # Split the TOITS_PQ2_filtered.shp into chunks - using smaller chunks to reduce memory usage
        chunk_size = max(len(toits) // (num_processes * 4), 1)  # Smaller chunks for better memory management
        toits_chunks = [toits[i:i + chunk_size] for i in range(0, len(toits), chunk_size)]
        print(f"Split data into {len(toits_chunks)} chunks, each with approximately {chunk_size} features.")

        # Save chunks to temporary files
        with tempfile.TemporaryDirectory() as temp_dir:
            print("Saving chunks to temporary files...")
            save_start = time.time()
            temp_files = []
            for i, chunk in enumerate(toits_chunks):
                try:
                    temp_file = save_chunk_to_file(chunk, temp_dir, i)
                    temp_files.append(temp_file)
                    if (i + 1) % 10 == 0:
                        print(f"Saved {i + 1}/{len(toits_chunks)} chunks...")
                except Exception as e:
                    print(f"Error saving chunk {i}: {e}")

            save_end = time.time()
            print(f"Chunk saving completed in {save_end - save_start:.2f} seconds.")
            print(f"Successfully saved {len(temp_files)} chunk files.")

            # Define a partial function for parallel processing
            func = partial(process_intersection_from_file, communes_gdf=communes)

            # Step 6: Perform the intersection in parallel
            print(f"Performing intersection using {num_processes} cores...")
            intersection_start = time.time()
            with mp.Pool(num_processes) as pool:
                results = list(tqdm(pool.imap(func, temp_files), total=len(temp_files), desc="Processing chunks"))
            intersection_end = time.time()
            print(f"Intersection completed in {intersection_end - intersection_start:.2f} seconds.")

            # Check if any result is empty
            empty_results = sum(1 for r in results if len(r) == 0)
            if empty_results > 0:
                print(f"Warning: {empty_results} out of {len(results)} result chunks are empty.")

            # Step 7: Combine the results
            print("Combining results...")
            combine_start = time.time()
            if all(len(r) == 0 for r in results):
                print("Error: All result chunks are empty. No intersections found.")
                return

            non_empty_results = [r for r in results if len(r) > 0]
            if len(non_empty_results) == 0:
                print("Error: No non-empty results found.")
                return

            print(f"Concatenating {len(non_empty_results)} non-empty chunks...")
            result = pd.concat(non_empty_results, ignore_index=True)
            combine_end = time.time()
            print(f"Result combination completed in {combine_end - combine_start:.2f} seconds.")
            print(f"Final result has {len(result)} features.")

        # Step 8: Save the result to a new shapefile
        output_dir = Path(output_path).parent
        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)

        print(f"Saving result to {output_path}...")
        save_start = time.time()
        # Ensure result has proper column types
        for col in result.columns:
            if col != 'geometry' and result[col].dtype == 'object':
                try:
                    # Try to convert object columns to numeric where possible
                    result[col] = pd.to_numeric(result[col])
                except:
                    pass

        # Remove any problematic columns
        if 'fid' in result.columns:
            result = result.drop(columns=['fid'])

        result.to_file(output_path, driver="ESRI Shapefile")
        save_end = time.time()
        print(f"File saving completed in {save_end - save_start:.2f} seconds.")
        end_time = time.time()
        print(f"Total execution time: {end_time - start_time:.2f} seconds.")
        print("Division completed successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    toits_path = "/home/mahdi/interface/data/raw/pq2/TOITS_PQ2_filtered.shp"
    communes_path = "/home/mahdi/interface/data/raw/pq2/communes-20220101.shp"
    output_path = "/home/mahdi/interface/data/output/divide/roofs_divided_by_communes_new.shp"
    # Run the division process with multiprocessing
    divide_toits_by_communes(
        toits_path,
        communes_path,
        output_path,
        num_processes=4  # Adjust based on your system capabilities
    )