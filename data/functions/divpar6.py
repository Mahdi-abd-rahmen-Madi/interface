# Standard library imports
import multiprocessing as mp
from functools import partial
import pandas as pd
from tqdm import tqdm
import time
import tempfile
from pathlib import Path
import os
import psutil  # For memory monitoring
import signal
import shutil  # For copying failed chunk files

# Geospatial library imports
import geopandas as gpd
from shapely.geometry import GeometryCollection

# Fiona for driver checks
import fiona
import warnings

# Suppress UserWarnings related to keep_geom_type
warnings.filterwarnings("ignore", category=UserWarning, message="`keep_geom_type=True` in overlay resulted in.*")

# Custom timeout exception
class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException("Processing timed out")

# Memory monitoring function
def get_memory_usage():
    """Returns current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

# Validate and reproject CRS
def validate_and_reproject(gdf, target_crs=2154):
    """
    Validates the CRS of a GeoDataFrame and reprojects it if necessary.
    """
    current_crs = gdf.crs.to_epsg() if gdf.crs and gdf.crs.is_projected else None
    if current_crs != target_crs:
        print(f"Reprojecting from {current_crs or 'unknown'} to EPSG:{target_crs}...")
        gdf = gdf.to_crs(epsg=target_crs)
    return gdf

# Drop unnecessary attributes
def drop_unnecessary_attributes(divided_roofs_gdf, parcelles_gdf):
    """
    Drops unnecessary attributes from the GeoDataFrames with case-insensitive matching.
    """
    print("Dropping unnecessary attributes...")

    # Print column names for debugging
    print(f"Divided roofs columns: {divided_roofs_gdf.columns.tolist()}")
    print(f"Parcelles columns: {parcelles_gdf.columns.tolist()}")

    # Rename 'fid' column to avoid conflicts with GeoPackage
    if 'fid' in divided_roofs_gdf.columns:
        print("Renaming 'fid' column to 'original_fid' to avoid conflicts")
        divided_roofs_gdf = divided_roofs_gdf.rename(columns={'fid': 'original_fid'})

    # Case-insensitive column dropping for parcelles
    for col in parcelles_gdf.columns:
        if col.lower() in ["nom"]:
            parcelles_gdf = parcelles_gdf.drop(columns=[col])

    return divided_roofs_gdf, parcelles_gdf

# Save chunk to file (with fallback to Shapefile if GeoPackage isn't available)
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

# Process intersection from file with spatial filtering and detailed logging
def process_intersection_from_file(temp_path, parcelles_gdf):
    """
    Processes the intersection for a single chunk loaded from a file with spatial filtering.
    """
    try:
        chunk_id = Path(temp_path).stem
        print(f"[{chunk_id}] Starting processing")

        # Load the chunk
        try:
            divided_roofs_chunk = gpd.read_file(temp_path)
            print(f"[{chunk_id}] Loaded chunk with {len(divided_roofs_chunk)} features")
        except Exception as e:
            print(f"[{chunk_id}] Error loading chunk: {e}")
            return gpd.GeoDataFrame(geometry=[]), False

        # Spatial filtering
        try:
            minx, miny, maxx, maxy = divided_roofs_chunk.total_bounds
            relevant_parcelles = parcelles_gdf.cx[minx:maxx, miny:maxy]
            print(f"[{chunk_id}] Filtered from {len(parcelles_gdf)} to {len(relevant_parcelles)} relevant parcelles")
        except Exception as e:
            print(f"[{chunk_id}] Error during spatial filtering: {e}")
            return gpd.GeoDataFrame(geometry=[]), False
        if len(relevant_parcelles) == 0:
            print(f"[{chunk_id}] No relevant parcels found. Skipping...")
            return gpd.GeoDataFrame(geometry=[]), True  # Not really an error, just no matches

        # Perform overlay with error handling
        try:
            print(f"[{chunk_id}] Performing overlay...")
            result = gpd.overlay(
                divided_roofs_chunk,
                relevant_parcelles,
                how='intersection'
            )
            print(f"[{chunk_id}] Overlay complete, got {len(result)} features")
        except Exception as e:
            print(f"[{chunk_id}] Error during overlay: {e}")
            return gpd.GeoDataFrame(geometry=[]), False

        # Filter by geometry type
        try:
            # Check for non-polygon geometries
            non_polys = result[~result.geometry.type.isin(['Polygon', 'MultiPolygon'])]
            if len(non_polys) > 0:
                print(f"[{chunk_id}] Found {len(non_polys)} non-polygon geometries: {non_polys.geometry.type.value_counts().to_dict()}")
                result = result[result.geometry.type.isin(['Polygon', 'MultiPolygon'])]

            # Check for empty geometries
            empties = result[result.geometry.is_empty]
            if len(empties) > 0:
                print(f"[{chunk_id}] Removing {len(empties)} empty geometries")
                result = result[~result.geometry.is_empty]

            print(f"[{chunk_id}] Final filtered result has {len(result)} features")
        except Exception as e:
            print(f"[{chunk_id}] Error during geometry filtering: {e}")
            return gpd.GeoDataFrame(geometry=[]), False

        return result, True
    except Exception as e:
        print(f"Error processing chunk {temp_path}: {e}")
        return gpd.GeoDataFrame(geometry=[]), False

# Process with timeout
def process_with_timeout(func, temp_path, parcelles_gdf, timeout=300):
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(timeout)
    try:
        return func(temp_path, parcelles_gdf)
    except TimeoutException:
        print(f"Processing timed out for {temp_path}")
        return gpd.GeoDataFrame(geometry=[]), False
    finally:
        signal.alarm(0)

# Main function to divide roofs by parcelles
def divide_roofs_by_parcelles(divided_roofs_path, parcelles_path, output_path, failed_chunks_dir, num_processes=None):
    """
    Divides the pre-divided roofs (by communes) using the PARCELLE.SHP boundaries.
    
    Args:
        divided_roofs_path: Path to the divided roofs shapefile
        parcelles_path: Path to the parcelles shapefile
        output_path: Path to save the output shapefile
        failed_chunks_dir: Directory to save failed chunks for later processing
        num_processes: Number of processes to use for parallel processing
    """
    try:
        start_time = time.time()

        # Create failed chunks directory if it doesn't exist
        Path(failed_chunks_dir).mkdir(parents=True, exist_ok=True)
        print(f"Created directory for failed chunks: {failed_chunks_dir}")

        # Step 1: Load the shapefiles
        print("Loading shapefiles...")
        load_start = time.time()
        divided_roofs = gpd.read_file(divided_roofs_path)
        parcelles = gpd.read_file(parcelles_path)
        load_end = time.time()
        print(f"Shapefile loading completed in {load_end - load_start:.2f} seconds.")
        print(f"Divided roofs shapefile has {len(divided_roofs)} features.")
        print(f"Parcelles shapefile has {len(parcelles)} features.")

        # Step 2: Validate and reproject CRS
        print("Validating and reprojecting CRS if necessary...")
        reprojection_start = time.time()
        divided_roofs = validate_and_reproject(divided_roofs)
        parcelles = validate_and_reproject(parcelles)
        reprojection_end = time.time()
        print(f"CRS reprojection completed in {reprojection_end - reprojection_start:.2f} seconds.")

        # Step 3: Drop unnecessary attributes
        print("Dropping unnecessary attributes...")
        divided_roofs, parcelles = drop_unnecessary_attributes(divided_roofs, parcelles)

        # Step 4: Create spatial indexes
        print("Creating spatial indexes...")
        index_start = time.time()
        divided_roofs.sindex
        parcelles.sindex
        index_end = time.time()
        print(f"Spatial index creation completed in {index_end - index_start:.2f} seconds.")

        # Step 5: Reduce parcelles to necessary attributes
        print("Reducing parcelles to necessary attributes...")
        parcelles = parcelles[['geometry', 'SECTION', 'CODE_DEP', 'CODE_COM']]  # Keep only necessary columns

        # Step 6: Prepare for parallel processing
        print("Preparing for parallel processing...")
        if num_processes is None:
            num_processes = mp.cpu_count()

        print(f"Using {num_processes} processes for parallel computation.")
        # Split the divided_roofs into chunks - smaller chunks for better memory management
        chunk_size = max(len(divided_roofs) // (num_processes * 16), 1)  # Reduced chunk size further
        divided_roofs_chunks = [divided_roofs[i:i + chunk_size] for i in range(0, len(divided_roofs), chunk_size)]
        print(f"Split data into {len(divided_roofs_chunks)} chunks, each with approximately {chunk_size} features.")

        # Save chunks to temporary files
        with tempfile.TemporaryDirectory() as temp_dir:
            print("Saving chunks to temporary files...")
            save_start = time.time()
            temp_files = []
            for i, chunk in enumerate(divided_roofs_chunks):
                try:
                    temp_file = save_chunk_to_file(chunk, temp_dir, i)
                    temp_files.append(temp_file)
                    if (i + 1) % 10 == 0:
                        print(f"Saved {i + 1}/{len(divided_roofs_chunks)} chunks...")
                except Exception as e:
                    print(f"Error saving chunk {i}: {e}")

            save_end = time.time()
            print(f"Chunk saving completed in {save_end - save_start:.2f} seconds.")
            print(f"Successfully saved {len(temp_files)} chunk files.")

            # Define a partial function for parallel processing
            func = partial(process_with_timeout, process_intersection_from_file, parcelles_gdf=parcelles)

            # Before starting intersection
            print(f"Memory usage before intersection: {get_memory_usage():.2f} MB")

            # Step 7: Perform the intersection in parallel
            print(f"Performing intersection using {num_processes} cores...")
            intersection_start = time.time()
            results = []
            with mp.Pool(num_processes) as pool:
                for i, temp_file in enumerate(temp_files):
                    print(f"Processing chunk {i+1}/{len(temp_files)}, memory: {get_memory_usage():.2f} MB")
                    result = pool.apply_async(func, args=(temp_file,))
                    results.append((i, temp_file, result))

                # Get results with progress tracking
                processed_results = []
                failed_chunks = []
                for i, temp_file, result in results:
                    try:
                        print(f"Getting result for chunk {i+1}/{len(results)}, memory: {get_memory_usage():.2f} MB")
                        processed_result, success = result.get(timeout=600)  # 10 minute timeout
                        
                        if success:
                            processed_results.append(processed_result)
                        else:
                            print(f"Chunk {i+1} failed. Saving for later processing.")
                            failed_chunks.append((i, temp_file))
                    except Exception as e:
                        print(f"Error getting result for chunk {i+1}: {e}")
                        failed_chunks.append((i, temp_file))
                        processed_results.append(gpd.GeoDataFrame(geometry=[]))
            
            intersection_end = time.time()
            print(f"Intersection completed in {intersection_end - intersection_start:.2f} seconds.")

            # Save failed chunks for later processing
            if failed_chunks:
                print(f"Saving {len(failed_chunks)} failed chunks to {failed_chunks_dir}...")
                for i, temp_file in failed_chunks:
                    # Get the file extension
                    file_ext = Path(temp_file).suffix
                    # Get the directory of the temp file (may contain additional files for shapefiles)
                    temp_file_dir = Path(temp_file).parent
                    
                    # Destination path
                    dest_file = os.path.join(failed_chunks_dir, f"failed_chunk_{i}{file_ext}")
                    
                    if file_ext == '.gpkg':
                        # For GeoPackage, just copy the file
                        shutil.copy2(temp_file, dest_file)
                        print(f"Saved failed chunk {i} to {dest_file}")
                    elif file_ext == '.shp':
                        # For Shapefile, need to copy all related files (.dbf, .shx, etc.)
                        base_name = Path(temp_file).stem
                        for f in os.listdir(temp_file_dir):
                            if f.startswith(base_name) and f.endswith(('.shp', '.shx', '.dbf', '.prj', '.cpg')):
                                source = os.path.join(temp_file_dir, f)
                                dest = os.path.join(failed_chunks_dir, f.replace(base_name, f"failed_chunk_{i}"))
                                shutil.copy2(source, dest)
                        print(f"Saved failed chunk {i} as Shapefile to {failed_chunks_dir}")
                
                # Save a metadata file with information about failed chunks
                with open(os.path.join(failed_chunks_dir, "failed_chunks_info.txt"), "w") as f:
                    f.write(f"Total failed chunks: {len(failed_chunks)}\n")
                    f.write(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                    for i, temp_file in failed_chunks:
                        f.write(f"Chunk ID: {i}, Original file: {temp_file}\n")

            # Check if any result is empty
            empty_results = sum(1 for r in processed_results if len(r) == 0)
            if empty_results > 0:
                print(f"Warning: {empty_results} out of {len(processed_results)} result chunks are empty.")

            # Step 8: Combine the results
            print("Combining results...")
            combine_start = time.time()
            if all(len(r) == 0 for r in processed_results):
                print("Error: All result chunks are empty. No intersections found.")
                return

            non_empty_results = [r for r in processed_results if len(r) > 0]
            if len(non_empty_results) == 0:
                print("Error: No non-empty results found.")
                return

            print(f"Concatenating {len(non_empty_results)} non-empty chunks...")
            result = pd.concat(non_empty_results, ignore_index=True)
            combine_end = time.time()
            print(f"Result combination completed in {combine_end - combine_start:.2f} seconds.")
            print(f"Final result has {len(result)} features.")

        # Step 9: Save the result to a new shapefile
        output_dir = Path(output_path).parent
        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)

        print(f"Saving result to {output_path}...")
        save_start = time.time()
        # Ensure result has proper column types
        for col in result.columns:
            if col != 'geometry' and result[col].dtype == 'object':
                try:
                    result[col] = pd.to_numeric(result[col], errors='ignore')
                except:
                    pass

        # Remove any problematic columns
        if 'fid' in result.columns:
            result = result.drop(columns=['fid'])

        result.to_file(output_path, driver="ESRI Shapefile")
        save_end = time.time()
        print(f"File saving completed in {save_end - save_start:.2f} seconds.")
        
        # Print summary information
        end_time = time.time()
        print(f"Total execution time: {end_time - start_time:.2f} seconds.")
        print(f"Successfully processed: {len(non_empty_results)} chunks")
        print(f"Failed chunks: {len(failed_chunks)} (saved to {failed_chunks_dir})")
        print("Division completed.")
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    divided_roofs_path = "/home/mahdi/interface/data/output/divide/filtered_roofs/filtered.shp"
    parcelles_path = "/home/mahdi/interface/data/raw/pq2/PARCELLE.SHP"
    output_path = "/home/mahdi/interface/data/output/divide/roofs_divided_by_parcelles60.shp"
    failed_chunks_dir = "/home/mahdi/interface/data/output/divide/failed"
    
    # Run the division process with multiprocessing
    divide_roofs_by_parcelles(
        divided_roofs_path,
        parcelles_path,
        output_path,
        failed_chunks_dir,
        num_processes=4  # Adjust based on your system capabilities
    )