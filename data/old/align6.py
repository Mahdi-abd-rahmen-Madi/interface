import os
import logging
import geopandas as gpd
from shapely.geometry import Polygon
from shapely.ops import unary_union

# Step 1: Configure Logging
def configure_logging():
    """
    Configure the logging system.
    Logs will be written to 'align_shapefiles.log' and also printed to the console.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("align_shapefiles.log"),
            logging.StreamHandler()
        ]
    )

# Step 2: Load Shapefiles
def load_shapefiles(target_shapefile_path, reference_shapefile_path, target_crs="EPSG:2154"):
    """
    Load target and reference shapefiles and ensure they use the specified CRS.
    Args:
        target_shapefile_path (str): Path to the target shapefile.
        reference_shapefile_path (str): Path to the reference shapefile.
        target_crs (str): Target CRS (e.g., "EPSG:2154").
    Returns:
        tuple: Two GeoDataFrames for the target and reference shapefiles.
    """
    logging.info("Loading shapefiles...")
    try:
        target_gdf = gpd.read_file(target_shapefile_path)
        reference_gdf = gpd.read_file(reference_shapefile_path)
        # Ensure both shapefiles use the specified CRS
        if target_gdf.crs is None or not target_gdf.crs.to_string().startswith(target_crs):
            logging.warning(f"Target shapefile CRS mismatch. Converting to {target_crs}.")
            target_gdf = target_gdf.to_crs(target_crs)
        if reference_gdf.crs is None or not reference_gdf.crs.to_string().startswith(target_crs):
            logging.warning(f"Reference shapefile CRS mismatch. Converting to {target_crs}.")
            reference_gdf = reference_gdf.to_crs(target_crs)
        logging.info(f"Loaded {len(target_gdf)} polygons from target shapefile.")
        logging.info(f"Loaded {len(reference_gdf)} polygons from reference shapefile.")
        return target_gdf, reference_gdf
    except Exception as e:
        logging.error(f"Error loading shapefiles: {e}")
        raise

# Step 3: Apply Buffer to Target Polygons
def apply_buffer_to_polygons(polygons, buffer_distance):
    """
    Apply a buffer to a list of Shapely Polygons.
    Args:
        polygons (list): List of Shapely Polygons.
        buffer_distance (float): Distance to buffer the polygons.
    Returns:
        list: List of buffered Shapely Polygons.
    """
    logging.info(f"Applying buffer of {buffer_distance} to target polygons...")
    buffered_polygons = [polygon.buffer(buffer_distance) for polygon in polygons]
    return buffered_polygons

# Step 4: Align Target Polygons to Reference Polygons
def align_target_to_reference(original_target_polygons, buffered_target_polygons, reference_polygons):
    """
    Align original target polygons to reference polygons using buffered polygons for overlap checks.
    Args:
        original_target_polygons (list): List of original Shapely Polygons from the target shapefile.
        buffered_target_polygons (list): List of buffered Shapely Polygons from the target shapefile.
        reference_polygons (list): List of Shapely Polygons from the reference shapefile.
    Returns:
        list: List of tuples (original_target_polygon, matched_reference_polygon).
    """
    matches = []
    reference_union = unary_union(reference_polygons)  # Combine all reference polygons for faster intersection checks
    logging.info("Aligning target polygons to reference polygons...")

    for original, buffered in zip(original_target_polygons, buffered_target_polygons):
        if buffered.intersects(reference_union):
            # Find the most overlapping reference polygon
            best_match = None
            max_overlap_area = 0
            for ref in reference_polygons:
                if buffered.intersects(ref):
                    overlap_area = buffered.intersection(ref).area
                    if overlap_area > max_overlap_area:
                        max_overlap_area = overlap_area
                        best_match = ref
            if best_match:
                # Simplify the matched reference polygon for better rendering
                simplified_ref = best_match.simplify(tolerance=0.5, preserve_topology=True)
                matches.append((original, simplified_ref))
                logging.debug(f"Matched target polygon to reference polygon.")

    logging.info(f"Matched {len(matches)} target polygons to reference polygons.")
    return matches

# Step 5: Save Aligned Results
def save_aligned_results(matches, output_path, output_format="shp", crs="EPSG:2154"):
    """
    Save the aligned results to a new shapefile or GeoJSON file.
    Args:
        matches (list): List of tuples (original_target_polygon, matched_reference_polygon).
        output_path (str): Path to save the output file.
        output_format (str): Output format ('shp' for Shapefile, 'geojson' for GeoJSON).
        crs (str): CRS for the output file (e.g., "EPSG:2154").
    """
    logging.info("Saving aligned results...")
    data = []
    for target, reference in matches:
        data.append({
            "geometry": target,  # Store the original target geometry
            "ovlp_area": target.intersection(reference).area
        })
    if data:
        gdf = gpd.GeoDataFrame(data, geometry="geometry", crs=crs)
        gdf.rename(columns={"ovlp_area": "overlap_area"}, inplace=True)  # Rename for clarity
        if output_format == "shp":
            gdf.to_file(output_path, driver="ESRI Shapefile")
            logging.info(f"Saved aligned results to {output_path}.")
        elif output_format == "geojson":
            gdf.to_file(output_path, driver="GeoJSON")
            logging.info(f"Saved aligned results to {output_path}.")
    else:
        logging.warning("No matches found. No output file created.")

# Step 6: Process Shapefiles
def process_shapefiles(target_shapefile_path, reference_shapefile_path, output_dir, buffer_distance=1.0, output_format="shp", target_crs="EPSG:2154"):
    """
    Process two shapefiles to align target polygons with reference polygons.
    Args:
        target_shapefile_path (str): Path to the target shapefile.
        reference_shapefile_path (str): Path to the reference shapefile.
        output_dir (str): Directory to save the output files.
        buffer_distance (float): Distance to buffer the target polygons.
        output_format (str): Output format ('shp' for Shapefile, 'geojson' for GeoJSON).
        target_crs (str): Target CRS (e.g., "EPSG:2154").
    """
    logging.info(f"Processing target shapefile: {target_shapefile_path}")
    logging.info(f"Using reference shapefile: {reference_shapefile_path}")
    # Load shapefiles
    target_gdf, reference_gdf = load_shapefiles(target_shapefile_path, reference_shapefile_path, target_crs=target_crs)
    original_target_polygons = [Polygon(polygon) for polygon in target_gdf.geometry]
    reference_polygons = [Polygon(polygon) for polygon in reference_gdf.geometry]

    # Apply buffer to target polygons
    buffered_target_polygons = apply_buffer_to_polygons(original_target_polygons, buffer_distance)

    # Align original target polygons to reference polygons using buffered polygons for overlap checks
    matches = align_target_to_reference(original_target_polygons, buffered_target_polygons, reference_polygons)

    # Save aligned results
    output_filename = os.path.join(output_dir, f"aligned_results.{output_format}")
    save_aligned_results(matches, output_filename, output_format=output_format, crs=target_crs)

# Step 7: Batch Processing (Hardcoded Paths)
def batch_process_shapefiles(output_dir, buffer_distance=1.0, output_format="shp", target_crs="EPSG:2154"):
    """
    Process hardcoded shapefiles for alignment.
    """
    logging.info("Starting shapefile alignment with hardcoded paths...")
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    # Hardcoded paths
    target_shapefile_path = "/home/mahdi/app/data/shapefiles/roof.shp"
    reference_shapefile_path = "/home/mahdi/app/data/shapefiles/reference.shp"
    # Process the shapefiles
    process_shapefiles(target_shapefile_path, reference_shapefile_path, output_dir, buffer_distance=buffer_distance, output_format=output_format, target_crs=target_crs)
    logging.info("Shapefile alignment completed.")

# Main Function
if __name__ == "__main__":
    # Configure logging
    configure_logging()
    # Define output directory
    output_dir = "/home/mahdi/app/data/output"
    # Run batch processing with hardcoded paths
    batch_process_shapefiles(output_dir, buffer_distance=1.0, output_format="shp", target_crs="EPSG:2154")