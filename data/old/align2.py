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

# Step 3: Align Target Polygons to Reference Polygons
def align_target_to_reference(target_gdf, reference_polygons):
    """
    Align target polygons to reference polygons while ensuring they are completely inside.
    Args:
        target_gdf (GeoDataFrame): GeoDataFrame of the target shapefile.
        reference_polygons (list): List of Shapely Polygons from the reference shapefile.
    Returns:
        GeoDataFrame: Aligned GeoDataFrame with preserved attributes and alignment status.
    """
    logging.info("Aligning target polygons to reference polygons...")
    reference_union = unary_union(reference_polygons)  # Combine all reference polygons for faster checks

    # Initialize a list to store aligned results
    aligned_data = []
    unaligned_data = []

    for idx, row in target_gdf.iterrows():
        target_geom = row.geometry
        attributes = row.drop("geometry").to_dict()  # Preserve all attributes except geometry

        # Check if the target polygon is completely inside any reference polygon
        best_match = None
        for ref in reference_polygons:
            if target_geom.within(ref):  # Ensure the target is completely inside the reference
                best_match = ref
                break

        if best_match:
            # Simplify the matched reference polygon for better rendering
            simplified_ref = best_match.simplify(tolerance=0.5, preserve_topology=True)
            attributes["alignment_status"] = "aligned"
            attributes["overlap_area"] = target_geom.area  # Full area since it's completely inside
            aligned_data.append({**attributes, "geometry": target_geom})
        else:
            # No match found, mark as "not_aligned"
            attributes["alignment_status"] = "not_aligned"
            attributes["overlap_area"] = 0
            unaligned_data.append({**attributes, "geometry": target_geom})

    logging.info(f"Matched {len(aligned_data)} target polygons completely inside reference polygons.")
    logging.info(f"Marked {len(unaligned_data)} target polygons as 'not_aligned'.")
    return aligned_data + unaligned_data

# Step 4: Save Aligned Results
def save_aligned_results(data, output_path, output_format="shp", crs="EPSG:2154"):
    """
    Save the aligned results to a new shapefile or GeoJSON file.
    Args:
        data (list): List of dictionaries containing aligned data.
        output_path (str): Path to save the output file.
        output_format (str): Output format ('shp' for Shapefile, 'geojson' for GeoJSON).
        crs (str): CRS for the output file (e.g., "EPSG:2154").
    """
    logging.info("Saving aligned results...")
    if data:
        gdf = gpd.GeoDataFrame(data, geometry="geometry", crs=crs)
        if output_format == "shp":
            gdf.to_file(output_path, driver="ESRI Shapefile")
            logging.info(f"Saved aligned results to {output_path}.")
        elif output_format == "geojson":
            gdf.to_file(output_path, driver="GeoJSON")
            logging.info(f"Saved aligned results to {output_path}.")
    else:
        logging.warning("No data to save. No output file created.")

# Step 5: Process Shapefiles
def process_shapefiles(target_shapefile_path, reference_shapefile_path, output_dir, output_format="shp", target_crs="EPSG:2154"):
    """
    Process two shapefiles to align target polygons with reference polygons.
    Args:
        target_shapefile_path (str): Path to the target shapefile.
        reference_shapefile_path (str): Path to the reference shapefile.
        output_dir (str): Directory to save the output files.
        output_format (str): Output format ('shp' for Shapefile, 'geojson' for GeoJSON).
        target_crs (str): Target CRS (e.g., "EPSG:2154").
    """
    logging.info(f"Processing target shapefile: {target_shapefile_path}")
    logging.info(f"Using reference shapefile: {reference_shapefile_path}")
    # Load shapefiles
    target_gdf, reference_gdf = load_shapefiles(target_shapefile_path, reference_shapefile_path, target_crs=target_crs)
    reference_polygons = [Polygon(polygon) for polygon in reference_gdf.geometry]
    # Align target polygons to reference polygons
    aligned_data = align_target_to_reference(target_gdf, reference_polygons)
    # Save aligned results
    output_filename = os.path.join(output_dir, f"aligned_results.{output_format}")
    save_aligned_results(aligned_data, output_filename, output_format=output_format, crs=target_crs)

# Step 6: Batch Processing (Hardcoded Paths)
def batch_process_shapefiles(output_dir, output_format="shp", target_crs="EPSG:2154"):
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
    process_shapefiles(target_shapefile_path, reference_shapefile_path, output_dir, output_format=output_format, target_crs=target_crs)
    logging.info("Shapefile alignment completed.")

# Main Function
if __name__ == "__main__":
    # Configure logging
    configure_logging()
    # Define output directory
    output_dir = "/home/mahdi/app/data/output"
    # Run batch processing with hardcoded paths
    batch_process_shapefiles(output_dir, output_format="shp", target_crs="EPSG:2154")