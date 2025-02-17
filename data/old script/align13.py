import os
import logging
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
from shapely.strtree import STRtree
from concurrent.futures import ThreadPoolExecutor

# Step 1: Configure Logging
def configure_logging():
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
    logging.info("Loading shapefiles...")
    try:
        target_gdf = gpd.read_file(target_shapefile_path)
        reference_gdf = gpd.read_file(reference_shapefile_path)

        if target_gdf.empty:
            raise ValueError("Target shapefile is empty.")
        if reference_gdf.empty:
            raise ValueError("Reference shapefile is empty.")

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

# Step 3: Clean and Validate Geometries
def clean_and_validate_geometries(gdf):
    logging.info("Cleaning and validating geometries...")
    initial_count = len(gdf)
    
    # Remove rows with invalid or None geometries
    gdf = gdf[gdf.geometry.notnull()]
    gdf = gdf[gdf.geometry.map(lambda geom: isinstance(geom, (Polygon, MultiPolygon)))]
    
    # Fix invalid geometries
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.make_valid() if not geom.is_valid else geom)
    
    # Ensure all geometries are valid Polygons or MultiPolygons
    gdf = gdf[gdf.geometry.map(lambda geom: isinstance(geom, (Polygon, MultiPolygon)))]
    
    if len(gdf) == 0:
        raise ValueError("No valid geometries found after cleaning.")
    
    logging.info(f"Initial count: {initial_count}, Cleaned GeoDataFrame contains {len(gdf)} valid polygons.")
    return gdf

# Helper Function: Flatten MultiPolygons
def flatten_geometries(geometry):
    if isinstance(geometry, MultiPolygon):
        return list(geometry.geoms)
    elif isinstance(geometry, Polygon):
        return [geometry]
    else:
        logging.warning(f"Skipping invalid geometry: Type: {type(geometry)}, Value: {geometry}")
        return []

# Helper Function: Find Best Match
def find_best_match(target_geom, reference_tree, reference_geometries):
    best_match = None
    max_overlap_area = 0
    
    # Query using STRtree for candidate polygons
    candidates = reference_tree.query(target_geom)
    
    for ref in candidates:
        try:
            if target_geom.intersects(ref):
                overlap_area = target_geom.intersection(ref).area
                if overlap_area > max_overlap_area:
                    max_overlap_area = overlap_area
                    best_match = ref
        except Exception as e:
            logging.error(f"Error processing geometry: {e}")
            logging.error(f"Target geometry: {target_geom}")
            logging.error(f"Reference geometry: {ref}")
            continue
    
    return best_match, max_overlap_area

# Step 4: Align Target Polygons to Reference Polygons
def align_target_to_reference(target_gdf, reference_polygons):
    logging.info("Aligning target polygons to reference polygons...")
    
    # Flatten MultiPolygons and create a spatial index using STRtree
    reference_geometries = []
    for polygon in reference_polygons:
        flat_geoms = flatten_geometries(polygon)
        if flat_geoms:
            reference_geometries.extend(flat_geoms)
        else:
            logging.warning(f"Skipping invalid geometry in reference polygons: {polygon}")
    
    if not reference_geometries:
        raise ValueError("No valid reference geometries found after flattening.")
    
    reference_tree = STRtree(reference_geometries)
    aligned_data = []
    unaligned_data = []
    
    # Function to process each target row
    def process_row(target_idx, row):
        target_geom = row.geometry
        if target_geom is None:
            logging.warning(f"Skipping None geometry at index {target_idx}")
            return None
        
        # Flatten target geometry if MultiPolygon
        target_polygons = flatten_geometries(target_geom)
        
        if not target_polygons:
            logging.warning(f"Skipping invalid geometry at index {target_idx}: {target_geom}")
            return None
        
        best_overall_match = None
        max_total_overlap = 0
        
        # Check each Polygon component
        for polygon in target_polygons:
            best_match, max_overlap_area = find_best_match(polygon, reference_tree, reference_geometries)
            
            if max_overlap_area > max_total_overlap:
                max_total_overlap = max_overlap_area
                best_overall_match = best_match
        
        attributes = row.drop("geometry").to_dict()
        
        # If a match is found
        if best_overall_match:
            attributes["align_stat"] = "aligned"
            attributes["overlap"] = max_total_overlap
            return {**attributes, "geometry": target_geom}
        else:
            attributes["align_stat"] = "not_aligned"
            attributes["overlap"] = 0
            return {**attributes, "geometry": target_geom}
    
    # Parallelize the alignment process using ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(lambda idx_row: process_row(*idx_row), target_gdf.iterrows()))
    
    # Collect results
    for result in results:
        if result:
            if result["align_stat"] == "aligned":
                aligned_data.append(result)
            else:
                unaligned_data.append(result)
    
    logging.info(f"Matched {len(aligned_data)} target polygons to reference polygons.")
    logging.info(f"Marked {len(unaligned_data)} target polygons as 'not_aligned'.")
    return aligned_data + unaligned_data

# Step 5: Save Aligned Results
def save_aligned_results(data, output_path, output_format="shp", crs="EPSG:2154"):
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

# Step 6: Process Shapefiles
def process_shapefiles(target_shapefile_path, reference_shapefile_path, output_dir, output_format="shp", target_crs="EPSG:2154"):
    logging.info(f"Processing target shapefile: {target_shapefile_path}")
    logging.info(f"Using reference shapefile: {reference_shapefile_path}")

    target_gdf, reference_gdf = load_shapefiles(target_shapefile_path, reference_shapefile_path, target_crs=target_crs)

    # Clean and validate geometries
    target_gdf = clean_and_validate_geometries(target_gdf)
    reference_gdf = clean_and_validate_geometries(reference_gdf)

    # Create reference polygons list
    reference_polygons = [polygon for polygon in reference_gdf.geometry if isinstance(polygon, (Polygon, MultiPolygon))]

    # Log types of geometries in reference_polygons
    logging.info(f"Number of reference polygons: {len(reference_polygons)}")

    # Align target polygons to reference polygons
    aligned_data = align_target_to_reference(target_gdf, reference_polygons)

    # Save aligned results
    output_filename = os.path.join(output_dir, f"aligned_results.{output_format}")
    save_aligned_results(aligned_data, output_filename, output_format=output_format, crs=target_crs)

    # Provide summary statistics
    aligned_count = sum(1 for d in aligned_data if d.get("align_stat") == "aligned")
    unaligned_count = sum(1 for d in aligned_data if d.get("align_stat") == "not_aligned")
    total_count = len(aligned_data)
    logging.info(f"Summary: Total polygons processed = {total_count}, Aligned = {aligned_count}, Not Aligned = {unaligned_count}")

# Main Function
if __name__ == "__main__":
    configure_logging()

    target_shapefile_path = "/home/mahdi/app/data/shapefiles/roof.shp"
    reference_shapefile_path = "/home/mahdi/app/data/shapefiles/reference.shp"
    output_dir = "/home/mahdi/app/data/output"
    output_format = "shp"
    target_crs = "EPSG:2154"

    os.makedirs(output_dir, exist_ok=True)

    try:
        process_shapefiles(target_shapefile_path, reference_shapefile_path, output_dir, output_format=output_format, target_crs=target_crs)
        logging.info("Shapefile alignment completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during processing: {e}")