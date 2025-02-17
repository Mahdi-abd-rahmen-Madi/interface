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
    
    # Log geometry types before cleaning
    logging.info(f"Geometry types in GeoDataFrame: {gdf.geometry.apply(type).value_counts()}")
    
    # Filter out non-Polygon and non-MultiPolygon geometries
    gdf = gdf[gdf.geometry.apply(lambda geom: isinstance(geom, (Polygon, MultiPolygon)))]
    
    # Fix invalid geometries
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.make_valid() if geom and not geom.is_valid else geom)
    
    # Ensure all geometries are valid Polygons or MultiPolygons
    gdf = gdf[gdf.geometry.apply(lambda geom: isinstance(geom, (Polygon, MultiPolygon)))]
    
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
        return []

# Helper Function: Calculate IoU (Intersection over Union)
def calculate_iou(target, reference):
    intersection_area = target.intersection(reference).area
    union_area = target.union(reference).area
    if union_area == 0:
        return 0
    return intersection_area / union_area

# Helper Function: Calculate Centroid Distance
def calculate_centroid_distance(target, reference):
    return target.centroid.distance(reference.centroid)

# Helper Function: Calculate Alignment Score
def calculate_alignment_score(target, reference, overlap_area, iou, centroid_distance):
    # Weighted scoring system for matching
    return (0.5 * overlap_area) + (0.3 * iou) - (0.2 * centroid_distance)

# Helper Function: Find Best Match with Enhanced Logic
def find_best_match(target_geom, reference_tree, reference_geometries, buffer_tolerance=0.5):
    best_match = None
    max_score = -1
    
    # Buffering strategy
    buffer_variants = [target_geom, target_geom.buffer(buffer_tolerance), target_geom.buffer(-buffer_tolerance)]
    
    for variant in buffer_variants:
        if variant.is_empty or not variant.is_valid:
            continue
        
        # Query using STRtree for candidate polygons
        candidates = reference_tree.query(variant)
        
        for ref in candidates:
            if not ref.is_valid or ref.is_empty:
                continue
            
            try:
                if variant.intersects(ref):
                    overlap_area = variant.intersection(ref).area
                    iou = calculate_iou(variant, ref)
                    centroid_distance = calculate_centroid_distance(variant, ref)
                    score = calculate_alignment_score(variant, ref, overlap_area, iou, centroid_distance)
                    
                    if score > max_score:
                        max_score = score
                        best_match = ref
            except Exception as e:
                logging.error(f"Error processing geometry: {e}")
                continue
    
    return best_match, max_score

# Step 4: Align Target Polygons to Reference Polygons
def align_target_to_reference(target_gdf, reference_polygons, buffer_tolerance=0.5):
    logging.info("Aligning target polygons to reference polygons...")

    # Flatten MultiPolygons and create a spatial index using STRtree
    reference_geometries = [geom for polygon in reference_polygons for geom in flatten_geometries(polygon)]
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
        
        best_overall_match = None
        max_total_score = -1
        
        # Check each Polygon component
        for polygon in target_polygons:
            best_match, max_score = find_best_match(polygon, reference_tree, reference_geometries, buffer_tolerance)
            
            if max_score > max_total_score:
                max_total_score = max_score
                best_overall_match = best_match
        
        attributes = row.drop("geometry").to_dict()  # Convert row to dictionary
        
        # If a match is found
        if best_overall_match:
            attributes["align_stat"] = "aligned"
            attributes["overlap_ar"] = max_total_score
        else:
            attributes["align_stat"] = "not_aligned"
            attributes["overlap_ar"] = 0

        # Return as a plain dictionary with geometry included
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
        
        # Explicitly rename columns to avoid truncation warnings
        gdf.rename(columns={
            "align_stat": "align_stat",
            "overlap_ar": "overlap_ar"
        }, inplace=True)
        
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

    # Print first few geometries for inspection
    print("First few geometries in target_gdf:")
    print(target_gdf.geometry.head())
    print("First few geometries in reference_gdf:")
    print(reference_gdf.geometry.head())

    # Log geometry types before cleaning
    logging.info(f"Geometry types in target_gdf: {target_gdf.geometry.apply(type).value_counts()}")
    logging.info(f"Geometry types in reference_gdf: {reference_gdf.geometry.apply(type).value_counts()}")

    # Clean and validate geometries
    target_gdf = clean_and_validate_geometries(target_gdf)
    reference_gdf = clean_and_validate_geometries(reference_gdf)

    # Log geometry types after cleaning
    logging.info(f"Geometry types in target_gdf after cleaning: {target_gdf.geometry.apply(type).value_counts()}")
    logging.info(f"Geometry types in reference_gdf after cleaning: {reference_gdf.geometry.apply(type).value_counts()}")

    # Create reference polygons list
    reference_polygons = [polygon for polygon in reference_gdf.geometry if isinstance(polygon, (Polygon, MultiPolygon))]

    # Log types of geometries in reference_polygons
    logging.info(f"Number of reference polygons: {len(reference_polygons)}")
    for i, polygon in enumerate(reference_polygons[:10]):
        logging.info(f"Index {i}: Type: {type(polygon)}, Value: {polygon}")

    # Align target polygons to reference polygons
    aligned_data = align_target_to_reference(target_gdf, reference_polygons)

    # Debugging: Log the type and content of each element in aligned_data
    for i, d in enumerate(aligned_data[:5]):  # Log first 5 items for inspection
        logging.debug(f"Index {i}: Type: {type(d)}, Content: {d}")

    # Ensure all elements in aligned_data are dictionaries
    aligned_data = [d for d in aligned_data if isinstance(d, dict)]

    # Save aligned results
    output_filename = os.path.join(output_dir, f"aligned_results.{output_format}")
    save_aligned_results(aligned_data, output_filename, output_format=output_format, crs=target_crs)

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