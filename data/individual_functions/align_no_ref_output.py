import os
import logging
import geopandas as gpd
from shapely.geometry import Polygon, Point
from shapely.ops import unary_union, nearest_points
from datetime import datetime  # Import datetime for timestamps

# ==========================================================    no refrence output         ========================================================================
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


# Step 3: Merge Small Adjacent Polygons
def merge_small_adjacent_polygons(reference_gdf, min_area=1000):
    """
    Merge small adjacent polygons in the reference GeoDataFrame based on a minimum area threshold.
    
    Args:
        reference_gdf (GeoDataFrame): GeoDataFrame of the reference polygons.
        min_area (float): Minimum area threshold (in square meters) to consider a polygon as "small".
    
    Returns:
        GeoDataFrame: A GeoDataFrame with merged small adjacent polygons.
    """
    logging.info("Merging small adjacent polygons based on area threshold...")
    
    # Separate small and large polygons
    small_polygons = [geom for geom in reference_gdf.geometry if geom.area < min_area]
    large_polygons = [geom for geom in reference_gdf.geometry if geom.area >= min_area]
    
    # Merge all small polygons using unary_union
    if small_polygons:
        small_merged = unary_union(small_polygons)
        
        # If the result is a MultiPolygon, split it back into individual polygons
        if small_merged.geom_type == 'MultiPolygon':
            small_polygons = list(small_merged.geoms)
        else:
            small_polygons = [small_merged]
    else:
        small_polygons = []
    
    # Combine large polygons and merged small polygons
    result_polygons = large_polygons + small_polygons
    
    # Create a new GeoDataFrame for the result
    result_gdf = gpd.GeoDataFrame(geometry=result_polygons, crs=reference_gdf.crs)
    logging.info(f"Merged {len(result_gdf)} polygons after processing small adjacent polygons.")
    return result_gdf


# Step 4: Simplify Reference Polygons
def simplify_reference_polygons(reference_gdf, max_merge_distance=50):
    """
    Simplify the reference polygons by merging those that are within a specified distance and share a common segment.
    
    Args:
        reference_gdf (GeoDataFrame): GeoDataFrame of the reference polygons.
        max_merge_distance (float): Maximum allowable distance (in meters) to consider merging polygons.
    
    Returns:
        GeoDataFrame: A simplified GeoDataFrame with merged polygons.
    """
    logging.info("Simplifying reference polygons by merging adjacent ones within the specified distance...")
    
    simplified_gdf = reference_gdf.copy()
    merged_polygons = []
    
    while not simplified_gdf.empty:
        base_polygon = simplified_gdf.iloc[0].geometry
        base_index = simplified_gdf.index[0]
        
        # Find all polygons within the maximum merge distance
        candidates = simplified_gdf[simplified_gdf.geometry.distance(base_polygon.centroid) <= max_merge_distance]
        
        # Filter candidates to include only those sharing a common segment with the base polygon
        candidates = candidates[candidates.geometry.touches(base_polygon)]
        
        # Merge all candidate polygons with the base polygon
        merged_polygon = unary_union([base_polygon] + list(candidates.geometry))
        merged_polygons.append(merged_polygon)
        
        # Remove the merged polygons from the GeoDataFrame
        simplified_gdf = simplified_gdf.drop(candidates.index.tolist() + [base_index])
    
    # Convert the merged polygons back into a GeoDataFrame
    simplified_gdf = gpd.GeoDataFrame(geometry=merged_polygons, crs=reference_gdf.crs)
    logging.info(f"Simplified {len(simplified_gdf)} reference polygons.")
    return simplified_gdf


# Step 5: Core Alignment Function with Distance Threshold
def align_target_to_reference_inside(target_gdf, reference_polygons, max_distance=25, min_overlap_ratio=0.5):
    """
    Align target polygons (roofs) to reference polygons by ensuring roofs are completely inside references.
    If a roof polygon does not fully overlap but has sufficient overlap with a reference polygon, it will be marked as "aligned".
    Otherwise, it may be adjusted or marked as "not_aligned".

    Args:
        target_gdf (GeoDataFrame): GeoDataFrame of the target shapefile (roofs).
        reference_polygons (list): List of Shapely Polygons from the reference shapefile.
        max_distance (float): Maximum allowable distance (in meters) to consider a reference polygon.
        min_overlap_ratio (float): Minimum ratio of target polygon area overlapping with reference polygon to be considered "aligned".

    Returns:
        list: A list of dictionaries containing aligned data with attributes and alignment status.
    """
    logging.info("Aligning target polygons (roofs) to ensure they are inside reference polygons...")
    aligned_data = []
    unaligned_data = []

    for idx, row in target_gdf.iterrows():
        target_geom = row.geometry
        attributes = row.drop("geometry").to_dict()  # Preserve all attributes except geometry

        best_match = None
        best_overlap_ratio = 0.0

        # Check overlap with each reference polygon
        for ref in reference_polygons:
            if target_geom.intersects(ref):  # Use `intersects` for lenient alignment
                intersection_area = target_geom.intersection(ref).area
                overlap_ratio = intersection_area / target_geom.area

                # Update the best match if the current reference has higher overlap
                if overlap_ratio > best_overlap_ratio:
                    best_overlap_ratio = overlap_ratio
                    best_match = ref

        if best_match and best_overlap_ratio >= min_overlap_ratio:
            # The polygon has sufficient overlap with the reference polygon
            attributes["alignment"] = "aligned"
            attributes["ref_area"] = best_match.area
            attributes["overlap"] = best_overlap_ratio
            aligned_data.append({**attributes, "geometry": target_geom})
        else:
            # Find the nearest reference polygon and check the distance
            nearest_ref = None
            min_distance = float('inf')
            for ref in reference_polygons:
                distance = target_geom.centroid.distance(ref.centroid)
                if distance < min_distance:
                    min_distance = distance
                    nearest_ref = ref

            if nearest_ref and min_distance <= max_distance:
                # Move the target polygon so its centroid lies inside the nearest reference polygon
                new_centroid = nearest_points(nearest_ref, target_geom.centroid)[0]
                adjusted_geom = translate_polygon_to_point(target_geom, new_centroid)

                # Add the adjusted polygon to the aligned data
                attributes["alignment"] = "adjusted"
                attributes["ref_area"] = nearest_ref.area
                attributes["overlap"] = 0  # No overlap for adjusted polygons
                aligned_data.append({**attributes, "geometry": adjusted_geom})
            else:
                # No valid reference polygon found within the distance threshold, mark as "not_aligned"
                attributes["alignment"] = "not_aligned"
                attributes["ref_area"] = 0
                attributes["overlap"] = 0
                unaligned_data.append({**attributes, "geometry": target_geom})

    logging.info(f"Matched {len(aligned_data)} target polygons to reference polygons (fully inside or adjusted).")
    logging.info(f"Marked {len(unaligned_data)} target polygons as 'not_aligned'.")
    return aligned_data + unaligned_data

# Step 6: Translate Polygon to Point
def translate_polygon_to_point(polygon, point):
    """
    Translate a polygon so its centroid aligns with a given point.
    
    Args:
        polygon (Polygon): The input polygon to translate.
        point (Point): The target point where the polygon's centroid should be moved.
    
    Returns:
        Polygon: The translated polygon.
    """
    centroid = polygon.centroid
    dx = point.x - centroid.x
    dy = point.y - centroid.y
    return gpd.GeoSeries([polygon]).translate(dx, dy).iloc[0]


# Step 7: Save Aligned Results with Unique Filenames
def save_aligned_results(data, output_dir, output_format="shp", crs="EPSG:2154"):
    """
    Save the aligned results to a new shapefile or GeoJSON file with a unique filename.
    
    Args:
        data (list): List of dictionaries containing aligned data.
        output_dir (str): Directory to save the output files.
        output_format (str): Output format ('shp' for Shapefile, 'geojson' for GeoJSON).
        crs (str): CRS for the output file (e.g., "EPSG:2154").
    """
    logging.info("Saving aligned results with a unique filename...")
    if data:
        gdf = gpd.GeoDataFrame(data, geometry="geometry", crs=crs)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = os.path.join(output_dir, f"aligned_results_{timestamp}.{output_format}")
        if output_format == "shp":
            gdf.to_file(output_filename, driver="ESRI Shapefile")
            logging.info(f"Saved aligned results to {output_filename}.")
        elif output_format == "geojson":
            gdf.to_file(output_filename, driver="GeoJSON")
            logging.info(f"Saved aligned results to {output_filename}.")
    else:
        logging.warning("No data to save. No output file created.")


# Step 8: Process Shapefiles
def process_shapefiles(target_shapefile_path, reference_shapefile_path, output_dir, output_format="shp", target_crs="EPSG:2154", max_distance=1, min_area=1000):
    """
    Process two shapefiles to align target polygons (roofs) with reference polygons.
    
    Args:
        target_shapefile_path (str): Path to the target shapefile (roofs).
        reference_shapefile_path (str): Path to the reference shapefile.
        output_dir (str): Directory to save the output files.
        output_format (str): Output format ('shp' for Shapefile, 'geojson' for GeoJSON).
        target_crs (str): Target CRS (e.g., "EPSG:2154").
        max_distance (float): Maximum allowable distance (in meters) to consider a reference polygon.
        min_area (float): Minimum area threshold (in square meters) to merge small adjacent polygons.
    """
    logging.info(f"Processing target shapefile: {target_shapefile_path}")
    logging.info(f"Using reference shapefile: {reference_shapefile_path}")
    
    # Load shapefiles
    target_gdf, reference_gdf = load_shapefiles(target_shapefile_path, reference_shapefile_path, target_crs=target_crs)
    
    # Merge small adjacent polygons in the reference GeoDataFrame
    reference_gdf = merge_small_adjacent_polygons(reference_gdf, min_area=min_area)
    
    # Simplify the reference polygons
    reference_gdf = simplify_reference_polygons(reference_gdf, max_merge_distance=50)
    
    # Extract simplified reference polygons, handling MultiPolygons
    reference_polygons = []
    for geom in reference_gdf.geometry:
        if geom.geom_type == "Polygon":
            reference_polygons.append(geom)
        elif geom.geom_type == "MultiPolygon":
            reference_polygons.extend(geom.geoms)
    
    logging.info(f"Extracted {len(reference_polygons)} reference polygons for alignment.")
    
    # Perform alignment ensuring roofs are inside reference polygons
    aligned_data = align_target_to_reference_inside(target_gdf, reference_polygons, max_distance=max_distance)
    
    # Save aligned results with a unique filename
    save_aligned_results(aligned_data, output_dir, output_format=output_format, crs=target_crs)


# Step 9: Batch Processing (Hardcoded Paths)
def batch_process_shapefiles(output_dir, output_format="shp", target_crs="EPSG:2154", max_distance=25, min_area=1000):
    """
    Process hardcoded shapefiles for alignment ensuring roofs are inside reference polygons.
    """
    logging.info("Starting shapefile alignment with hardcoded paths...")
    os.makedirs(output_dir, exist_ok=True)
    target_shapefile_path = "/home/mahdi/interface/data/shapefiles/roof.shp"
    reference_shapefile_path = "/home/mahdi/interface/data/output/merged_polygons.shp"
    process_shapefiles(target_shapefile_path, reference_shapefile_path, output_dir, output_format=output_format, target_crs=target_crs, max_distance=max_distance, min_area=min_area)
    logging.info("Shapefile alignment completed.")


# Main Function
if __name__ == "__main__":
    # Configure logging
    configure_logging()
    
    # Define output directory
    output_dir = "/home/mahdi/interface/data/output"
    
    # Run batch processing with hardcoded paths
    batch_process_shapefiles(output_dir, output_format="shp", target_crs="EPSG:2154", max_distance=25, min_area=1000)