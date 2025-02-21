# align.py
import os
import logging
import geopandas as gpd
from shapely.geometry import Polygon, Point
from shapely.ops import unary_union, nearest_points

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

    # Log the initial number of polygons
    initial_polygon_count = len(reference_gdf)
    logging.info(f"Initial number of reference polygons: {initial_polygon_count}")

    # Initialize an empty list to store merged polygons
    merged_polygons = []
    attributes_list = []  # Preserve attributes

    while not reference_gdf.empty:
        # Take the first polygon as the base for potential merging
        base_polygon = reference_gdf.iloc[0].geometry
        base_index = reference_gdf.index[0]

        # Find all polygons within the maximum merge distance
        candidates = reference_gdf[
            reference_gdf.geometry.distance(base_polygon.centroid) <= max_merge_distance
        ]

        if len(candidates) > 1:
            logging.info(f"Merging {len(candidates)} polygons into a single polygon.")

            # Merge the base polygon with all candidates
            merged_polygon = unary_union([base_polygon] + candidates.geometry.tolist())
            merged_polygons.append(merged_polygon)

            # Preserve attributes from the base polygon
            attributes_list.extend([reference_gdf.loc[base_index].drop("geometry").to_dict()])

            # Remove the merged polygons from the GeoDataFrame
            reference_gdf = reference_gdf.drop(candidates.index.tolist() + [base_index])
        else:
            # No candidates found, keep the base polygon as-is
            merged_polygons.append(base_polygon)
            attributes_list.append(reference_gdf.loc[base_index].drop("geometry").to_dict())

            reference_gdf = reference_gdf.drop(base_index)

    # Combine attributes and geometries into a new GeoDataFrame
    result_gdf = gpd.GeoDataFrame(attributes_list, geometry=merged_polygons, crs=reference_gdf.crs)

    # Log the final number of polygons
    final_polygon_count = len(result_gdf)
    logging.info(f"Simplified {final_polygon_count} reference polygons.")
    logging.info(f"Reduced the number of polygons from {initial_polygon_count} to {final_polygon_count}.")
    return result_gdf


# Step 5: Core Alignment Function with Distance Threshold
# align.py
def align_target_to_reference_inside(target_gdf, reference_polygons, max_distance=25, min_overlap_ratio=0.5):
    """
    Align target polygons (roofs) to reference polygons by ensuring roofs are completely inside references.
    
    Args:
        target_gdf (GeoDataFrame): GeoDataFrame of the target shapefile (roofs).
        reference_polygons (list): List of Shapely Polygons from the reference shapefile.
        max_distance (float): Maximum allowable distance (in meters) to consider a reference polygon.
        min_overlap_ratio (float): Minimum ratio of target polygon area overlapping with reference polygon to be considered "aligned".
    
    Returns:
        GeoDataFrame: A GeoDataFrame containing aligned data with attributes and alignment status.
    """
    logging.info("Aligning target polygons (roofs) to ensure they are inside reference polygons...")
    aligned_data = []

    for idx, row in target_gdf.iterrows():
        target_geom = row.geometry
        attributes = row.drop("geometry").to_dict()  # Preserve all attributes, including 'nom'

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
                attributes["alignment"] = "nearby"
                attributes["nearest_ref_distance"] = min_distance
            else:
                attributes["alignment"] = "not_aligned"

        # Append the aligned polygon with its attributes
        aligned_data.append({**attributes, "geometry": target_geom})

    # Create a new GeoDataFrame for the aligned data
    aligned_gdf = gpd.GeoDataFrame(aligned_data, crs=target_gdf.crs)

    # Debug: Log column names of the aligned GeoDataFrame
    logging.debug(f"Aligned GeoDataFrame columns: {aligned_gdf.columns.tolist()}")

    logging.info(f"Aligned {len(aligned_gdf)} target polygons.")
    return aligned_gdf