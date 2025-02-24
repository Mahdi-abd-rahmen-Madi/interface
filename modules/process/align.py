# align.py

import os
import logging
import geopandas as gpd
from shapely.geometry import Polygon, Point
from shapely.ops import unary_union, nearest_points
from shapely.affinity import translate
from config import TARGET_CRS, MIN_AREA_THRESHOLD, MAX_MERGE_DISTANCE, MAX_DISTANCE, MIN_OVERLAP_RATIO, BUFFER_DISTANCE

def load_shapefiles(target_path, reference_path, target_crs=TARGET_CRS):
    """
    Load target and reference shapefiles and ensure they use the specified CRS.
    """
    logging.info("Loading shapefiles...")
    try:
        target_gdf = gpd.read_file(target_path)
        reference_gdf = gpd.read_file(reference_path)

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

def merge_small_adjacent_polygons(reference_gdf, min_area=MIN_AREA_THRESHOLD):
    """
    Merge small adjacent polygons in the reference GeoDataFrame based on a minimum area threshold.
    """
    logging.info("Merging small adjacent polygons based on area threshold...")

    small_polygons = [geom for geom in reference_gdf.geometry if geom.area < min_area]
    large_polygons = [geom for geom in reference_gdf.geometry if geom.area >= min_area]

    if small_polygons:
        small_merged = unary_union(small_polygons)
        small_polygons = list(small_merged.geoms) if small_merged.geom_type == 'MultiPolygon' else [small_merged]

    result_polygons = large_polygons + small_polygons
    result_gdf = gpd.GeoDataFrame(geometry=result_polygons, crs=reference_gdf.crs)
    logging.info(f"Merged {len(result_gdf)} polygons after processing small adjacent polygons.")
    return result_gdf

def simplify_reference_polygons(reference_gdf, max_merge_distance=MAX_MERGE_DISTANCE, buffer_distance=BUFFER_DISTANCE):
    """
    Simplify the reference polygons by dissolving adjacent polygons with small gaps between them.
    
    Args:
        reference_gdf (GeoDataFrame): GeoDataFrame of the reference polygons.
        max_merge_distance (float): Maximum allowable distance (in meters) to consider merging polygons.
        buffer_distance (float): Distance to buffer polygons before dissolving (to close small gaps).
    
    Returns:
        GeoDataFrame: A simplified GeoDataFrame with dissolved polygons.
    """
    logging.info("Simplifying reference polygons using dissolve-based merging...")

    # Buffer polygons slightly to close small gaps
    buffered_gdf = reference_gdf.copy()
    buffered_gdf['geometry'] = buffered_gdf.geometry.buffer(buffer_distance)

    # Dissolve adjacent polygons
    dissolved_geometry = unary_union(buffered_gdf.geometry.tolist())

    # Convert MultiPolygon back to individual polygons
    if dissolved_geometry.geom_type == 'MultiPolygon':
        result_polygons = list(dissolved_geometry.geoms)
    else:
        result_polygons = [dissolved_geometry]

    # Create a new GeoDataFrame for the result
    final_gdf = gpd.GeoDataFrame(geometry=result_polygons, crs=reference_gdf.crs)

    # Log the final number of polygons
    initial_polygon_count = len(reference_gdf)
    final_polygon_count = len(final_gdf)
    logging.info(f"Simplified {final_polygon_count} reference polygons.")
    logging.info(f"Reduced the number of polygons from {initial_polygon_count} to {final_polygon_count}.")

    return final_gdf

def align_target_to_reference_inside(target_gdf, reference_polygons, max_distance=MAX_DISTANCE, min_overlap_ratio=MIN_OVERLAP_RATIO):
    """
    Align target polygons (roofs) to reference polygons based on overlap and distance criteria.
    """
    aligned_data = []
    unaligned_data = []

    for idx, row in target_gdf.iterrows():
        target_geom = row.geometry
        attributes = row.drop("geometry").to_dict()

        best_match = None
        best_overlap_ratio = 0.0

        for ref in reference_polygons:
            if target_geom.intersects(ref):
                intersection_area = target_geom.intersection(ref).area
                overlap_ratio = intersection_area / target_geom.area

                if overlap_ratio > best_overlap_ratio:
                    best_overlap_ratio = overlap_ratio
                    best_match = ref

        if best_match and best_overlap_ratio >= min_overlap_ratio:
            attributes["alignment"] = "aligned"
            attributes["ref_area"] = best_match.area
            attributes["overlap"] = best_overlap_ratio
            aligned_data.append({**attributes, "geometry": target_geom})
        else:
            nearest_ref = None
            min_distance = float('inf')

            for ref in reference_polygons:
                distance = target_geom.centroid.distance(ref.centroid)
                if distance < min_distance:
                    min_distance = distance
                    nearest_ref = ref

            if nearest_ref and min_distance <= max_distance:
                new_centroid = nearest_points(nearest_ref, target_geom.centroid)[0]
                adjusted_geom = translate(
                    target_geom,
                    xoff=new_centroid.x - target_geom.centroid.x,
                    yoff=new_centroid.y - target_geom.centroid.y
                )
                attributes["alignment"] = "adjusted"
                attributes["ref_area"] = nearest_ref.area
                attributes["overlap"] = 0
                aligned_data.append({**attributes, "geometry": adjusted_geom})
            else:
                attributes["alignment"] = "not_aligned"
                attributes["ref_area"] = 0
                attributes["overlap"] = 0
                unaligned_data.append({**attributes, "geometry": target_geom})

    logging.info(f"Matched {len(aligned_data)} target polygons (aligned or adjusted).")
    logging.info(f"Marked {len(unaligned_data)} target polygons as 'not_aligned'.")
    return aligned_data + unaligned_data