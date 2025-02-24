# main.py

import argparse
import os 
import geopandas as gpd
import logging
from align import load_shapefiles, simplify_reference_polygons, align_target_to_reference_inside
from split import split_and_save, upload_to_postgis
from utils import configure_logging, check_dependencies
from config import TARGET_CRS, MIN_AREA_THRESHOLD, MAX_MERGE_DISTANCE, OUTPUT_FOLDER, POSTGIS_SCHEMA

def main(target_path, reference_path):
    """
    Main function to execute the alignment and splitting process.
    """
    configure_logging()
    check_dependencies()

    # Step 1: Load shapefiles
    logging.info("Loading shapefiles...")
    target_gdf, reference_gdf = load_shapefiles(target_path, reference_path, target_crs=TARGET_CRS)

    # Step 2: Simplify reference polygons (combines merging and simplification)
    logging.info("Simplifying reference polygons...")
    reference_gdf = simplify_reference_polygons(reference_gdf, max_merge_distance=MAX_MERGE_DISTANCE)

    # Step 3: Align target polygons to reference polygons
    logging.info("Aligning target polygons...")
    reference_polygons = list(reference_gdf.geometry)
    aligned_results = align_target_to_reference_inside(target_gdf, reference_polygons)

    # Step 4: Save aligned results
    logging.info("Saving aligned results...")
    aligned_gdf = gpd.GeoDataFrame(aligned_results, crs=target_gdf.crs)
    split_and_save(aligned_gdf, attribute="alignment")

    # Step 5: Upload to PostGIS
    logging.info("Uploading data to PostGIS...")
    upload_to_postgis(aligned_gdf, "aligned_roofs")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Shapefile Alignment and Splitting Tool")
    parser.add_argument("target_shapefile", help="Path to the target shapefile")
    parser.add_argument("reference_shapefile", help="Path to the reference shapefile")
    args = parser.parse_args()

    main(args.target_shapefile, args.reference_shapefile)