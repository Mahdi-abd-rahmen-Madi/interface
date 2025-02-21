# main.py
import os
import glob
import logging
from concurrent.futures import ThreadPoolExecutor
from config import configure_pipeline
from align import (
    configure_logging as align_configure_logging,
    load_shapefiles,
    merge_small_adjacent_polygons,
    simplify_reference_polygons,
    align_target_to_reference_inside
)
from split import (
    split_by_attribute,
    upload_split_data_to_postgis
)

# Configure logging for the main script
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("main.log"),
        logging.StreamHandler()
    ]
)

def main():
    try:
        # Step 1: Load configuration
        config = configure_pipeline()

        # Step 2: Configure alignment logging
        align_configure_logging()

        # Step 3: Load reference and target shapefiles
        logging.info("Loading reference and target shapefiles...")
        reference_gdf, _ = load_shapefiles(config["reference_shapefile"], config["reference_shapefile"])

        # Step 4: Merge small adjacent polygons in the reference dataset
        logging.info("Merging small adjacent polygons in the reference dataset...")
        simplified_reference_gdf = merge_small_adjacent_polygons(reference_gdf, min_area=config["min_area_threshold"])
        simplified_reference_path = os.path.join(config["output_dir"], "simplified_reference.shp")
        simplified_reference_gdf.to_file(simplified_reference_path)
        logging.info(f"Simplified reference polygons saved to {simplified_reference_path}.")

        # Step 5: Align target polygons to the simplified reference polygons
        logging.info("Aligning target polygons to the simplified reference polygons...")
        aligned_output_dir = os.path.join(config["output_dir"], "aligned")
        os.makedirs(aligned_output_dir, exist_ok=True)

        # Find all target shapefiles matching the pattern
        target_shapefiles = glob.glob(config["target_shapefile_pattern"])
        if not target_shapefiles:
            logging.error(f"No target shapefiles found matching the pattern: {config['target_shapefile_pattern']}")
            return

        for target_shp in target_shapefiles:
            target_gdf, reference_gdf = load_shapefiles(target_shp, simplified_reference_path)

            # Align target polygons to reference polygons
            aligned_filename = os.path.basename(target_shp).replace("aligned_results", "aligned")
            aligned_path = os.path.join(aligned_output_dir, aligned_filename)

            aligned_gdf = align_target_to_reference_inside(
                target_gdf,
                reference_gdf.geometry.tolist(),
                max_distance=config["alignment_config"]["max_distance"],
                min_overlap_ratio=config["alignment_config"]["min_overlap_ratio"]
            )

            # Save the aligned GeoDataFrame explicitly to a file
            os.makedirs(os.path.dirname(aligned_path), exist_ok=True)
            aligned_gdf.to_file(aligned_path)
            logging.info(f"Aligned polygons saved to {aligned_path}.")

        # Step 6: Split aligned data by attribute and upload to PostGIS
        logging.info("Splitting aligned data by attribute and uploading to PostGIS...")

        aligned_shapefiles = glob.glob(os.path.join(aligned_output_dir, "*.shp"))
        if not aligned_shapefiles:
            logging.error(f"No aligned shapefiles found in directory: {aligned_output_dir}")
            return

        for aligned_shp in aligned_shapefiles:
            # Read aligned shapefile
            aligned_gdf = gpd.read_file(aligned_shp)

            # Validate the presence of the 'nom' attribute
            if config["split_attribute"] not in aligned_gdf.columns:
                raise ValueError(f"'{config['split_attribute']}' attribute missing in aligned GeoDataFrame for {aligned_shp}.")

            # Split data by the specified attribute
            split_output_dir = os.path.join(config["output_dir"], "split_data")
            fgb_paths = split_by_attribute(aligned_gdf, attribute=config["split_attribute"], output_folder=split_output_dir)

            # Upload split data to PostGIS
            upload_split_data_to_postgis(
                fgb_paths,
                connection_string=config["postgis_config"]["connection_string"],
                schema=config["postgis_config"]["schema"]
            )

        logging.info("Pipeline completed successfully.")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")


if __name__ == "__main__":
    main()