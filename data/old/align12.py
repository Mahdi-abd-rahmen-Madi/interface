import os
import logging
import geopandas as gpd
from shapely.geometry import Polygon
from shapely.ops import unary_union
from rtree import index  # For spatial indexing

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
    gdf = gdf[gdf.geometry.map(lambda geom: isinstance(geom, Polygon))]
    
    # Fix invalid geometries
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.make_valid() if not geom.is_valid else geom)
    
    # Ensure all geometries are valid Polygons
    gdf = gdf[gdf.geometry.map(lambda geom: isinstance(geom, Polygon))]
    
    if len(gdf) == 0:
        raise ValueError("No valid geometries found after cleaning.")
    
    logging.info(f"Initial count: {initial_count}, Cleaned GeoDataFrame contains {len(gdf)} valid polygons.")
    return gdf

# Step 4: Align Target Polygons to Reference Polygons
def align_target_to_reference(target_gdf, reference_polygons):
    logging.info("Aligning target polygons to reference polygons...")

    # Create an R-tree index for faster spatial queries
    idx = index.Index()
    for pos, polygon in enumerate(reference_polygons):
        if polygon is not None and isinstance(polygon, Polygon):
            idx.insert(pos, polygon.bounds)
        else:
            logging.warning(f"Skipping invalid geometry at index {pos} in reference polygons. Type: {type(polygon)}, Value: {polygon}")

    aligned_data = []
    unaligned_data = []

    for target_idx, row in target_gdf.iterrows():
        target_geom = row.geometry

        if target_geom is None or not isinstance(target_geom, Polygon):
            logging.warning(f"Skipping invalid geometry at index {target_idx} in target polygons. Type: {type(target_geom)}, Value: {target_geom}")
            continue

        attributes = row.drop("geometry").to_dict()

        best_match = None
        max_overlap_area = 0

        # Use R-tree to find candidate reference polygons that intersect with the target polygon
        candidates = [reference_polygons[i] for i in idx.intersection(target_geom.bounds) if reference_polygons[i] is not None and isinstance(reference_polygons[i], Polygon)]

        for ref in candidates:
            try:
                if target_geom.intersects(ref):
                    overlap_area = target_geom.intersection(ref).area
                    if overlap_area > max_overlap_area:
                        max_overlap_area = overlap_area
                        best_match = ref
            except Exception as e:
                logging.error(f"Error processing geometry at index {target_idx}: {e}")
                logging.error(f"Target geometry: {target_geom}")
                logging.error(f"Reference geometry: {ref}")
                continue

        if best_match:
            attributes["alignment_status"] = "aligned"
            attributes["overlap_area"] = max_overlap_area
            aligned_data.append({**attributes, "geometry": target_geom})
        else:
            attributes["alignment_status"] = "not_aligned"
            attributes["overlap_area"] = 0
            unaligned_data.append({**attributes, "geometry": target_geom})

    logging.info(f"Matched {len(aligned_data)} target polygons to reference polygons.")
    logging.info(f"Marked {len(unaligned_data)} target polygons as 'not_aligned'.")
    return aligned_data + unaligned_data

# Step 5: Save Aligned Results
def save_aligned_results(data, output_path, output_format="shp", crs="EPSG:2154"):
    logging.info("Saving aligned results...")
    if data:
        # Rename columns to avoid truncation
        data = [{**d, "alignment_st": d.pop("alignment_status"), "overlap_ar": d.pop("overlap_area")} for d in data]
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
    reference_polygons = [polygon for polygon in reference_gdf.geometry if isinstance(polygon, Polygon)]

    # Log types of geometries in reference_polygons
    logging.info(f"Number of reference polygons: {len(reference_polygons)}")

    # Align target polygons to reference polygons
    aligned_data = align_target_to_reference(target_gdf, reference_polygons)

    # Save aligned results
    output_filename = os.path.join(output_dir, f"aligned_results.{output_format}")
    save_aligned_results(aligned_data, output_filename, output_format=output_format, crs=target_crs)

    # Provide summary statistics
    aligned_count = sum(1 for d in aligned_data if d.get("alignment_st") == "aligned")
    unaligned_count = sum(1 for d in aligned_data if d.get("alignment_st") == "not_aligned")
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