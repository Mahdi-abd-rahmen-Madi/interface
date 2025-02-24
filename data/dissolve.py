# simplify_reference_dissolve.py

import geopandas as gpd
from shapely.ops import unary_union
import logging
import os

# Step 1: Configure Logging
def configure_logging():
    """
    Configure the logging system.
    Logs will be written to 'simplify_reference.log' and also printed to the console.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("simplify_reference.log"),
            logging.StreamHandler()
        ]
    )

# Step 2: Simplify Reference Polygons Using Dissolve
def simplify_reference_polygons_dissolve(reference_gdf, buffer_distance=0.1):
    """
    Simplify the reference polygons by dissolving adjacent polygons with small gaps between them.

    Args:
        reference_gdf (GeoDataFrame): GeoDataFrame of the reference polygons.
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

# Step 3: Main Function
def main(reference_shapefile_path, output_folder="output", buffer_distance=0.1):
    """
    Load the reference shapefile, simplify it using the dissolve method, and save the result.

    Args:
        reference_shapefile_path (str): Path to the reference shapefile.
        output_folder (str): Folder to save the simplified shapefile.
        buffer_distance (float): Distance to buffer polygons before dissolving.
    """
    configure_logging()

    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)

    try:
        # Step 1: Load the reference shapefile
        logging.info("Loading reference shapefile...")
        reference_gdf = gpd.read_file(reference_shapefile_path)

        # Step 2: Simplify reference polygons using dissolve
        logging.info("Simplifying reference polygons...")
        simplified_gdf = simplify_reference_polygons_dissolve(reference_gdf, buffer_distance=buffer_distance)

        # Step 3: Save the simplified shapefile
        output_path = os.path.join(output_folder, "simplified_reference.shp")
        logging.info(f"Saving simplified reference shapefile to {output_path}...")
        simplified_gdf.to_file(output_path, driver="ESRI Shapefile")

        logging.info("Simplification complete. Preview the simplified shapefile in your GIS software.")
    except Exception as e:
        logging.error(f"Error during simplification: {e}")

# Entry Point
if __name__ == "__main__":
    # Update these paths as needed
    reference_shapefile_path = "/home/mahdi/interface/data/shapefiles/reference.shp"  # Replace with your reference shapefile path
    output_folder = "/home/mahdi/interface/data/output/dissolve_test"  # Output folder for the simplified shapefile

    main(reference_shapefile_path, output_folder=output_folder, buffer_distance=0.1)