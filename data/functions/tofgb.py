import geopandas as gpd
from shapely.geometry import shape, Polygon
from shapely.ops import unary_union
from shapely.wkt import loads
import matplotlib.pyplot as plt
import os
import logging
import json
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def validate_and_repair_geometries(gdf):
    """
    Validate and repair geometries in a GeoDataFrame.

    Parameters:
        gdf (GeoDataFrame): Input GeoDataFrame.

    Returns:
        GeoDataFrame: GeoDataFrame with repaired geometries.
    """
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.buffer(0) if geom.is_valid else None)
    gdf = gdf[gdf.geometry.notnull()]  # Drop invalid geometries
    return gdf


def simplify_geometries(gdf, tolerance=10):
    """
    Simplify geometries in a GeoDataFrame.

    Parameters:
        gdf (GeoDataFrame): Input GeoDataFrame.
        tolerance (float): Simplification tolerance in meters.

    Returns:
        GeoDataFrame: Simplified GeoDataFrame.
    """
    simplified_gdf = gdf.copy()
    simplified_gdf['geometry'] = simplified_gdf['geometry'].simplify(tolerance)
    return simplified_gdf


def simplify_for_zoom_levels(gdf, zoom_levels=[(0, 5, 100), (6, 10, 10), (11, 20, 1)]):
    """
    Simplify geometries for multiple zoom levels.

    Parameters:
        gdf (GeoDataFrame): Input GeoDataFrame.
        zoom_levels (list of tuples): List of (min_zoom, max_zoom, tolerance) tuples.

    Returns:
        dict: Dictionary of simplified GeoDataFrames for each zoom level.
    """
    simplified_gdfs = {}
    for min_zoom, max_zoom, tolerance in zoom_levels:
        simplified_gdf = gdf.copy()
        simplified_gdf['geometry'] = simplified_gdf['geometry'].simplify(tolerance)
        simplified_gdfs[f"zoom_{min_zoom}_{max_zoom}"] = simplified_gdf
    return simplified_gdfs


def compare_simplified_geometries(original_gdf, simplified_gdf):
    """
    Compare original and simplified geometries.

    Parameters:
        original_gdf (GeoDataFrame): Original GeoDataFrame.
        simplified_gdf (GeoDataFrame): Simplified GeoDataFrame.

    Returns:
        dict: A dictionary containing comparison metrics.
    """
    metrics = {
        "vertex_reduction": [],
        "area_difference": []
    }

    valid_features = 0

    for idx, row in original_gdf.iterrows():
        original_geom = row['geometry']
        simplified_geom = simplified_gdf.loc[idx, 'geometry']

        if original_geom and simplified_geom:
            valid_features += 1

            # 1. Vertex Count Reduction
            original_vertex_count = len(original_geom.exterior.coords) if hasattr(original_geom, 'exterior') else 0
            simplified_vertex_count = len(simplified_geom.exterior.coords) if hasattr(simplified_geom, 'exterior') else 0
            if original_vertex_count > 0:
                vertex_reduction = (original_vertex_count - simplified_vertex_count) / original_vertex_count
                metrics["vertex_reduction"].append(vertex_reduction)

            # 2. Area Difference
            if hasattr(original_geom, 'area') and hasattr(simplified_geom, 'area') and original_geom.area > 0:
                area_diff = abs(original_geom.area - simplified_geom.area) / original_geom.area
                metrics["area_difference"].append(area_diff)

    if valid_features == 0:
        logging.warning("No valid geometries found for metric calculation.")

    return metrics


def drop_unnecessary_columns(gdf, columns_to_drop=['reference_', 'overlap_ra']):
    """
    Drop unnecessary columns from a GeoDataFrame.

    Parameters:
        gdf (GeoDataFrame): Input GeoDataFrame.
        columns_to_drop (list): List of column names to drop.

    Returns:
        GeoDataFrame: GeoDataFrame with specified columns dropped.
    """
    return gdf.drop(columns=columns_to_drop, errors='ignore')


def round_numeric_columns(gdf, columns_to_round={'surface_ut': 2, 'production': 2, 'production_attribute': 2}):
    """
    Round numeric columns in a GeoDataFrame to a specified number of decimal places.

    Parameters:
        gdf (GeoDataFrame): Input GeoDataFrame.
        columns_to_round (dict): Dictionary mapping column names to the number of decimal places.

    Returns:
        GeoDataFrame: GeoDataFrame with numeric columns rounded.
    """
    for column, decimals in columns_to_round.items():
        if column in gdf.columns and gdf[column].dtype in ['float64', 'float32']:
            gdf[column] = gdf[column].round(decimals)
    return gdf


def generate_metadata(gdf, output_path="metadata.json"):
    """
    Generate metadata for a GeoDataFrame and save it as a JSON file.

    Parameters:
        gdf (GeoDataFrame): Input GeoDataFrame.
        output_path (str): Path to save the metadata file.
    """
    metadata = {
        "bounding_box": gdf.total_bounds.tolist(),  # Bounding box [minx, miny, maxx, maxy]
        "feature_count": len(gdf),                  # Number of features
        "crs": gdf.crs.to_string(),                 # Coordinate Reference System
        "attributes": {col: str(gdf[col].dtype) for col in gdf.columns if col != "geometry"},  # Attribute details
        "processed_at": pd.Timestamp.now().isoformat(),  # Timestamp of processing
        "script_version": "1.0.0"                   # Script version
    }

    # Save metadata to a JSON file
    with open(output_path, "w") as f:
        json.dump(metadata, f, indent=4)

    logging.info(f"Metadata saved to {output_path}")


def plot_comparison(original_gdf, simplified_gdf, output_image="comparison.png"):
    """
    Plot original and simplified geometries for visual comparison and save as an image.

    Parameters:
        original_gdf (GeoDataFrame): Original GeoDataFrame.
        simplified_gdf (GeoDataFrame): Simplified GeoDataFrame.
        output_image (str): Path to save the output image file.
    """
    fig, ax = plt.subplots(1, 2, figsize=(12, 6))

    # Plot original geometries
    original_gdf.plot(ax=ax[0], color='blue', alpha=0.5, edgecolor='black')
    ax[0].set_title("Original Geometries")

    # Plot simplified geometries
    simplified_gdf.plot(ax=ax[1], color='green', alpha=0.5, edgecolor='black')
    ax[1].set_title("Simplified Geometries")

    plt.tight_layout()

    # Save the plot to a file
    plt.savefig(output_image)
    print(f"Comparison plot saved to {output_image}")


def convert_shapefile_to_flatgeobuf(input_shapefile, output_folder):
    """
    Converts a shapefile to multiple FlatGeobuf files for different zoom levels, drops unnecessary attributes,
    rounds numeric attributes, generates metadata, and calculates metrics.

    Parameters:
        input_shapefile (str): Path to the input shapefile.
        output_folder (str): Folder to save the output FlatGeobuf files.
    """
    try:
        print("Reading shapefile...")
        gdf = gpd.read_file(input_shapefile)

        # Step 1: Validate and repair geometries
        print("Validating and repairing geometries...")
        gdf = validate_and_repair_geometries(gdf)

        if len(gdf) == 0:
            logging.error("No valid geometries found in the input shapefile.")
            return

        # Step 2: Ensure CRS is EPSG:2154
        print("Ensuring CRS is EPSG:2154...")
        gdf = gdf.to_crs(epsg=2154)

        # Step 3: Drop unnecessary columns
        print("Dropping unnecessary attributes (reference_, overlap_ra)...")
        gdf = drop_unnecessary_columns(gdf, columns_to_drop=['reference_', 'overlap_ra'])

        # Step 4: Round numeric columns to specified precision
        print("Rounding numeric attributes (surface_ut, production, production_attribute)...")
        gdf = round_numeric_columns(gdf, columns_to_round={
            'surface_ut': 2,       # Round to 2 decimal places
            'production': 2,       # Round to 2 decimal places
            'production_attribute': 2  # Round to 2 decimal places
        })

        # Step 5: Generate metadata
        print("Generating metadata...")
        metadata_path = os.path.join(output_folder, "metadata.json")
        generate_metadata(gdf, output_path=metadata_path)

        # Step 6: Simplify geometries for multiple zoom levels
        print("Simplifying geometries for multiple zoom levels...")
        zoom_levels = [(0, 5, 100), (6, 10, 10), (11, 20, 1)]
        simplified_gdfs = simplify_for_zoom_levels(gdf, zoom_levels)

        # Step 7: Export simplified geometries to FlatGeobuf files and calculate metrics
        print("Exporting simplified geometries to FlatGeobuf and calculating metrics...")
        for zoom_range, simplified_gdf in simplified_gdfs.items():
            # Drop unnecessary columns and round numeric columns for the simplified GeoDataFrame
            simplified_gdf = drop_unnecessary_columns(simplified_gdf, columns_to_drop=['reference_', 'overlap_ra'])
            simplified_gdf = round_numeric_columns(simplified_gdf, columns_to_round={
                'surface_ut': 2,       # Round to 2 decimal places
                'production': 2,       # Round to 2 decimal places
                'production_attribute': 2  # Round to 2 decimal places
            })

            output_path = os.path.join(output_folder, f"{zoom_range}.fgb")
            simplified_gdf.to_file(output_path, driver="FlatGeobuf")
            print(f"Saved {output_path}")

            # Calculate and display metrics
            metrics = compare_simplified_geometries(gdf, simplified_gdf)
            if metrics["vertex_reduction"]:
                avg_vertex_reduction = sum(metrics["vertex_reduction"]) / len(metrics["vertex_reduction"])
                print(f"Zoom Range {zoom_range}: Average Vertex Reduction: {avg_vertex_reduction:.2%}")
            else:
                print(f"Zoom Range {zoom_range}: No vertex reduction data available.")

            if metrics["area_difference"]:
                avg_area_difference = sum(metrics["area_difference"]) / len(metrics["area_difference"])
                print(f"Zoom Range {zoom_range}: Average Area Difference: {avg_area_difference:.2%}")
            else:
                print(f"Zoom Range {zoom_range}: No area difference data available.")

        print("Conversion completed.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    # Example usage
    input_shapefile =  "/home/mahdi/interface/data/output/aligned_results_20250217_093030.shp"   # Replace with your shapefile path
    output_folder = "/home/mahdi/interface/data/output/multi/"     # Replace with desired output folder

    convert_shapefile_to_flatgeobuf(input_shapefile, output_folder)

