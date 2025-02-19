import geopandas as gpd
from shapely.geometry import shape, Polygon
from shapely.ops import unary_union
from shapely.wkt import loads
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
import os
import logging
import subprocess

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


def round_numeric_columns(gdf, columns_to_round={'surface_ut': 2, 'production': 3, 'PROD_EURO': 4}):
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


def generate_vector_tiles(input_fgb, output_mbtiles, max_zoom=14, min_zoom=0):
    """
    Generate vector tiles (MBTiles) from a FlatGeobuf file.

    Parameters:
        input_fgb (str): Path to the input FlatGeobuf file.
        output_mbtiles (str): Path to save the output MBTiles file.
        max_zoom (int): Maximum zoom level for tile generation.
        min_zoom (int): Minimum zoom level for tile generation.
    """
    try:
        print(f"Generating vector tiles from {input_fgb}...")

        # Run tippecanoe to generate MBTiles
        subprocess.run([
            "tippecanoe",
            "-o", output_mbtiles,  # Output MBTiles file
            "--force",             # Overwrite existing files
            "--no-tile-stats",     # Disable tile statistics for faster processing
            "--maximum-zoom", str(max_zoom),
            "--minimum-zoom", str(min_zoom),
            "--simplification", "10",  # Simplify geometries during tile generation
            input_fgb              # Input FlatGeobuf file
        ], check=True)

        print(f"Vector tiles saved to {output_mbtiles}")
    except subprocess.CalledProcessError as e:
        logging.error(f"An error occurred while generating vector tiles: {e}")


def calculate_gradient_classes(gdf, attribute="PROD_EURO", num_classes=5, colormap="cividis"):
    """
    Calculate gradient classes for an attribute using pretty breaks.

    Parameters:
        gdf (GeoDataFrame): Input GeoDataFrame.
        attribute (str): Attribute to calculate gradient classes for.
        num_classes (int): Number of classes for the gradient.
        colormap (str): Matplotlib colormap name.

    Returns:
        dict: A dictionary containing class breaks and colors.
    """
    if attribute not in gdf.columns:
        raise ValueError(f"Attribute '{attribute}' not found in the dataset.")

    # Extract values for the specified attribute
    values = gdf[attribute].dropna()

    if len(values) == 0:
        raise ValueError(f"No valid values found for attribute '{attribute}'.")

    # Calculate pretty breaks
    breaks = plt.matplotlib.ticker.MaxNLocator(nbins=num_classes).tick_values(values.min(), values.max())

    # Generate colors from the colormap
    cmap = plt.cm.get_cmap(colormap, num_classes)
    colors = [mcolors.to_hex(cmap(i)) for i in range(num_classes)]

    # Create a mapping of breaks to colors
    gradient_classes = {
        "breaks": breaks.tolist(),
        "colors": colors
    }

    return gradient_classes


def convert_shapefile_to_vector_tiles(input_shapefile, output_folder, attribute="PROD_EURO", num_classes=5, colormap="cividis"):
    """
    Converts a shapefile to vector tiles (MBTiles) with gradient styling.

    Parameters:
        input_shapefile (str): Path to the input shapefile.
        output_folder (str): Folder to save the output MBTiles file.
        attribute (str): Attribute to use for gradient styling.
        num_classes (int): Number of classes for the gradient.
        colormap (str): Matplotlib colormap name.
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

        # Step 4: Round numeric columns
        print("Rounding numeric attributes (surface_ut, production, PROD_EURO)...")
        gdf = round_numeric_columns(gdf, columns_to_round={
            'surface_ut': 2,       # Round to 2 decimal places
            'production': 3,       # Round to 3 decimal places
            'PROD_EURO': 4         # Round to 4 decimal places
        })

        # Step 5: Export to FlatGeobuf
        print("Exporting data to FlatGeobuf...")
        fgb_path = os.path.join(output_folder, "data.fgb")
        gdf.to_file(fgb_path, driver="FlatGeobuf")
        print(f"FlatGeobuf file saved to {fgb_path}")

        # Step 6: Generate vector tiles (MBTiles)
        print("Generating vector tiles with gradient styling...")
        mbtiles_path = os.path.join(output_folder, "data.mbtiles")
        generate_vector_tiles(fgb_path, mbtiles_path, max_zoom=14, min_zoom=0)
        print(f"Vector tiles saved to {mbtiles_path}")

        # Step 7: Calculate gradient classes for styling
        gradient_classes = calculate_gradient_classes(gdf, attribute=attribute, num_classes=num_classes, colormap=colormap)
        print("Gradient classes calculated:")
        print(gradient_classes)

        # Save gradient classes to a JSON file for use in frontend styling
        json_path = os.path.join(output_folder, "gradient_classes.json")
        with open(json_path, "w") as f:
            import json
            json.dump(gradient_classes, f, indent=4)
        print(f"Gradient classes saved to {json_path}")

        print("Conversion completed.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    # Example usage
    input_shapefile = "/home/mahdi/interface/data/output/aligned_results_20250217_093030.shp"  # Replace with your shapefile path
    output_folder = "/home/mahdi/interface/data/output/Vector_tiles/"      # Replace with desired output folder

    convert_shapefile_to_vector_tiles(input_shapefile, output_folder, attribute="PROD_EURO/", num_classes=5, colormap="cividis")
