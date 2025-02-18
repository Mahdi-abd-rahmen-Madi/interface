import geopandas as gpd
import json

def reduce_precision(input_shapefile, output_geojson):
    """
    Reduces the precision and length of numeric fields in a shapefile and saves the output as GeoJSON.

    Args:
        input_shapefile (str): Path to the input shapefile.
        output_geojson (str): Path to save the output GeoJSON file.
    """
    try:
        # Read the shapefile
        print(f"Reading shapefile: {input_shapefile}...")
        gdf = gpd.read_file(input_shapefile)

        # Define target numeric columns based on the provided table
        numeric_columns = ['superficie', 'surface_ut', 'production', 'PROD_EURO', 'reference_', 'overlap_ra']

        if not numeric_columns:
            print("No numeric columns found to process.")
            return

        print(f"Processing numeric columns: {numeric_columns}")

        # Process each numeric column
        for col in numeric_columns:
            if col in gdf.columns:
                print(f"Processing column: {col}")
                # Ensure values are float for consistent processing
                gdf[col] = gdf[col].astype(float)
                # Round to 4 decimal places
                gdf[col] = gdf[col].round(4)
                # Truncate to ensure string representation does not exceed 8 characters
                gdf[col] = gdf[col].apply(lambda x: truncate_to_length(x))

        # Convert GeoDataFrame to dictionary and round coordinates to 4 decimal places
        geojson_data = json.loads(gdf.to_json())
        for feature in geojson_data.get("features", []):
            if "geometry" in feature and feature["geometry"] and feature["geometry"]["coordinates"]:
                feature["geometry"]["coordinates"] = recursive_round_coordinates(feature["geometry"]["coordinates"], 4)

        # Save the modified data as GeoJSON with limited precision
        print(f"Saving output GeoJSON: {output_geojson}...")
        with open(output_geojson, "w") as f:
            json.dump(geojson_data, f, indent=2)

        print(f"GeoJSON saved successfully: {output_geojson}")

    except Exception as e:
        print(f"An error occurred: {e}")


def truncate_to_length(value, max_length=8):
    """
    Truncates a numeric value to ensure its string representation does not exceed a specified length.

    Args:
        value (float): The numeric value to truncate.
        max_length (int): Maximum allowed length of the string representation.

    Returns:
        float: The truncated value.
    """
    str_value = f"{value:.4f}"  # Convert to string with 4 decimal places
    if len(str_value) > max_length:
        # Truncate the string representation
        truncated_str = str_value[:max_length]
        # Handle cases where truncation affects the number format
        try:
            return float(truncated_str.rstrip('0').rstrip('.'))
        except ValueError:
            # If truncation results in an invalid number, round down further
            return float(truncated_str[:-1])  # Remove the last character to fix formatting
    return value


def recursive_round_coordinates(coordinates, precision):
    """
    Recursively rounds the coordinates in a nested list structure.

    Args:
        coordinates (list): A nested list of coordinates.
        precision (int): Number of decimal places to round to.

    Returns:
        list: Rounded coordinates.
    """
    if isinstance(coordinates, (float, int)):
        return round(coordinates, precision)
    elif isinstance(coordinates, list):
        return [recursive_round_coordinates(coord, precision) for coord in coordinates]
    return coordinates


if __name__ == "__main__":
    # Input and output file paths
    input_shapefile= "/home/mahdi/interface/data/output/aligned_results_20250217_093030.shp"  # Replace with your input shapefile path
    output_geojson = "/home/mahdi/interface/data/output/reduced1.geojson"   # Replace with your desired output GeoJSON path

    # Run the function
    reduce_precision(input_shapefile, output_geojson)




