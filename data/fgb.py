import geopandas as gpd
from shapely.geometry import shape, Polygon
from shapely.ops import unary_union
from shapely.wkt import loads
import matplotlib.pyplot as plt

def validate_and_repair_geometries(gdf):
    """
    Validate and repair geometries in a GeoDataFrame.

    Parameters:
        gdf (GeoDataFrame): Input GeoDataFrame.

    Returns:
        GeoDataFrame: GeoDataFrame with repaired geometries.
    """
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.buffer(0) if geom.is_valid else geom)
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

    # Debug: Print details for each geometry
    for idx, row in simplified_gdf.iterrows():
        original_geom = gdf.loc[idx, 'geometry']
        simplified_geom = row['geometry']
        if original_geom and simplified_geom:
            original_vertex_count = len(original_geom.exterior.coords) if hasattr(original_geom, 'exterior') else 0
            simplified_vertex_count = len(simplified_geom.exterior.coords) if hasattr(simplified_geom, 'exterior') else 0
            print(f"Feature {idx}: Original Vertices={original_vertex_count}, Simplified Vertices={simplified_vertex_count}")

    return simplified_gdf


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
        "area_difference": [],
        "max_distance": []
    }

    for idx, row in original_gdf.iterrows():
        original_geom = row['geometry']
        simplified_geom = simplified_gdf.loc[idx, 'geometry']

        if original_geom and simplified_geom:
            # 1. Vertex Count Reduction
            original_vertex_count = len(original_geom.exterior.coords) if hasattr(original_geom, 'exterior') else 0
            simplified_vertex_count = len(simplified_geom.exterior.coords) if hasattr(simplified_geom, 'exterior') else 0
            vertex_reduction = (original_vertex_count - simplified_vertex_count) / original_vertex_count if original_vertex_count > 0 else 0
            metrics["vertex_reduction"].append(vertex_reduction)

            # 2. Area Difference
            if hasattr(original_geom, 'area') and hasattr(simplified_geom, 'area'):
                area_diff = abs(original_geom.area - simplified_geom.area) / original_geom.area if original_geom.area > 0 else 0
                metrics["area_difference"].append(area_diff)

            # 3. Maximum Distance Between Geometries
            max_distance = original_geom.distance(simplified_geom)
            metrics["max_distance"].append(max_distance)

    return metrics


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


def convert_shapefile_to_flatgeobuf(input_shapefile, output_flatgeobuf):
    """
    Converts a shapefile to an optimized FlatGeobuf file and compares simplification results.

    Parameters:
        input_shapefile (str): Path to the input shapefile.
        output_flatgeobuf (str): Path to save the output FlatGeobuf file.
    """
    try:
        print("Reading shapefile...")
        gdf = gpd.read_file(input_shapefile)

        # Step 1: Validate and repair geometries
        print("Validating and repairing geometries...")
        gdf = validate_and_repair_geometries(gdf)

        # Step 2: Ensure CRS is EPSG:2154
        print("Ensuring CRS is EPSG:2154...")
        gdf = gdf.to_crs(epsg=2154)

        # Step 3: Simplify geometries
        print("Simplifying geometries...")
        simplified_gdf = simplify_geometries(gdf, tolerance=10)  # Adjust tolerance as needed

        # Step 4: Compare original and simplified geometries
        print("Comparing original and simplified geometries...")
        metrics = compare_simplified_geometries(gdf, simplified_gdf)

        # Print summary of metrics
        print("\nComparison Metrics:")
        print(f"Average Vertex Reduction: {sum(metrics['vertex_reduction']) / len(metrics['vertex_reduction']):.2%}")
        print(f"Average Area Difference: {sum(metrics['area_difference']) / len(metrics['area_difference']):.2%}")
        print(f"Maximum Distance: {max(metrics['max_distance']):.4f} meters")

        # Step 5: Visualize comparison
        print("Plotting comparison...")
        plot_comparison(gdf, simplified_gdf, output_image="comparison.png")

        # Step 6: Export to FlatGeobuf
        print("Exporting to FlatGeobuf...")
        simplified_gdf.to_file(output_flatgeobuf, driver="FlatGeobuf")

        print(f"Conversion completed. Output saved to {output_flatgeobuf}")

    except Exception as e:
        print(f"An error occurred: {e}")



if __name__ == "__main__":
    
    input_shapefile = "/home/mahdi/interface/data/output/aligned_results_20250217_093030.shp"  # Replace with your shapefile path
    output_flatgeobuf = "/home/mahdi/interface/data/output/simplifytolerance1.fgb"  # Replace with desired output path

    convert_shapefile_to_flatgeobuf(input_shapefile, output_flatgeobuf)