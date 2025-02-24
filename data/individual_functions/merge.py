import geopandas as gpd
from shapely.geometry import Polygon
from shapely.ops import unary_union
import os

def merge_adjacent_small_polygons(input_shapefile, output_dir, min_area=1000):
    """
    Merges adjacent polygons that are smaller than the specified minimum area.
    Args:
        input_shapefile (str): Path to the input shapefile.
        output_dir (str): Directory where the output shapefile will be saved.
        min_area (float): Minimum area threshold in square meters. Default is 1000.
    Returns:
        None: Saves the merged polygons to a new shapefile in the output directory.
    """
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Read the input shapefile into a GeoDataFrame
    gdf = gpd.read_file(input_shapefile)

    # Separate polygons into small and large based on the area threshold
    small_polygons = [geom for geom in gdf.geometry if geom.area < min_area]
    large_polygons = [geom for geom in gdf.geometry if geom.area >= min_area]

    # Merge all small polygons into a single geometry using unary_union
    if small_polygons:
        small_merged = unary_union(small_polygons)
        
        # If the merged result is a MultiPolygon, split it back into individual polygons
        if small_merged.geom_type == 'MultiPolygon':
            small_polygons = list(small_merged.geoms)
        else:
            small_polygons = [small_merged]
    else:
        small_polygons = []

    # Combine the large polygons and the merged small polygons
    result_polygons = large_polygons + small_polygons

    # Create a new GeoDataFrame for the result
    result_gdf = gpd.GeoDataFrame(geometry=result_polygons, crs=gdf.crs)

    # Save the result to a new shapefile in the output directory
    output_path = os.path.join(output_dir, "merged_polygons.shp")
    result_gdf.to_file(output_path)

    print(f"Merged polygons saved to {output_path}")

# Example usage:
if __name__ == "__main__":
    reference_shapefile_path = "/home/mahdi/interface/data/shapefiles/reference.shp"
    output_dir = "/home/mahdi/interface/data/output"

    # Merge adjacent small polygons (< 1000 sqm) and save the result
    merge_adjacent_small_polygons(reference_shapefile_path, output_dir, min_area=100)