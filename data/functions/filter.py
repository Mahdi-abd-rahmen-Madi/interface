import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon

def filter_polygons(input_shapefile, output_shapefile):
    """
    Reads a shapefile, removes non-polygon geometries, and exports only polygons.

    Parameters:
        input_shapefile (str): Path to the input shapefile.
        output_shapefile (str): Path to save the filtered shapefile with only polygons.
    """
    try:
        # Load the shapefile into a GeoDataFrame
        gdf = gpd.read_file(input_shapefile)
        
        # Check if the GeoDataFrame has a geometry column
        if 'geometry' not in gdf.columns:
            raise ValueError("The input shapefile does not contain a 'geometry' column.")
        
        # Filter rows where the geometry is either a Polygon or MultiPolygon
        polygon_gdf = gdf[gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])]
        
        # Check if any polygons were found
        if polygon_gdf.empty:
            print("No polygon geometries found in the input shapefile.")
            return
        
        # Export the filtered GeoDataFrame to a new shapefile
        polygon_gdf.to_file(output_shapefile)
        print(f"Filtered shapefile with only polygons has been saved to {output_shapefile}")
    
    except FileNotFoundError:
        print(f"Error: The file '{input_shapefile}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
if __name__ == "__main__":
    input_path = "/home/mahdi/interface/data/raw/pq2/TOITS_PQ2_filtered.shp"  # Replace with your input shapefile path
    output_path = "/home/mahdi/interface/data/output/divide/filtered_roofs/filtered.shp"  # Replace with desired output path
    
    filter_polygons(input_path, output_path)