# config.py
import os

def configure_pipeline():
    """
    Configure the pipeline by defining paths and settings.
    Returns:
        dict: Configuration dictionary.
    """
    return {
        "reference_shapefile": "/home/mahdi/interface/data/shapefiles/reference.shp",  # Path to the reference shapefile
        "target_shapefile_pattern": "/home/mahdi/interface/data/output/aligned/aligned_results*.shp",  # Pattern for target shapefiles
        "output_dir": "/home/mahdi/interface/data/output",  # Output directory for processed files
        "postgis_config": {  # PostGIS connection details
            "host": "localhost",
            "port": "5432",
            "dbname": "roofs",
            "user": "mahdi",
            "schema": "public",
            "connection_string": "postgresql://mahdi:mahdi@localhost:5432/roofs"
        },
        "min_area_threshold": 1000,  # Minimum area threshold for merging small polygons
        "max_merge_distance": 50,    # Maximum distance for simplifying reference polygons
        "alignment_config": {  # Alignment-specific configuration
            "max_distance": 25,      # Maximum allowable distance for alignment
            "min_overlap_ratio": 0.5  # Minimum overlap ratio for alignment
        },
        "split_attribute": "nom",     # Attribute to split data by in the splitting process
        "vector_tile_schema": "public"  # Schema for vector tiles in PostGIS
    }