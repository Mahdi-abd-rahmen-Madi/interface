import dask_geopandas
import geopandas as gpd
from shapely.geometry import GeometryCollection
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import time
from dask.distributed import Client
from dask import delayed, compute

def validate_and_reproject(gdf, target_crs=2154):
    """
    Validates the CRS of a GeoDataFrame and reprojects it if necessary.
    """
    current_crs = gdf.crs.to_epsg() if gdf.crs and gdf.crs.is_projected else None
    if current_crs != target_crs:
        print(f"Reprojecting from {current_crs or 'unknown'} to EPSG:{target_crs}...")
        gdf = gdf.to_crs(epsg=target_crs)
    return gdf


def simplify_geometries(gdf, tolerance=0.2):  # Increased tolerance further for faster simplification
    """
    Simplifies the geometries in a GeoDataFrame.
    """
    print("Simplifying geometries...")
    gdf.geometry = gdf.geometry.simplify(tolerance=tolerance, preserve_topology=False)  # Disable topology preservation
    return gdf


def drop_unnecessary_attributes(parcelles_gdf, communes_gdf):
    """
    Drops unnecessary attributes from the GeoDataFrames.
    """
    print("Dropping unnecessary attributes...")
    if "numero" in parcelles_gdf.columns:
        parcelles_gdf = parcelles_gdf.drop(columns=["numero"], errors="ignore")
    if "feuille" in parcelles_gdf.columns:
        parcelles_gdf = parcelles_gdf.drop(columns=["feuille"], errors="ignore")
    if "wikipedia" in communes_gdf.columns:
        communes_gdf = communes_gdf.drop(columns=["wikipedia"], errors="ignore")
    return parcelles_gdf, communes_gdf


@delayed
def process_intersection_chunk(parcelle_chunk, communes_gdf):
    """
    Processes the intersection for a single chunk of parcels.
    """
    result = gpd.overlay(parcelle_chunk, communes_gdf, how='intersection')
    result = result[~result.geometry.is_empty]
    result.geometry = result.geometry.apply(lambda geom: geom if not isinstance(geom, GeometryCollection) else None)
    result = result[result.geometry.notnull()]
    return result


def divide_parcelles_by_communes(parcelle_path, communes_path, output_path, npartitions=16, simplify_tolerance=0.2, n_workers=8, threads_per_worker=2):
    """
    Divides the PARCELLE.shp file using the communes-20220101.shp boundaries.
    """
    try:
        # Initialize Dask distributed client
        client = Client(n_workers=n_workers, threads_per_worker=threads_per_worker)
        print(f"Dask client initialized with {n_workers} workers and {threads_per_worker} threads per worker.")
        print(f"Dask dashboard: {client.dashboard_link}")  # Print dashboard link for monitoring

        start_time = time.time()

        # Step 1: Load the shapefiles
        print("Loading shapefiles...")
        load_start = time.time()
        parcelles = gpd.read_file(parcelle_path)
        communes = gpd.read_file(communes_path)
        load_end = time.time()
        print(f"Shapefile loading completed in {load_end - load_start:.2f} seconds.")

        # Step 2: Validate and reproject CRS
        print("Validating and reprojecting CRS if necessary...")
        reprojection_start = time.time()
        parcelles = validate_and_reproject(parcelles)
        communes = validate_and_reproject(communes)
        reprojection_end = time.time()
        print(f"CRS reprojection completed in {reprojection_end - reprojection_start:.2f} seconds.")

        # Step 3: Simplify geometries
        print("Simplifying geometries...")
        simplify_start = time.time()
        parcelles = simplify_geometries(parcelles, tolerance=simplify_tolerance)
        communes = simplify_geometries(communes, tolerance=simplify_tolerance)
        simplify_end = time.time()
        print(f"Geometry simplification completed in {simplify_end - simplify_start:.2f} seconds.")

        # Step 4: Drop unnecessary attributes
        attribute_drop_start = time.time()
        parcelles, communes = drop_unnecessary_attributes(parcelles, communes)
        attribute_drop_end = time.time()
        print(f"Attribute dropping completed in {attribute_drop_end - attribute_drop_start:.2f} seconds.")

        # Step 5: Create spatial indexes
        print("Creating spatial indexes...")
        parcelles.sindex
        communes.sindex

        # Step 6: Convert to Dask-GeoDataFrames
        print("Converting to Dask-GeoDataFrames...")
        dask_conversion_start = time.time()
        dask_parcelles = dask_geopandas.from_geopandas(parcelles, npartitions=npartitions)
        communes_gdf = communes
        dask_conversion_end = time.time()
        print(f"Dask conversion completed in {dask_conversion_end - dask_conversion_start:.2f} seconds.")

        # Step 7: Perform intersection using delayed computations
        print("Performing intersection using chunk-based processing...")
        intersection_start = time.time()
        results = []
        for partition in tqdm(dask_parcelles.partitions, desc="Processing partitions"):
            parcelle_chunk = partition.compute()
            result = process_intersection_chunk(parcelle_chunk, communes_gdf)
            results.append(result)

        # Compute all delayed results at once
        results = compute(*results)
        intersection_end = time.time()
        print(f"Intersection completed in {intersection_end - intersection_start:.2f} seconds.")

        # Step 8: Combine the results
        print("Combining results...")
        combine_start = time.time()
        result = pd.concat(results, ignore_index=True)
        combine_end = time.time()
        print(f"Result combination completed in {combine_end - combine_start:.2f} seconds.")

        # Step 9: Save the result to a new shapefile
        output_dir = Path(output_path).parent
        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)

        print(f"Saving result to {output_path}...")
        save_start = time.time()
        result.to_file(output_path, driver="ESRI Shapefile")  # Output as Shapefile
        save_end = time.time()
        print(f"File saving completed in {save_end - save_start:.2f} seconds.")

        end_time = time.time()
        print(f"Total execution time: {end_time - start_time:.2f} seconds.")

        # Close the Dask client
        client.close()
        print("Dask client closed.")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    parcelle_path = "/home/mahdi/interface/data/shapefiles/pq2/PARCELLE.SHP"
    communes_path = "/home/mahdi/interface/data/shapefiles/pq2/communes-20220101.shp"
    output_path = "/home/mahdi/interface/data/shapefiles/pq2/divided_parcelles_dask4.shp"  # Output as Shapefile

    # Run the division process with Dask distributed scheduler
    divide_parcelles_by_communes(
        parcelle_path,
        communes_path,
        output_path,
        npartitions=16,  # Reduced number of partitions
        simplify_tolerance=0.2,  # Geometry simplification tolerance
        n_workers=8,  # Number of Dask workers (CPU cores)
        threads_per_worker=2  # Threads per worker
    )