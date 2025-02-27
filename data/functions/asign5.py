import geopandas as gpd
import os
import time
from datetime import timedelta
import multiprocessing as mp
from functools import partial
import numpy as np
import pandas as pd

def format_time(seconds):
    """Format seconds into a readable time string"""
    return str(timedelta(seconds=seconds))

def validate_geometries(gdf, name):
    """
    Validate geometries in a GeoDataFrame and attempt to fix invalid ones.
    """
    invalid = gdf[~gdf.is_valid]
    if not invalid.empty:
        print(f"Found {len(invalid)} invalid geometries in {name}. Attempting to fix...")
        gdf['geometry'] = gdf.buffer(0)  # Attempt to fix invalid geometries
        remaining_invalid = gdf[~gdf.is_valid]
        if not remaining_invalid.empty:
            print(f"{len(remaining_invalid)} geometries remain invalid after fixing.")
        else:
            print("All geometries have been successfully fixed.")

def find_nearest_address(roof_idx, roofs_gdf, address_gdf, address_sindex):
    """
    Find the nearest address for a given roof using the spatial index.
    Handles cases where all distance computations result in NaN.
    """
    roof = roofs_gdf.loc[roof_idx]
    
    # Use the roof's bounding box to query candidate addresses
    possible_matches_index = list(address_sindex.intersection(roof.geometry.bounds))
    candidate_addresses = address_gdf.iloc[possible_matches_index]
    
    # If no candidates are found, fall back to the entire address dataset
    if candidate_addresses.empty:
        candidate_addresses = address_gdf

    # Compute distances to the roof
    distances = candidate_addresses.geometry.distance(roof.geometry)
    
    # Handle case where all distances are NaN
    if distances.isnull().all():
        return {
            'roof_idx': roof_idx,
            'distance': None,  # Indicate missing data
            'error': 'No valid distance computed'
        }
    
    # Find the closest address (ignoring NaNs)
    closest_idx = distances.idxmin(skipna=True)
    closest_point = candidate_addresses.loc[closest_idx]
    min_distance = distances.loc[closest_idx]
    
    # Build result
    result = {'roof_idx': roof_idx}
    for col in closest_point.index:
        if col != 'geometry' and not col.startswith('index_'):
            result[f'address_{col}'] = closest_point[col]
    result['distance'] = min_distance
    
    return result

def process_chunk(roof_indices, roofs_gdf, address_gdf):
    """
    Process a chunk of roof indices and find the nearest address for each roof.
    Rebuilds the spatial index within each worker.
    """
    results = []
    address_sindex = address_gdf.sindex  # Rebuild spatial index in each worker
    
    for count, idx in enumerate(roof_indices):
        if count % 1000 == 0:  # Add verbose logging
            print(f"Worker processing roof {count}/{len(roof_indices)}")
        try:
            results.append(find_nearest_address(idx, roofs_gdf, address_gdf, address_sindex))
        except Exception as e:
            print(f"Error processing roof {idx}: {e}")
            results.append({'roof_idx': idx, 'distance': None, 'error': str(e)})  # Append a placeholder for problematic roofs
    
    return results

if __name__ == "__main__":
    # Input/output file paths
    roofs_shp = "/home/mahdi/interface/data/output/divide/roofs_divided_by_parcelles.shp"
    address_shp = "/home/mahdi/interface/data/raw/pq2/adresse.shp"
    output_shp = "/home/mahdi/interface/data/output/asign/roofs_with_addresses.shp"
    
    # Multiprocessing setup
    num_processes = max(1, mp.cpu_count() // 2)
    total_start_time = time.time()
    
    print(f"Starting processing with {num_processes} parallel processes")
    print("Using nearest neighbor approach (no parcels)")
    
    # Read shapefiles
    print("Reading input shapefiles...")
    read_start_time = time.time()
    
    roofs_gdf = gpd.read_file(roofs_shp)
    address_gdf = gpd.read_file(address_shp)
    
    read_time = time.time() - read_start_time
    print(f"Read shapefiles in {format_time(read_time)}")
    print(f"Roofs count: {len(roofs_gdf)}, Addresses: {len(address_gdf)}")
    
    # CRS normalization
    target_crs = "EPSG:2154"
    for layer, name in zip([roofs_gdf, address_gdf], ["Roofs", "Addresses"]):
        if layer.crs != target_crs:
            print(f"Converting {name} to {target_crs}...")
            layer.to_crs(target_crs, inplace=True)
    
    # Validate geometries
    print("Validating geometries...")
    validate_geometries(roofs_gdf, "Roofs")
    validate_geometries(address_gdf, "Addresses")
    
    # Optimize GeoDataFrames by dropping unnecessary columns
    print("Optimizing GeoDataFrames...")
    address_gdf = address_gdf[[col for col in address_gdf.columns if col != 'index_right']]
    roofs_gdf = roofs_gdf[[col for col in roofs_gdf.columns if col != 'index_right']]
    
    # Multiprocessing setup
    roof_indices = roofs_gdf.index.tolist()
    chunk_size = max(1, len(roof_indices) // (num_processes * 16))  # Reduce chunk size further
    chunks = [roof_indices[i:i + chunk_size] for i in range(0, len(roof_indices), chunk_size)]
    
    print(f"Processing {len(roof_indices)} roofs in {len(chunks)} chunks using {num_processes} processes")
    
    with mp.Pool(processes=num_processes) as pool:
        chunk_processor = partial(
            process_chunk,
            roofs_gdf=roofs_gdf,
            address_gdf=address_gdf
        )
        
        results = []
        for i, chunk_result in enumerate(pool.imap_unordered(chunk_processor, chunks)):
            results.extend(chunk_result)
            if (i+1) % max(1, len(chunks) // 10) == 0:
                print(f"Progress: {i+1}/{len(chunks)} chunks processed ({(i+1)/len(chunks)*100:.1f}%)")
    
    # Merge results
    results_df = pd.DataFrame(results)
    output_gdf = roofs_gdf.copy()
    
    for _, row in results_df.iterrows():
        roof_idx = row['roof_idx']
        for col in row.index:
            if col.startswith('address_'):
                orig_col = col.replace('address_', '')
                output_gdf.at[roof_idx, orig_col] = row[col]
        output_gdf.at[roof_idx, 'dist_to_addr'] = row.get('distance', None)
        if 'error' in row:
            output_gdf.at[roof_idx, 'error'] = row['error']  # Store error message if present
    
    # Save output
    print(f"Saving output to {output_shp}...")
    output_gdf.to_file(output_shp)
    
    # Execution summary
    total_time = time.time() - total_start_time
    print("\n--- Execution Summary ---")
    print(f"Total execution time: {format_time(total_time)}")
    print(f"Reading files: {format_time(read_time)} ({read_time/total_time*100:.1f}%)")
    print(f"Processing: {format_time(total_time - read_time)}")
    print("Process completed successfully!")