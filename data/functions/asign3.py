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

def find_nearest_address_in_parcel(roof_idx, roofs_gdf, address_gdf, parcel_gdf, roof_to_parcel_mapping, address_sindex):
    """
    Find the nearest address for a given roof within its associated parcel.
    If no parcel or addresses are found, return None for the address details.
    """
    roof = roofs_gdf.loc[roof_idx]
    parcel_id = roof_to_parcel_mapping.get(roof_idx)
    
    if not parcel_id or parcel_id not in parcel_gdf.index:
        # No valid parcel association; skip global search
        return {
            'roof_idx': roof_idx,
            'distance': None,
            'parcel_id': 'none'
        }
    
    # Use parcel geometry to filter addresses
    parcel_geom = parcel_gdf.loc[parcel_id, 'geometry']
    possible_matches_index = list(address_sindex.intersection(parcel_geom.bounds))
    addresses_in_parcel = address_gdf.iloc[possible_matches_index]
    addresses_in_parcel = addresses_in_parcel[addresses_in_parcel.intersects(parcel_geom)]
    
    if addresses_in_parcel.empty:
        # No addresses found in the parcel
        return {
            'roof_idx': roof_idx,
            'distance': None,
            'parcel_id': parcel_id
        }
    
    # Compute distances for remaining candidates
    distances = addresses_in_parcel.geometry.distance(roof.geometry)
    closest_idx = distances.idxmin()
    closest_point = addresses_in_parcel.loc[closest_idx]
    min_distance = distances.min()
    
    result = {'roof_idx': roof_idx}
    for col in closest_point.index:
        if col != 'geometry' and not col.startswith('index_'):
            result[f'address_{col}'] = closest_point[col]
    result['distance'] = min_distance
    result['parcel_id'] = parcel_id
    
    return result

def process_chunk(roof_indices, roofs_gdf, address_gdf, parcel_gdf, roof_to_parcel_mapping):
    """
    Process a chunk of roof indices and find the nearest address for each roof.
    Rebuilds the spatial index within each worker to reduce memory duplication.
    """
    results = []
    address_sindex = address_gdf.sindex  # Rebuild spatial index in each worker
    
    for idx in roof_indices:
        results.append(find_nearest_address_in_parcel(idx, roofs_gdf, address_gdf, parcel_gdf, roof_to_parcel_mapping, address_sindex))
    
    return results

if __name__ == "__main__":
    # Input/output file paths
    roofs_shp = "/home/mahdi/interface/data/output/divide/roofs_divided_by_parcelles.shp"
    address_shp = "/home/mahdi/interface/data/raw/pq2/adresse.shp"
    parcels_shp = "/home/mahdi/interface/data/raw/pq2/PARCELLE.SHP"
    output_shp = "/home/mahdi/interface/data/output/asign/roofs_with_addresses.shp"
    
    # Multiprocessing setup
    num_processes = max(1, mp.cpu_count() // 2)  # Reduce number of processes to avoid excessive memory usage
    total_start_time = time.time()
    
    print(f"Starting processing with {num_processes} parallel processes")
    print("Using nearest neighbor within parcel boundaries approach")
    
    # Read shapefiles
    print("Reading input shapefiles...")
    read_start_time = time.time()
    
    roofs_gdf = gpd.read_file(roofs_shp)
    address_gdf = gpd.read_file(address_shp)
    parcels_gdf = gpd.read_file(parcels_shp)
    
    read_time = time.time() - read_start_time
    print(f"Read shapefiles in {format_time(read_time)}")
    print(f"Roofs count: {len(roofs_gdf)}, Addresses: {len(address_gdf)}, Parcels: {len(parcels_gdf)}")
    
    # CRS normalization
    target_crs = "EPSG:2154"
    for layer, name in zip([roofs_gdf, address_gdf, parcels_gdf], ["Roofs", "Addresses", "Parcels"]):
        if layer.crs != target_crs:
            print(f"Converting {name} to {target_crs}...")
            layer.to_crs(target_crs, inplace=True)
    
    # Prepare parcels (ensure unique ID and correct index)
    print("Preparing parcels...")
    if 'parcel_id' not in parcels_gdf.columns:
        parcels_gdf['parcel_id'] = parcels_gdf.index.astype(str)
    parcels_gdf = parcels_gdf.set_index('parcel_id', drop=True)  # Drop column to avoid conflicts
    
    # Map roofs to parcels using spatial join (replace attribute-based mapping)
    print("Mapping roofs to parcels via spatial join...")
    roofs_with_parcel = gpd.sjoin(roofs_gdf, parcels_gdf[['geometry']], how="left", predicate="intersects")
    roof_to_parcel_mapping = dict(zip(roofs_with_parcel.index, roofs_with_parcel['parcel_id']))
    
    parcel_match_count = len([v for v in roof_to_parcel_mapping.values() if v is not None])
    print(f"Roofs matched to parcels: {parcel_match_count}/{len(roofs_gdf)} ({parcel_match_count/len(roofs_gdf)*100:.1f}%)")
    
    # Optimize GeoDataFrames by dropping unnecessary columns
    print("Optimizing GeoDataFrames...")
    address_gdf = address_gdf[[col for col in address_gdf.columns if col != 'index_right']]  # Drop unused columns
    roofs_gdf = roofs_gdf[[col for col in roofs_gdf.columns if col != 'index_right']]
    parcels_gdf = parcels_gdf[[col for col in parcels_gdf.columns if col != 'index_right']]
    
    # Multiprocessing setup
    roof_indices = roofs_gdf.index.tolist()
    chunk_size = max(1, len(roof_indices) // (num_processes * 8))  # Smaller chunks to reduce memory pressure
    chunks = [roof_indices[i:i + chunk_size] for i in range(0, len(roof_indices), chunk_size)]
    
    print(f"Processing {len(roof_indices)} roofs in {len(chunks)} chunks using {num_processes} processes")
    
    with mp.Pool(processes=num_processes) as pool:
        chunk_processor = partial(
            process_chunk,
            roofs_gdf=roofs_gdf,
            address_gdf=address_gdf,
            parcel_gdf=parcels_gdf,
            roof_to_parcel_mapping=roof_to_parcel_mapping
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
        output_gdf.at[roof_idx, 'dist_to_addr'] = row['distance']
        output_gdf.at[roof_idx, 'parcel_id'] = row['parcel_id']
    
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