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

def find_nearest_address_in_parcel(roof_idx, roofs_gdf, address_gdf, parcel_gdf, roof_to_parcel_mapping):
    roof = roofs_gdf.loc[roof_idx]
    parcel_id = roof_to_parcel_mapping.get(roof_idx)
    
    try:
        parcel_geom = parcel_gdf.loc[parcel_id, 'geometry']
        addresses_in_parcel = address_gdf[address_gdf.intersects(parcel_geom)]
        candidate_addresses = addresses_in_parcel if not addresses_in_parcel.empty else address_gdf
    except (KeyError, TypeError):
        candidate_addresses = address_gdf
    
    distances = candidate_addresses.geometry.distance(roof.geometry)
    
    if len(distances) > 0:
        closest_idx = distances.idxmin()
        closest_point = candidate_addresses.loc[closest_idx]
        min_distance = distances.min()
    else:
        distances = address_gdf.geometry.distance(roof.geometry)
        closest_idx = distances.idxmin()
        closest_point = address_gdf.loc[closest_idx]
        min_distance = distances.min()
    
    result = {'roof_idx': roof_idx}
    for col in closest_point.index:
        if col != 'geometry' and not col.startswith('index_'):
            result[f'address_{col}'] = closest_point[col]
    result['distance'] = min_distance
    result['parcel_id'] = parcel_id if parcel_id else 'none'
    
    return result

def process_chunk(roof_indices, roofs_gdf, address_gdf, parcel_gdf, roof_to_parcel_mapping):
    results = []
    for idx in roof_indices:
        results.append(find_nearest_address_in_parcel(idx, roofs_gdf, address_gdf, parcel_gdf, roof_to_parcel_mapping))
    return results

if __name__ == "__main__":
    roofs_shp = "/home/mahdi/interface/data/output/divide/roofs_divided_by_parcelles.shp"
    address_shp = "/home/mahdi/interface/data/raw/pq2/adresse.shp"
    parcels_shp = "/home/mahdi/interface/data/raw/pq2/PARCELLE.SHP"
    output_shp = "/home/mahdi/interface/data/output/asign/roofs_with_addresses.shp"
    
    num_processes = max(1, mp.cpu_count() - 1)
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
    
    # Directly map roofs to parcels using the 'nom' attribute
    print("Mapping roofs to parcels via 'nom' attribute...")
    roof_to_parcel_mapping = {}
    for idx, row in roofs_gdf.iterrows():
        parcel_id = row.get('nom', None)  # Replace 'nom' with your actual column name
        if parcel_id is not None:
            # Ensure parcel_id exists in parcels_gdf
            if parcel_id in parcels_gdf.index:
                roof_to_parcel_mapping[idx] = parcel_id
    
    # Validate mapping
    parcel_match_count = len(roof_to_parcel_mapping)
    print(f"Roofs matched to parcels: {parcel_match_count}/{len(roofs_gdf)} ({parcel_match_count/len(roofs_gdf)*100:.1f}%)")
    
    # Precompute address-parcel relationships
    print("Precomputing address-parcel mappings...")
    address_parcel_joined = gpd.sjoin(address_gdf, parcels_gdf[['geometry']], how="left", predicate="within", rsuffix="_parcel")
    print("Columns in address_parcel_joined:", address_parcel_joined.columns)
    address_parcel_mapping = {}
    for parcel_id, group in address_parcel_joined.groupby('parcel_id'):
        address_parcel_mapping[str(parcel_id)] = group.copy()
    
    # Analyze address distribution
    addresses_per_parcel = {k: len(v) for k, v in address_parcel_mapping.items()}
    addresses_per_parcel_series = pd.Series(addresses_per_parcel)
    print(f"Parcels with addresses: {addresses_per_parcel_series[addresses_per_parcel_series > 0].count()}/{len(parcels_gdf)}")
    
    # Multiprocessing setup
    roof_indices = roofs_gdf.index.tolist()
    chunk_size = max(1, len(roof_indices) // (num_processes * 4))
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