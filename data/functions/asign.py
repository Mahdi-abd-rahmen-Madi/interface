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
    """
    Find the nearest address point for a roof within its parcel's boundary
    """
    roof = roofs_gdf.loc[roof_idx]
    
    # Get the parcel ID for this roof
    parcel_id = roof_to_parcel_mapping.get(roof_idx)
    
    if parcel_id is not None and pd.notna(parcel_id):
        try:
            parcel_geom = parcel_gdf.loc[parcel_id, 'geometry']
            
            # Filter addresses to only those within this parcel
            addresses_in_parcel = address_gdf[address_gdf.intersects(parcel_geom)]
            
            # If we found addresses in this parcel, use them. Otherwise, fall back to all addresses
            if len(addresses_in_parcel) > 0:
                candidate_addresses = addresses_in_parcel
            else:
                candidate_addresses = address_gdf
        except (KeyError, TypeError):
            # Fallback if the parcel ID doesn't exist in the parcel GDF
            candidate_addresses = address_gdf
    else:
        # No parcel mapping, use all addresses
        candidate_addresses = address_gdf
    
    # Calculate distances to candidate address points
    distances = candidate_addresses.geometry.distance(roof.geometry)
    
    # Get the closest point
    if len(distances) > 0:
        closest_idx = distances.idxmin()
        closest_point = candidate_addresses.loc[closest_idx]
        min_distance = distances.min()
    else:
        # Fallback if no candidate addresses (shouldn't happen but just in case)
        distances = address_gdf.geometry.distance(roof.geometry)
        closest_idx = distances.idxmin()
        closest_point = address_gdf.loc[closest_idx]
        min_distance = distances.min()
    
    # Prepare result dictionary with the roof index and address attributes
    result = {'roof_idx': roof_idx}
    
    # Add address attributes
    for col in closest_point.index:
        if col != 'geometry' and not col.startswith('index_'):
            result[f'address_{col}'] = closest_point[col]
    
    # Add distance for reference
    result['distance'] = min_distance
    result['parcel_id'] = parcel_id if parcel_id is not None else 'none'
    
    return result

def process_chunk(roof_indices, roofs_gdf, address_gdf, parcel_gdf, roof_to_parcel_mapping):
    """Process a chunk of roof indices to find nearest addresses within parcel"""
    results = []
    for idx in roof_indices:
        results.append(find_nearest_address_in_parcel(idx, roofs_gdf, address_gdf, parcel_gdf, roof_to_parcel_mapping))
    return results

if __name__ == "__main__":
    # Define input and output file paths
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
    print(f"Roofs count: {len(roofs_gdf)}, Address points count: {len(address_gdf)}, Parcels count: {len(parcels_gdf)}")
    
    # Check and transform CRS if needed
    crs_start_time = time.time()
    
    print(f"Roofs CRS: {roofs_gdf.crs}")
    print(f"Address CRS: {address_gdf.crs}")
    print(f"Parcels CRS: {parcels_gdf.crs}")
    
    # Make sure all datasets are in EPSG:2154
    if roofs_gdf.crs != "EPSG:2154":
        print("Converting roofs to EPSG:2154...")
        roofs_gdf = roofs_gdf.to_crs("EPSG:2154")
        
    if address_gdf.crs != "EPSG:2154":
        print("Converting addresses to EPSG:2154...")
        address_gdf = address_gdf.to_crs("EPSG:2154")
    
    if parcels_gdf.crs != "EPSG:2154":
        print("Converting parcels to EPSG:2154...")
        parcels_gdf = parcels_gdf.to_crs("EPSG:2154")
    
    crs_time = time.time() - crs_start_time
    print(f"CRS transformations completed in {format_time(crs_time)}")
    
    # Prepare for spatial joins
    parcel_prep_start_time = time.time()
     
    # Ensure parcels have a unique ID
    if 'parcel_id' not in parcels_gdf.columns:
        parcels_gdf['parcel_id'] = parcels_gdf.index.astype(str)
    
    # Set index to parcel_id for easier lookup
    parcels_gdf = parcels_gdf.set_index('parcel_id', drop=True)  # ✅ Drop the column
    
    # Join roofs to parcels to establish which roof belongs to which parcel
    print("Joining roofs to parcels...")
    roofs_to_parcels = gpd.sjoin(
        roofs_gdf,
        parcels_gdf.reset_index(),  # Reset index to avoid naming conflict
        how="left",
        predicate="within",
        rsuffix="_right"  # Add suffix to resolve column name conflicts
    )
    
    # Create a mapping from roof index to parcel ID
    roof_to_parcel_mapping = {}
    for idx, row in roofs_to_parcels.iterrows():
        if 'parcel_id_right' in row and pd.notna(row['parcel_id_right']):
            roof_to_parcel_mapping[idx] = row['parcel_id_right']
    
    # Check how many roofs got assigned to parcels
    parcel_match_count = len(roof_to_parcel_mapping)
    print(f"Roofs matched to parcels: {parcel_match_count}/{len(roofs_gdf)} ({parcel_match_count/len(roofs_gdf)*100:.1f}%)")
    
    # Count addresses per parcel for information
    print("Analyzing address distribution within parcels...")
    addresses_per_parcel = {}
    for parcel_id in parcels_gdf.index:
        parcel_geom = parcels_gdf.loc[parcel_id, 'geometry']
        addresses_in_parcel = address_gdf[address_gdf.intersects(parcel_geom)]
        addresses_per_parcel[parcel_id] = len(addresses_in_parcel)
    
    # Convert to Series for easier analysis
    addresses_per_parcel_series = pd.Series(addresses_per_parcel)
    parcels_with_addresses = addresses_per_parcel_series[addresses_per_parcel_series > 0].count()
    
    print(f"Parcels with at least one address: {parcels_with_addresses}/{len(parcels_gdf)} ({parcels_with_addresses/len(parcels_gdf)*100:.1f}%)")
    if parcels_with_addresses > 0:
        print(f"Average addresses per parcel (excluding empty parcels): {addresses_per_parcel_series[addresses_per_parcel_series > 0].mean():.2f}")
        print(f"Max addresses in a single parcel: {addresses_per_parcel_series.max()}")
    
    parcel_prep_time = time.time() - parcel_prep_start_time
    print(f"Parcel and address analysis completed in {format_time(parcel_prep_time)}")
    
    # Use multiprocessing to find the nearest address for each roof
    processing_start_time = time.time()
    
    # Get all roof indices
    roof_indices = roofs_gdf.index.tolist()
    
    # Split indices into chunks for multiprocessing
    chunk_size = max(1, len(roof_indices) // (num_processes * 4))  # Create more chunks than processes
    chunks = [roof_indices[i:i + chunk_size] for i in range(0, len(roof_indices), chunk_size)]
    
    print(f"Processing {len(roof_indices)} roofs in {len(chunks)} chunks using {num_processes} processes")
    
    # Create a pool of processes
    with mp.Pool(processes=num_processes) as pool:
        # Create a partial function with the dataframes
        chunk_processor = partial(
            process_chunk,
            roofs_gdf=roofs_gdf,
            address_gdf=address_gdf,
            parcel_gdf=parcels_gdf,
            roof_to_parcel_mapping=roof_to_parcel_mapping
        )
        
        # Process chunks in parallel and collect results
        results = []
        for i, chunk_result in enumerate(pool.imap_unordered(chunk_processor, chunks)):
            results.extend(chunk_result)
            if (i+1) % max(1, len(chunks) // 10) == 0:  # Update every ~10%
                print(f"Progress: {i+1}/{len(chunks)} chunks processed ({(i+1)/len(chunks)*100:.1f}%)")
    
    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
    
    # Count roofs that used addresses within their parcel vs. fallback
    parcel_matches = results_df[results_df['parcel_id'] != 'none']
    print(f"Roofs that used addresses within their parcel: {len(parcel_matches)}/{len(results_df)} ({len(parcel_matches)/len(results_df)*100:.1f}%)")
    
    processing_time = time.time() - processing_start_time
    print(f"Address assignment processing completed in {format_time(processing_time)}")
    
    # Merge results back to the original roofs GeoDataFrame
    merge_start_time = time.time()
    
    # Create a new GeoDataFrame to store the results
    output_gdf = roofs_gdf.copy()
    
    # Add address attributes from results
    for _, row in results_df.iterrows():
        roof_idx = row['roof_idx']
        for col in row.index:
            if col.startswith('address_'):
                orig_col = col.replace('address_', '')
                output_gdf.at[roof_idx, orig_col] = row[col]
        
        output_gdf.at[roof_idx, 'dist_to_addr'] = row['distance']
        output_gdf.at[roof_idx, 'parcel_id'] = row['parcel_id']
    
    merge_time = time.time() - merge_start_time
    print(f"Results merged in {format_time(merge_time)}")
    
    # Save the result
    save_start_time = time.time()
    
    print(f"Saving output to {output_shp}...")
    output_gdf.to_file(output_shp)
    
    save_time = time.time() - save_start_time
    print(f"Output saved in {format_time(save_time)}")
     
    # Calculate and display total execution time
    total_time = time.time() - total_start_time
    print("\n--- Execution Summary ---")
    print(f"Total execution time: {format_time(total_time)}")
    print(f"Reading files: {format_time(read_time)} ({read_time/total_time*100:.1f}%)")
    print(f"CRS transformations: {format_time(crs_time)} ({crs_time/total_time*100:.1f}%)")
    print(f"Parcel preparation: {format_time(parcel_prep_time)} ({parcel_prep_time/total_time*100:.1f}%)")
    print(f"Address assignment: {format_time(processing_time)} ({processing_time/total_time*100:.1f}%)")
    print(f"Results merging: {format_time(merge_time)} ({merge_time/total_time*100:.1f}%)")
    print(f"Saving output: {format_time(save_time)} ({save_time/total_time*100:.1f}%)")
    print("Process completed successfully!")