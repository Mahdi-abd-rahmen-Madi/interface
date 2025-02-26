import fiona
import os

# Check if the file exists
file_path = "/tmp/tmp57kp4xp9/chunk_0.fgb"
if os.path.exists(file_path):
    print(f"File exists. Size: {os.path.getsize(file_path)} bytes")
    
    # Try opening with explicit driver
    try:
        with fiona.open(file_path, driver="FlatGeobuf") as src:
            print(f"Successfully opened. CRS: {src.crs}")
            print(f"Schema: {src.schema}")
            print(f"Number of features: {len(src)}")
    except Exception as e:
        print(f"Error opening file: {e}")
else:
    print("File does not exist at the specified path")