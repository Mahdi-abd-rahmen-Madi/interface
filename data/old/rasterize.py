import subprocess

rasterize_cmd = [
    "gdal_rasterize",
    "-ot", "Byte",                 # Convert to 8-bit 
    "-burn", "1",                   # Burn a constant value (modify as needed)
    "-tr", "0.1", "0.1",            # High resolution: 0.1x0.1 units
    "-of", "GTiff",                 # Output format
    "roof.shp",                     # Input vector file
    "roof1.tif"                      # Output raster file
]

print("Rasterizing the vector at high resolution...")
subprocess.run(rasterize_cmd, check=True)
print("Rasterization complete.")