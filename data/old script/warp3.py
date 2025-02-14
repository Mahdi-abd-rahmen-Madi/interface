import subprocess

# Step 1: Rasterize the shapefile
rasterize_cmd = [
    'gdal_rasterize',
    '-burn', '1',         # or use -a <attribute> if needed
    '-tr', '10', '10',
    '-ot', 'Byte',
    '-of', 'GTiff',
    'roof.shp',
    'roof.tif'
]
subprocess.run(rasterize_cmd, check=True)

# (Step 2 would be handled either manually or by a separate script to add GCPs)
# For demonstration, we assume GCPs are added via gdal_edit.py or a pre-prepared VRT.

# Step 3: Warp the raster using TPS
warp_cmd = [
    'gdalwarp',
    '-tps',
    '-r', 'bilinear',
    '-of', 'GTiff',
    'roof.tif',
    'warped_roof.tif'
]
subprocess.run(warp_cmd, check=True)

# Step 4: (Optional) Vectorize the warped raster
polygonize_cmd = [
    'gdal_polygonize.py',
    'warped_roof.tif',
    '-f', 'ESRI Shapefile',
    'warped_roof.shp'
]
subprocess.run(polygonize_cmd, check=True)
