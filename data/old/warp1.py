#optimized for polygone data

import numpy as np
from scipy.interpolate import Rbf

# Ground control points:

# These are the original coordinates:
src_x = np.array([900086,900092,900002.5,900054,900054,900101.5,900113.5,900152,900182,900002.5,900009,900359,900119.5,900156,900047.5,900050,900017,900026,900035,900029,900392.5,900394.5,900457,900471.5,900466,900470,900753.5,900835,900846,900918.5,900924,900963,900962])
src_y = np.array([6271006,6271005.5,6271003.5,6271092,6271095.5,6271182.5,6271181,6271204,6271200.5,6271310,6271234.5,6271307,6271696.5,6271692.5,6271852.5,6271852.5,6271911,6271906.5,6271901,6271902.5,6271807,6271808.5,6271779.5,6271778,6271774.5,6271770,6271934,6271948,6271917,6271502.5,6271509.5,6271517,6271508])

# These are the desired (warped) coordinates:

dst_x = np.array([900086,900092,900002.5,900054,900054,900101.5,900113.5,900152,900182,900002.5,900009,900359,900120.1839,900156.6839,900048.1839,900050.6839,900017.6839,900026.6839,900035.6839,900029.6839,900393.1839,900395.1839,900457.6839,900472.1839,900466.6839,900470.6839,900754.1839,900835.6839,900846.6839,900919.1839,900924.6839,900963.6839,900962.6839])
dst_y = np.array([6271006,6271005.5,6271003.5,6271092,6271095.5,6271182.5,6271181,6271204,6271200.5,6271310,6271234.5,6271307,6271691.318,6271687.318,6271847.318,6271847.318,6271905.818,6271901.318,6271895.818,6271897.318,6271801.818,6271803.318,6271774.318,6271772.818,6271769.318,6271764.818,6271928.818,6271942.818,6271911.818,6271497.318,6271504.318,6271511.818,6271502.818])


# Build the TPS (thin-plate spline) transformation functions
rbf_x = Rbf(src_x, src_y, dst_x, function='thin_plate')
rbf_y = Rbf(src_x, src_y, dst_y, function='thin_plate')

def rubber_sheet_transform(x, y):
    """Applies the TPS transformation to the coordinate (x,y)"""
    return float(rbf_x(x, y)), float(rbf_y(x, y))


from osgeo import ogr
ogr.UseExceptions()  # Make sure to enable exceptions if desired

def transform_polygon(geom):
    """
    Transforms a polygon geometry using the rubber sheet transformation.
    """
    new_poly = ogr.Geometry(ogr.wkbPolygon)
    
    # Loop through each ring in the polygon
    for ring_idx in range(geom.GetGeometryCount()):
        ring = geom.GetGeometryRef(ring_idx)
        new_ring = ogr.Geometry(ogr.wkbLinearRing)
        num_points = ring.GetPointCount()
        
        for pt_idx in range(num_points):
            x, y, *rest = ring.GetPoint(pt_idx)
            new_x, new_y = rubber_sheet_transform(x, y)  # Assuming this function is defined
            print(f"Ring {ring_idx} - Point {pt_idx}: ({x}, {y}) -> ({new_x}, {new_y})")
            new_ring.AddPoint(float(new_x), float(new_y))
        
        # Ensure the ring is closed if needed:
        if new_ring.GetPointCount() > 0:
            first_pt = new_ring.GetPoint(0)
            last_pt = new_ring.GetPoint(new_ring.GetPointCount() - 1)
            if (abs(first_pt[0] - last_pt[0]) > 1e-6 or
                abs(first_pt[1] - last_pt[1]) > 1e-6):
                new_ring.AddPoint(first_pt[0], first_pt[1])
        
        new_poly.AddGeometry(new_ring)
    return new_poly


input_file = "roof.shp"   # Your original shapefile
output_file = "Proof.shp" # The warped output shapefile

# Open the input data source
in_ds = ogr.Open(input_file)
in_layer = in_ds.GetLayer()

# Create the output data source
driver = ogr.GetDriverByName("ESRI Shapefile")
out_ds = driver.CreateDataSource(output_file)
srs = in_layer.GetSpatialRef()
out_layer = out_ds.CreateLayer("Proof", srs, in_layer.GetGeomType())

# Copy fields from the input layer
in_layer_defn = in_layer.GetLayerDefn()
for i in range(in_layer_defn.GetFieldCount()):
    field_defn = in_layer_defn.GetFieldDefn(i)
    out_layer.CreateField(field_defn)

# Process each feature (polygons only)
for in_feat in in_layer:
    geom = in_feat.GetGeometryRef()
    if geom is not None:
        # Use the polygon-specific transform function
        new_geom = transform_polygon(geom)
    else:
        new_geom = None

    out_feat = ogr.Feature(out_layer.GetLayerDefn())
    if new_geom is not None:
        out_feat.SetGeometry(new_geom)

    # Copy attribute fields
    for i in range(in_layer_defn.GetFieldCount()):
        field_name = in_layer_defn.GetFieldDefn(i).GetNameRef()
        out_feat.SetField(field_name, in_feat.GetField(i))

    out_layer.CreateFeature(out_feat)
    out_feat = None

# Cleanup
in_ds = None
out_ds = None


