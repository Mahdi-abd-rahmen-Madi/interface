import numpy as np

# Example control points (you’ll need to supply your own values)
# These are the original coordinates:
src_x = np.array([900015.500, 900755.000, 900335.500, 900369.500, 900962.500, 900906.000, 900753.500, 900748.000, 900015.500, 900025.000, 900406.000])
src_y = np.array([6271008.000, 6271830.500, 6271961.000, 6271968.000, 6271518.500, 6271507.500, 6271619.500, 6271934.000, 6271911.000, 6271645.000, 6271426.500])

# These are the desired (warped) coordinates:
dst_x = np.array([900016.829, 900755.280, 900336.549, 900370.549, 900963.549, 900907.049, 900755.669, 900747.790, 900016.322, 900025.157, 900408.379])
dst_y = np.array([6271009.119, 6271824.553, 6271960.273, 6271961.773, 6271512.273, 6271501.273, 6271616.387, 6271928.648, 6271905.386, 6271641.869, 6271425.556])



from scipy.interpolate import Rbf

# Create RBF (thin-plate spline) functions for x and y
rbf_x = Rbf(src_x, src_y, dst_x, function='thin_plate')
rbf_y = Rbf(src_x, src_y, dst_y, function='thin_plate')

def rubber_sheet_transform(x, y):
    """
    Applies the TPS transformation to a coordinate (x, y).
    Returns the transformed (x, y).
    """
    return rbf_x(x, y), rbf_y(x, y)


from osgeo import ogr
import numpy as np
from scipy.interpolate import Rbf

# Enable exceptions to help with debugging
ogr.UseExceptions()

# --- Step 1: Define your control points ---
# (Replace these with your actual control points)
src_x = np.array([900015.500, 900755.000, 900335.500, 900369.500, 900962.500, 900906.000, 900753.500, 900748.000, 900015.500, 900025.000, 900406.000])
src_y = np.array([6271008.000, 6271830.500, 6271961.000, 6271968.000, 6271518.500, 6271507.500, 6271619.500, 6271934.000, 6271911.000, 6271645.000, 6271426.500])

# These are the desired (warped) coordinates:
dst_x = np.array([900016.829, 900755.280, 900336.549, 900370.549, 900963.549, 900907.049, 900755.669, 900747.790, 900016.322, 900025.157, 900408.379])
dst_y = np.array([6271009.119, 6271824.553, 6271960.273, 6271961.773, 6271512.273, 6271501.273, 6271616.387, 6271928.648, 6271905.386, 6271641.869, 6271425.556])

# --- Step 2: Build the TPS transformation ---


# thin plate
#rbf_x = Rbf(src_x, src_y, dst_x, function='thin_plate')
#rbf_y = Rbf(src_x, src_y, dst_y, function='thin_plate')


# multiquadratic

#rbf_x = Rbf(src_x, src_y, dst_x, function='multiquadric')
#rbf_y = Rbf(src_x, src_y, dst_y, function='multiquadric')

# Linear

rbf_x = Rbf(src_x, src_y, dst_x, function='linear', smooth=0.1)
rbf_y = Rbf(src_x, src_y, dst_y, function='linear', smooth=0.1)


def rubber_sheet_transform(x, y):
    return rbf_x(x, y), rbf_y(x, y)

# --- Step 3: Define a function to transform geometries (points , lines and polygones ---
def transform_geometry(geom):
    geom_type = geom.GetGeometryType()

    if geom_type == ogr.wkbPoint:
        x, y, *_ = geom.GetPoint()
        new_x, new_y = rubber_sheet_transform(x, y)
        new_point = ogr.Geometry(ogr.wkbPoint)
        new_point.AddPoint(float(new_x), float(new_y))
        return new_point

    elif geom_type in (ogr.wkbLineString, ogr.wkbLinearRing):
        # Preserve the ring type if needed
        if geom_type == ogr.wkbLinearRing:
            new_line = ogr.Geometry(ogr.wkbLinearRing)
        else:
            new_line = ogr.Geometry(ogr.wkbLineString)
        for i in range(geom.GetPointCount()):
            pt = geom.GetPoint(i)
            new_x, new_y = rubber_sheet_transform(pt[0], pt[1])
            new_line.AddPoint(float(new_x), float(new_y))
        return new_line

    elif geom_type == ogr.wkbPolygon:
        new_poly = ogr.Geometry(ogr.wkbPolygon)
        for i in range(geom.GetGeometryCount()):
            ring = geom.GetGeometryRef(i)
            new_ring = ogr.Geometry(ogr.wkbLinearRing)
            for j in range(ring.GetPointCount()):
                pt = ring.GetPoint(j)
                new_x, new_y = rubber_sheet_transform(pt[0], pt[1])
                new_ring.AddPoint(float(new_x), float(new_y))
            new_poly.AddGeometry(new_ring)
        return new_poly

    elif geom.GetGeometryCount() > 0:
        # For geometry collections, transform each sub-geometry
        new_collection = ogr.Geometry(geom_type)
        for i in range(geom.GetGeometryCount()):
            sub_geom = geom.GetGeometryRef(i)
            new_sub_geom = transform_geometry(sub_geom)
            new_collection.AddGeometry(new_sub_geom)
        return new_collection

    else:
        raise NotImplementedError(f"Transformation for geometry type {geom_type} is not implemented.")

# --- Step 4: Process the Shapefile ---
input_file = "roof.shp"
output_file = "Troof.shp"

# Open the input shapefile
in_ds = ogr.Open(input_file)
in_layer = in_ds.GetLayer()

# Create the output shapefile
driver = ogr.GetDriverByName("ESRI Shapefile")
out_ds = driver.CreateDataSource(output_file)
srs = in_layer.GetSpatialRef()
out_layer = out_ds.CreateLayer("Troof", srs, in_layer.GetGeomType())

# Copy fields from the input layer
in_layer_defn = in_layer.GetLayerDefn()
for i in range(in_layer_defn.GetFieldCount()):
    field_defn = in_layer_defn.GetFieldDefn(i)
    out_layer.CreateField(field_defn)

# Process each feature
for in_feat in in_layer:
    geom = in_feat.GetGeometryRef()
    if geom is not None:
        new_geom = transform_geometry(geom)
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
