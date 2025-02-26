from osgeo import ogr
ogr.UseExceptions()  # Enable exceptions explicitly to avoid warnings.

def translate_geometry(geom, dx, dy):
    """
    Returns a new geometry translated by dx and dy.
    Handles points, linestrings, and polygons.
    Extend as needed for other geometry types.
    """
    geom_type = geom.GetGeometryType()
    
    # Handle point geometries:
    if geom_type == ogr.wkbPoint:
        x, y, *_ = geom.GetPoint()  # Works for both 2D and 3D points
        new_point = ogr.Geometry(ogr.wkbPoint)
        new_point.AddPoint(x + dx, y + dy)
        return new_point

    # Handle linestrings and linear rings:
    elif geom_type in (ogr.wkbLineString, ogr.wkbLinearRing):
        # If this geometry is a linear ring, keep it as such.
        if geom_type == ogr.wkbLinearRing:
            new_line = ogr.Geometry(ogr.wkbLinearRing)
        else:
            new_line = ogr.Geometry(ogr.wkbLineString)
        for i in range(geom.GetPointCount()):
            pt = geom.GetPoint(i)
            new_line.AddPoint(pt[0] + dx, pt[1] + dy)
        return new_line

    # Handle polygon geometries:
    elif geom_type == ogr.wkbPolygon:
        new_poly = ogr.Geometry(ogr.wkbPolygon)
        # A polygon is made of rings; the first is the exterior ring,
        # and subsequent rings (if any) are interior rings.
        for i in range(geom.GetGeometryCount()):
            ring = geom.GetGeometryRef(i)
            # Always create a new linear ring for polygon rings.
            new_ring = ogr.Geometry(ogr.wkbLinearRing)
            for j in range(ring.GetPointCount()):
                pt = ring.GetPoint(j)
                new_ring.AddPoint(pt[0] + dx, pt[1] + dy)
            new_poly.AddGeometry(new_ring)
        return new_poly

    # Handle multi-geometries and geometry collections:
    elif geom.GetGeometryCount() > 0:
        new_collection = ogr.Geometry(geom_type)
        for i in range(geom.GetGeometryCount()):
            sub_geom = geom.GetGeometryRef(i)
            new_sub_geom = translate_geometry(sub_geom, dx, dy)
            new_collection.AddGeometry(new_sub_geom)
        return new_collection

    else:
        raise NotImplementedError(f"Translation for geometry type {geom_type} is not implemented.")

# Example usage: reading from an input shapefile and writing to an output shapefile
if __name__ == "__main__":
    input_file = "roof.shp"
    output_file = "output.shp"
    
    # Open the input data source
    in_ds = ogr.Open(input_file)
    in_layer = in_ds.GetLayer()

    # Create the output data source
    driver = ogr.GetDriverByName("ESRI Shapefile")
    out_ds = driver.CreateDataSource(output_file)
    
    # Create a new layer with the same spatial reference and geometry type as the input layer
    srs = in_layer.GetSpatialRef()
    out_layer = out_ds.CreateLayer("output", srs, in_layer.GetGeomType())

    # Copy the fields from the input layer to the output layer
    in_layer_defn = in_layer.GetLayerDefn()
    for i in range(in_layer_defn.GetFieldCount()):
        field_defn = in_layer_defn.GetFieldDefn(i)
        out_layer.CreateField(field_defn)

    # Define the translation offsets
    dx = -1.056
    dy = 7.32

    # Process each feature in the input layer
    for in_feat in in_layer:
        geom = in_feat.GetGeometryRef()
        if geom is not None:
            new_geom = translate_geometry(geom, dx, dy)
        else:
            new_geom = None

        # Create a new feature for the output layer
        out_feat = ogr.Feature(out_layer.GetLayerDefn())
        if new_geom is not None:
            out_feat.SetGeometry(new_geom)

        # Copy field values from the input feature to the output feature
        for i in range(in_layer_defn.GetFieldCount()):
            field_name = in_layer_defn.GetFieldDefn(i).GetNameRef()
            out_feat.SetField(field_name, in_feat.GetField(i))

        # Add the feature to the output layer
        out_layer.CreateFeature(out_feat)
        out_feat = None

    # Cleanup: close data sources
    in_ds = None
    out_ds = None
