import cv2 as cv
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import Polygon
from shapely.ops import transform
import pyproj

def detect_and_transform_shapes():
    # Load image
    img1 = cv.imread("tile.tif", cv.IMREAD_GRAYSCALE)

    # Print image details
    print(f"Image1 shape: {img1.shape}, dtype: {img1.dtype}")

    # Check if image is loaded correctly
    if img1 is None:
        print("Image could not be loaded. Please check the file path.")
        return

    # Enhance contrast
    img1 = cv.equalizeHist(img1)

    # Resize image to a consistent size
    def resize_image(img, target_size=(2758, 2833)):
        h, w = img.shape
        if h > w:
            new_h = target_size[0]
            new_w = int(w * (target_size[0] / h))
        else:
            new_w = target_size[1]
            new_h = int(h * (target_size[1] / w))
        return cv.resize(img, (new_w, new_h))

    img1 = resize_image(img1, target_size=(2758, 2833))

    # Read shapefile
    shapefile_path = "roof.shp"
    gdf = gpd.read_file(shapefile_path)

    # Print shapefile details
    print(f"Number of shapes in shapefile: {len(gdf)}")

    # Check if shapes are detected
    if gdf.empty:
        print("No shapes detected in the shapefile.")
        return

    # Define coordinate systems
    crs_shapefile = gdf.crs
    crs_image = "EPSG:2154"  # Assuming the image is in EPSG:2154

    # Create transformer
    transformer = pyproj.Transformer.from_crs(crs_shapefile, crs_image, always_xy=True)

    # Function to transform coordinates
    def transform_coordinates(coords):
        transformed_coords = [transformer.transform(x, y) for x, y in coords]
        return np.array(transformed_coords, dtype=np.float32)

    # Convert geometries to contours with transformed coordinates
    def geometry_to_contours(geometry):
        if isinstance(geometry, Polygon):
            coords = geometry.exterior.coords
            transformed_coords = transform_coordinates(coords)
            transformed_coords = transformed_coords.reshape(-1, 1, 2)
            return [transformed_coords]
        return []

    contours_from_shapefile = []
    for geom in gdf.geometry:
        contours_from_shapefile.extend(geometry_to_contours(geom))

    # Print number of contours from shapefile
    print(f"Number of contours from shapefile: {len(contours_from_shapefile)}")

    # Check if contours are detected
    if len(contours_from_shapefile) == 0:
        print("No contours detected from the shapefile.")
        return

    # Manually define control points for homography
    # These points should be known correspondences between the shapefile and the image
    control_points_shapefile = np.float32([
        [900159.193, 6271206.336],
        [900215.699, 6271199.372],
        [900943.123, 6271289.268],
        [900673.195, 6271805.313],
        [900457.698, 6271776.301],
        [900393.661, 6271805.32],
        [900304.688, 6271783.813],
        [900991.148, 6271715.381],
        [900990.244, 6271698.861],
        [900906.71,  6271502.431],
        [900956.675, 6271503.865],
        [900862.792, 6271045.237],
        [900717.106, 6271026.91],
        [900696.69,  6271017.824],
        [900693.153, 6271055.367],
        [900710.265, 6271055.741],
        [900741.683, 6271090.807],
        [900755.647, 6271098.319],
        [900570.283, 6271099.753],
        [900581.628, 6271098.849],
        [900515.175, 6271074.84],
        [900514.24,  6271079.337],
        [900516.687, 6271080.35],
        [900488.167, 6271055.726],
        [900475.73,  6271056.318],
        [900051.186, 6271849.317],
        [900048.183, 6271847.312],
        [900036.184, 6271900.777],
        [900016.192, 6271905.8],
        [900406.724, 6271421.319],
        [900421.785, 6271393.895],
        [900794.185, 6271950.8],
        [900754.143, 6271928.857],
        [900748.723, 6271928.844],
        [900335.235, 6271961.253],
        [900336.231, 6271955.833],
        [900339.197, 6271940.312],
        [900370.182, 6271962.835],
        [900016.201, 6271002.826],
        [900013.179, 6270999.804],
        [900003.215, 6270998.337],
        [900076.723, 6271004.863],
        [900084.135, 6271004.877],
        [900093.248, 6270999.328],
        [900000.722, 6271275.405],
        [900057.204, 6271701.829]
    ])
    control_points_image = np.float32([
        [900159.705, 6271215.24],
        [900216.939, 6271207.755],
        [900942.807, 6271292.411],
        [900673.177, 6271807.047],
        [900457.606, 6271776.292],
        [900393.736, 6271805.318],
        [900304.677, 6271783.831],
        [900991.621, 6271712.406],
        [900990.552, 6271695.048],
        [900906.902, 6271501.143],
        [900956.557, 6271502.59],
        [900861.398, 6271046.981],
        [900718.721, 6271033.458],
        [900696.802, 6271022.766],
        [900693.406, 6271060.755],
        [900709.821, 6271058.994],
        [900741.52,  6271094.969],
        [900756.206, 6271103.492],
        [900569.378, 6271105.598],
        [900580.699, 6271103.963],
        [900516.389, 6271080.897],
        [900515.084, 6271084.482],
        [900518.559, 6271086.777],
        [900488.495, 6271062.814],
        [900476.356, 6271063.946],
        [900051.911, 6271848.982],
        [900048.672, 6271847.205],
        [900036.722, 6271900.572],
        [900016.455, 6271905.43],
        [900408.399, 6271425.371],
        [900423.808, 6271396.848],
        [900794.902, 6271951.501],
        [900754.775, 6271928.67],
        [900748.8,   6271928.921],
        [900335.519, 6271960.243],
        [900336.494, 6271955.148],
        [900340.284, 6271941.61],
        [900371.527, 6271962.444],
        [900016.895, 6271008.961],
        [900013.027, 6271005.965],
        [900001.894, 6271003.489],
        [900076.142, 6271011.319],
        [900084.208, 6271010.517],
        [900092.463, 6271005.564],
        [900000.243, 6271279.793],
        [900057.674, 6271703.578]
    ])

    # Compute homography matrix using control points
    M, mask = cv.findHomography(control_points_shapefile, control_points_image, cv.RANSAC, ransacReprojThreshold=5.0)
    matches_mask = mask.ravel().tolist()

    # Print homography matrix
    print(f"Homography Matrix:\n{M}")

    # Transform contours from shapefile to image
    transformed_contours = []
    for contour in contours_from_shapefile:
        contour = contour.astype(np.float32)  # Ensure the contour points are float32
        contour = contour.reshape(-1, 1, 2)
        transformed_contour = cv.perspectiveTransform(contour, M)
        transformed_contours.append(transformed_contour)

    # Draw transformed contours on img1
    img1_with_transformed_contours = cv.cvtColor(img1, cv.COLOR_GRAY2BGR)
    for contour in transformed_contours:
        contour = np.int32(contour)
        cv.drawContours(img1_with_transformed_contours, [contour], -1, (0, 255, 0), 2)

    # Save the transformed contours image to a file
    plt.figure(figsize=(15, 10))
    plt.imshow(cv.cvtColor(img1_with_transformed_contours, cv.COLOR_BGR2RGB))
    plt.title('Transformed Contours')
    plt.axis('off')
    plt.savefig('transformed_contours.png')
    plt.close()

    # Create resizable window
    cv.namedWindow("Feature Matching", cv.WINDOW_NORMAL) 
    cv.imshow("Feature Matching", img1_with_transformed_contours)
    cv.waitKey(0)
    cv.destroyAllWindows()


try:
    detect_and_transform_shapes()
except Exception as e:
    print(f"An error occurred: {e}")