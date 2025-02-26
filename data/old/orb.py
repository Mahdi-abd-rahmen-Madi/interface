import os
os.environ["QT_QPA_PLATFORM_PLUGIN_PATH"] = "/home/mahdi/app/data/env/lib/python3.12/site-packages/cv2/qt/plugins/platforms"

import cv2


#os.environ["QT_DEBUG_PLUGINS"] = "1"

# Load the GeoTIFF image in grayscale
img = cv2.imread("tile.tif", cv2.IMREAD_GRAYSCALE)

# Create an ORB detector
orb = cv2.ORB_create(nfeatures=10000)

# Detect keypoints and compute descriptors
keypoints, descriptors = orb.detectAndCompute(img, None)

# Draw the keypoints on the image for visualization
img_keypoints = cv2.drawKeypoints(img, keypoints, None, color=(0,255,0))
cv2.imshow("ORB Features", img_keypoints)
cv2.waitKey(0)
cv2.destroyAllWindows()


