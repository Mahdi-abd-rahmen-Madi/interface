import os
import cv2

# Set the OpenCV Qt plugin path (if needed)
os.environ["QT_QPA_PLATFORM_PLUGIN_PATH"] = "/home/mahdi/app/data/env/lib/python3.12/site-packages/cv2/qt/plugins/platforms"

# Load the two GeoTIFF images in grayscale
img1 = cv2.imread("tile.tif", cv2.IMREAD_GRAYSCALE)
img2 = cv2.imread("roof.tif", cv2.IMREAD_GRAYSCALE)

# Create an ORB detector
orb = cv2.ORB_create(nfeatures=5000)

# Detect keypoints and compute descriptors for both images
keypoints1, descriptors1 = orb.detectAndCompute(img1, None)
keypoints2, descriptors2 = orb.detectAndCompute(img2, None)

descriptors1 = descriptors1.astype('uint8')
descriptors2 = descriptors2.astype('uint8')

# Create a BFMatcher (Brute-Force Matcher) with Hamming distance
bf = cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=True)

# Match descriptors between the two images
matches = bf.match(descriptors1, descriptors2)

# Sort matches by distance (lower distance is better)
matches = sorted(matches, key=lambda x: x.distance)

# Draw the top 50 matches
img_matches = cv2.drawMatches(img1, keypoints1, img2, keypoints2, matches[:50], None, flags=cv2.DrawMatchesFlags_NOT_DRAW_SINGLE_POINTS)

# Show the matched features
cv2.imshow("Feature Matching", img_matches)
cv2.waitKey(0)
cv2.destroyAllWindows()
