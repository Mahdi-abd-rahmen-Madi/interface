import cv2
import numpy as np

# Load images in grayscale
img1 = cv2.imread("tile.tif", cv2.IMREAD_GRAYSCALE)
img2 = cv2.imread("roof.tif", cv2.IMREAD_GRAYSCALE)

if img1 is None or img2 is None:
    print("Error: One or both images could not be loaded. Check file paths and formats.")
    exit()

# Print image details
print(f"Image1 shape: {img1.shape}, dtype: {img1.dtype}")
print(f"Image2 shape: {img2.shape}, dtype: {img2.dtype}")

# Enhance contrast
img1 = cv2.equalizeHist(img1)
img2 = cv2.equalizeHist(img2)

# Resize if needed
def resize_image(img, max_size=1000):
    h, w = img.shape
    if max(h, w) > max_size:
        scale = max_size / max(h, w)
        img = cv2.resize(img, (int(w * scale), int(h * scale)))
    return img

img1 = resize_image(img1)
img2 = resize_image(img2)

# Create ORB detector
orb = cv2.ORB_create(nfeatures=10000)

# Detect keypoints and compute descriptors
keypoints1, descriptors1 = orb.detectAndCompute(img1, None)
keypoints2, descriptors2 = orb.detectAndCompute(img2, None)

if descriptors1 is None or descriptors2 is None:
    print("No descriptors found even after preprocessing.")
    exit()

# Match descriptors
bf = cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=True)
matches = bf.match(descriptors1, descriptors2)

# Sort matches by distance
matches = sorted(matches, key=lambda x: x.distance)

# Print the number of matches found
print(f"Number of matches found: {len(matches)}")

# Draw the top 100 matches
img_matches = cv2.drawMatches(img1, keypoints1, img2, keypoints2, matches[:100], None, flags=cv2.DrawMatchesFlags_NOT_DRAW_SINGLE_POINTS)

# Create resizable window
cv2.namedWindow("Feature Matching", cv2.WINDOW_NORMAL)  

# Move window to second screen
cv2.moveWindow("Feature Matching", -1920, 0)  # Adjust coordinates based on second screen position

# Fullscreen mode
cv2.setWindowProperty("Feature Matching", cv2.WND_PROP_FULLSCREEN, cv2.WINDOW_FULLSCREEN)

# Show result
cv2.imshow("Feature Matching", img_matches)
cv2.waitKey(0)
cv2.destroyAllWindows()
