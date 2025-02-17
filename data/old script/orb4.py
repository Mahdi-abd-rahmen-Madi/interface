import cv2
import numpy as np


# Set the OpenCV Qt plugin path (if needed)
os.environ["QT_QPA_PLATFORM_PLUGIN_PATH"] = "/home/mahdi/app/data/env/lib/python3.12/site-packages/cv2/qt/plugins/platforms"

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

# Resize

def resize_image(img, target_size=(2758, 2833)):
    h, w = img.shape
    if h > w:
        new_h = target_size[0]
        new_w = int(w * (target_size[0] / h))
    else:
        new_w = target_size[1]
        new_h = int(h * (target_size[1] / w))
    return cv2.resize(img, (new_w, new_h))

img1 = resize_image(img1, target_size=(2758, 2833))
img2 = resize_image(img2, target_size=(2758, 2833))



# Create ORB detector
orb = cv2.ORB_create(nfeatures=5000)

# Detect keypoints and compute descriptors
keypoints1, descriptors1 = orb.detectAndCompute(img1, None)
keypoints2, descriptors2 = orb.detectAndCompute(img2, None)

if descriptors1 is None or descriptors2 is None:
    print("No descriptors found even after preprocessing.")
    exit()

# Match descriptors
# Perform KNN matching with BFMatcher
bf = cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=False)
matches = bf.knnMatch(descriptors1, descriptors2, k=2)

# Apply ratio test to filter good matches
good_matches = []
for m, n in matches:
    if m.distance < 0.8 * n.distance:  # Adjust the threshold as needed
        good_matches.append(m)

print(f"Number of good matches after ratio test: {len(good_matches)}")

# Homography estimation  

if len(good_matches) >= 4:  # Minimum number of points required for homography
    src_pts = np.float32([keypoints1[m.queryIdx].pt for m in good_matches]).reshape(-1, 1, 2)
    dst_pts = np.float32([keypoints2[m.trainIdx].pt for m in good_matches]).reshape(-1, 1, 2)

    # Find homography matrix using RANSAC
    M, mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, 5.0)

    if M is not None:
        print("Homography matrix estimated successfully.")
        # Draw inliers
        inliers = [m for i, m in enumerate(good_matches) if mask[i] == 1]
        img_matches = cv2.drawMatches(
            img1, keypoints1, img2, keypoints2, inliers[:100], None,
            flags=cv2.DrawMatchesFlags_NOT_DRAW_SINGLE_POINTS
        )
    else:
        print("Failed to estimate homography matrix.")
else:
    print("Not enough matches to estimate homography.")

# Draw the top 100 good matches
img_matches = cv2.drawMatches(
    img1, keypoints1, img2, keypoints2, good_matches[:100], None,
    flags=cv2.DrawMatchesFlags_NOT_DRAW_SINGLE_POINTS
)

# Create resizable window
cv2.namedWindow("Feature Matching", cv2.WINDOW_NORMAL)  

# Move window to second screen
#cv2.moveWindow("Feature Matching", -1920, 0)  # Adjust coordinates based on second screen position

# Fullscreen mode
#cv2.setWindowProperty("Feature Matching", cv2.WND_PROP_FULLSCREEN, cv2.WINDOW_FULLSCREEN)


# Draw keypoints on images
img1_with_keypoints = cv2.drawKeypoints(img1, keypoints1, None, color=(0, 255, 0))
img2_with_keypoints = cv2.drawKeypoints(img2, keypoints2, None, color=(0, 255, 0))

# Show result + Display keypoints
#cv2.imshow("Keypoints in Image 1", img1_with_keypoints)
#cv2.imshow("Keypoints in Image 2", img2_with_keypoints)
cv2.imshow("Feature Matching", img_matches)
cv2.waitKey(0)
cv2.destroyAllWindows()
