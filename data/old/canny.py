import cv2 as cv
import numpy as np
import matplotlib.pyplot as plt

def detect_and_transform_shapes():
    # Load images
    img1 = cv.imread("tile.tif", cv.IMREAD_GRAYSCALE)
    img2 = cv.imread("roof.tif", cv.IMREAD_GRAYSCALE)

    # Print image details
    print(f"Image1 shape: {img1.shape}, dtype: {img1.dtype}")
    print(f"Image2 shape: {img2.shape}, dtype: {img2.dtype}")

    # Check if images are loaded correctly
    if img1 is None or img2 is None:
        print("One or both images could not be loaded. Please check the file paths.")
        return

    # Enhance contrast
    img1 = cv.equalizeHist(img1)
    img2 = cv.equalizeHist(img2)

    # Resize images to a consistent size
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
    img2 = resize_image(img2, target_size=(2758, 2833))

    # Apply Canny edge detection
    edges1 = cv.Canny(img1, 50, 150)
    edges2 = cv.Canny(img2, 50, 150)

    # Find contours
    contours1, _ = cv.findContours(edges1, cv.RETR_EXTERNAL, cv.CHAIN_APPROX_SIMPLE)
    contours2, _ = cv.findContours(edges2, cv.RETR_EXTERNAL, cv.CHAIN_APPROX_SIMPLE)

    # Print number of contours detected
    print(f"Number of contours in Image1: {len(contours1)}")
    print(f"Number of contours in Image2: {len(contours2)}")

    # Check if contours are detected
    if len(contours1) == 0 or len(contours2) == 0:
        print("No contours detected in one or both images.")
        return

    # Create keypoints from contours
    def create_keypoints_from_contours(contours):
        keypoints = []
        for contour in contours:
            M = cv.moments(contour)
            if M["m00"] != 0:
                cx = int(M["m10"] / M["m00"])
                cy = int(M["m01"] / M["m00"])
                size = 1.0  # Set a default size for keypoints
                keypoints.append(cv.KeyPoint(cx, cy, size))
        return keypoints

    keypoints1 = create_keypoints_from_contours(contours1)
    keypoints2 = create_keypoints_from_contours(contours2)

    # Match contours using shape matching
    good_matches = []
    for i1, (contour1, kp1) in enumerate(zip(contours1, keypoints1)):
        best_match = None
        best_match_score = float('inf')
        best_match_index = -1
        for i2, (contour2, kp2) in enumerate(zip(contours2, keypoints2)):
            match_score = cv.matchShapes(contour1, contour2, cv.CONTOURS_MATCH_I1, 0.0)
            if match_score < best_match_score:
                best_match_score = match_score
                best_match = kp2
                best_match_index = i2
        if best_match_score < 0.1:  # Threshold for matching
            good_matches.append((i1, best_match_index))

    # Print number of good matches
    print(f"Number of good matches: {len(good_matches)}")

    # Check if there are enough good matches
    if len(good_matches) < 5:
        print("Not enough good matches. Required at least 5.")
        return

    # Extract points from good matches
    src_points = np.float32([keypoints1[i1].pt for i1, _ in good_matches]).reshape(-1, 1, 2)
    dst_points = np.float32([keypoints2[i2].pt for _, i2 in good_matches]).reshape(-1, 1, 2)

    # Compute homography matrix using RANSAC
    M, mask = cv.findHomography(src_points, dst_points, cv.RANSAC, ransacReprojThreshold=5.0)
    matches_mask = mask.ravel().tolist()

    # Draw matches with mask
    green = (0, 255, 0)
    draw_params = dict(
        matchColor=green,
        singlePointColor=None,
        matchesMask=matches_mask,
        flags=cv.DrawMatchesFlags_NOT_DRAW_SINGLE_POINTS
    )

    # Convert images to BGR for drawing
    img1_bgr = cv.cvtColor(img1, cv.COLOR_GRAY2BGR)
    img2_bgr = cv.cvtColor(img2, cv.COLOR_GRAY2BGR)

    # Create matches list for cv.drawMatches
    matches = [cv.DMatch(i1, i2, 0) for i1, i2 in good_matches]

    img_matches = cv.drawMatches(img1_bgr, keypoints1, img2_bgr, keypoints2, matches, None, **draw_params)

    # Save the matched image to a file
    plt.figure(figsize=(15, 10))
    plt.imshow(cv.cvtColor(img_matches, cv.COLOR_BGR2RGB))
    plt.title('Matches')
    plt.axis('off')
    plt.savefig('matched_contours.png')
    plt.close()

    # Transform contours from roof.tif to tile.tif
    transformed_contours = []
    for contour2 in contours2:
        contour2 = contour2.astype(np.float32)  # Ensure the contour points are float32
        contour2 = contour2.reshape(-1, 1, 2)
        contour1 = cv.perspectiveTransform(contour2, M)
        transformed_contours.append(contour1)

    # Draw transformed contours on img1
    img1_with_transformed_contours = img1_bgr.copy()
    for contour1 in transformed_contours:
        contour1 = np.int32(contour1)
        cv.drawContours(img1_with_transformed_contours, [contour1], -1, (0, 255, 0), 2)

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