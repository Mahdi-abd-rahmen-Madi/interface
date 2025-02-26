import os
import cv2 as cv
import matplotlib.pyplot as plt
import numpy as np


def featureMatchingHomography():
    img1 = cv.imread("tile.tif", cv.IMREAD_GRAYSCALE)
    img2 = cv.imread("roof.tif", cv.IMREAD_GRAYSCALE)


    # Print image details
    print(f"Image1 shape: {img1.shape}, dtype: {img1.dtype}")
    print(f"Image2 shape: {img2.shape}, dtype: {img2.dtype}")
    # Enhance contrast
    img1 = cv.equalizeHist(img1)
    img2 = cv.equalizeHist(img2)


    # Resize

    def resize_image(img, target_size=(600, 480)):
        h, w = img.shape
        if h > w:
            new_h = target_size[0]
            new_w = int(w * (target_size[0] / h))
        else:
            new_w = target_size[1]
            new_h = int(h * (target_size[1] / w))
        return cv.resize(img, (new_w, new_h))

    img1 = resize_image(img1, target_size=(1920, 1080))
    img2 = resize_image(img2, target_size=(1920, 1080))

    sift = cv.SIFT_create()
    keypoints1, descriptors1 = sift.detectAndCompute(img1, None)
    keypoints2, descriptors2 = sift.detectAndCompute(img2, None)


     # Print number of keypoints detected
    print(f"Number of keypoints in Image1: {len(keypoints1)}")
    print(f"Number of keypoints in Image2: {len(keypoints2)}")

    # Check if keypoints are detected
    if len(keypoints1) == 0 or len(keypoints2) == 0:
        print("No keypoints detected in one or both images.")
        return

    FLANN_INDEX_KDRTREE = 1
    indexParams = dict(algorithm=FLANN_INDEX_KDRTREE, trees=5)
    searchParams = dict(checks=50)
    flann = cv.FlannBasedMatcher(indexParams, searchParams)
    nNeighbors = 2
    matches = flann.knnMatch(descriptors1, descriptors2, k=nNeighbors)

    goodMatches = []

    for m, n in matches:
        if m.distance < 0.8 * n.distance:
            goodMatches.append(m)
    mingGoodMatches = 20

    if len(goodMatches) > mingGoodMatches:
        print(f"Number of good matches: {len(goodMatches)}")

        # Extract source and destination points
        src_pts = np.float32([keypoints1[m.queryIdx].pt for m in goodMatches]).reshape(
            -1, 1, 2
        )
        dst_pts = np.float32([keypoints2[m.trainIdx].pt for m in goodMatches]).reshape(
            -1, 1, 2
        )
       

        # Compute homography matrix using RANSAC
        errorThreshold = 5
        M, mask = cv.findHomography(src_pts, dst_pts, cv.RANSAC, errorThreshold)

        matchesMask = mask.ravel().tolist()
        h, w = img1.shape
        imgBorder = np.float32([[0, 0], [0, h - 1], [w - 1, h - 1], [w - 1, 0]]).reshape(
            -1, 1, 2
        )
        warpedImgBorder = cv.perspectiveTransform(imgBorder,M)
        img2 = cv.polylines(img2,[np.int32(warpedImgBorder)],True,255,3,cv.LINE_AA)
    else:
        print(f"Not enough good matches ({len(goodMatches)}). Required at least {mingGoodMatches}.")
        matchesMask = None
    
    # Draw matches with mask
    green = (0,255,0)
    drawParams = dict(matchColor= green,singlePointColor=None, matchesMask=matchesMask, flags=cv.DrawMatchesFlags_NOT_DRAW_SINGLE_POINTS)
    ImgMatch = cv.drawMatches(img1,keypoints1,img2,keypoints2,goodMatches,None,**drawParams)

    #plt.figure()
    #plt.imshow(ImgMatch, 'gray')
    #plt.show()

     # Save the matched image to a file
    plt.figure(figsize=(15, 10))
    plt.imshow(ImgMatch, 'gray')
    plt.title('Matches')
    plt.axis('off')
    plt.savefig('matched_images.png')
    plt.close()

    # Example: Transform a point from roof.tif to tile.tif
    # Define a point in roof.tif (e.g., (100, 100))
    point_in_roof = np.array([[100, 100]], dtype=np.float32).reshape(-1, 1, 2)
    point_in_tile = cv.perspectiveTransform(point_in_roof, M)

    print(f"Point in roof.tif: {point_in_roof.flatten()}")
    print(f"Corresponding point in tile.tif: {point_in_tile.flatten()}")

    # Create resizable window
    cv.namedWindow("Feature Matching", cv.WINDOW_NORMAL) 

    cv.imshow("Feature Matching", ImgMatch)
    cv.waitKey(0)
    cv.destroyAllWindows()

    
try:
    featureMatchingHomography()
except Exception as e:
    print(f"An error occurred: {e}")