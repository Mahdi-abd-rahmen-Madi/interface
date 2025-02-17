import os
import torch
from torchvision.models.detection.faster_rcnn import fasterrcnn_resnet50_fpn, FasterRCNN_ResNet50_FPN_Weights
from torchvision.transforms import functional as F
import cv2
import geopandas as gpd
from shapely.geometry import Polygon, box
import numpy as np
import logging

# Step 1: Configure Logging
def configure_logging():
    """
    Configure the logging system.
    Logs will be written to 'processML.log' and also printed to the console.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("processML.log"),
            logging.StreamHandler()
        ]
    )

# Step 2: Load Faster R-CNN Model
def load_model():
    """
    Load pre-trained Faster R-CNN model with updated weights parameter.
    """
    logging.info("Loading Faster R-CNN model...")
    weights = FasterRCNN_ResNet50_FPN_Weights.DEFAULT
    model = fasterrcnn_resnet50_fpn(weights=weights)
    model.eval()
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model.to(device)
    logging.info(f"Model loaded successfully on {device}.")
    return model, device

# Step 3: Object Detection Function
def detect_objects(image, model, device, confidence_threshold=0.7):
    """
    Perform object detection on the input image using Faster R-CNN.
    Args:
        image (numpy.ndarray): Input image in BGR format.
        model: Pre-trained Faster R-CNN model.
        device: Device (CPU or GPU) to run the model.
        confidence_threshold (float): Minimum confidence score to consider a detection.
    Returns:
        list: List of detected bounding boxes [x_min, y_min, x_max, y_max].
    """
    logging.info("Performing object detection...")
    image_tensor = F.to_tensor(image).to(device)
    predictions = model([image_tensor])[0]

    # Detach tensors from computation graph and convert to NumPy
    boxes = predictions['boxes'].cpu().detach().numpy()
    scores = predictions['scores'].cpu().detach().numpy()

    # Filter detections based on confidence
    filtered_boxes = [box for box, score in zip(boxes, scores) if score > confidence_threshold]
    logging.info(f"Detected {len(filtered_boxes)} objects.")
    return filtered_boxes

# Step 4: Compute IoU Between Bounding Box and Polygon
def compute_iou(box_coords, polygon):
    """
    Compute Intersection over Union (IoU) between a bounding box and a polygon.
    Args:
        box_coords (list): Bounding box coordinates [x_min, y_min, x_max, y_max].
        polygon (Polygon): Shapely Polygon object.
    Returns:
        float: IoU value between the bounding box and the polygon.
    """
    bbox = box(box_coords[0], box_coords[1], box_coords[2], box_coords[3])
    intersection = bbox.intersection(polygon).area
    union = bbox.union(polygon).area
    return intersection / union if union > 0 else 0

# Step 5: Match Detected Boxes to Polygons Using IoU
def match_detected_boxes_to_polygons(detected_boxes, polygons, iou_threshold=0.1):
    """
    Match detected bounding boxes to polygons using IoU.
    Args:
        detected_boxes (list): List of bounding boxes [x_min, y_min, x_max, y_max].
        polygons (list): List of Shapely Polygon objects.
        iou_threshold (float): Minimum IoU value for a match.
    Returns:
        list: List of tuples (detected_box, matched_polygon).
    """
    matches = []

    for idx, box_coords in enumerate(detected_boxes):
        # Compute IoU with all polygons
        ious = [compute_iou(box_coords, poly) for poly in polygons]

        if len(ious) == 0:
            logging.warning("No polygons available for matching.")
            continue

        max_iou = max(ious)
        closest_index = ious.index(max_iou)

        # Check if the IoU is above the threshold
        if max_iou >= iou_threshold:
            matches.append((box_coords, polygons[closest_index]))
            logging.debug(f"Matched bounding box {idx} to polygon {closest_index} with IoU {max_iou:.2f}.")

    logging.info(f"Matched {len(matches)} bounding boxes to polygons using IoU.")
    return matches

# Step 6: Process a Single Image and Shapefile Pair
def process_image_and_shapefile(image_path, shapefile_path, model, device, output_dir, confidence_threshold=0.7, iou_threshold=0.1):
    """
    Process a single satellite image and its corresponding shapefile.
    Args:
        image_path (str): Path to the satellite image (.tif).
        shapefile_path (str): Path to the shapefile.
        model: Pre-trained Faster R-CNN model.
        device: Device (CPU or GPU) to run the model.
        output_dir (str): Directory to save the output image.
        confidence_threshold (float): Minimum confidence score for object detection.
        iou_threshold (float): Minimum IoU value for a match.
    """
    logging.info(f"Processing image: {image_path}")

    # Load satellite image (supports .tif format)
    try:
        satellite_image = cv2.imread(image_path, cv2.IMREAD_COLOR)
        if satellite_image is None:
            raise ValueError(f"Failed to load image {image_path}")
    except Exception as e:
        logging.error(f"Error loading image {image_path}: {e}. Skipping...")
        return

    # Set SHAPE_RESTORE_SHX option to YES
    os.environ['SHAPE_RESTORE_SHX'] = 'YES'

    # Load shapefile
    try:
        shapefile = gpd.read_file(shapefile_path)
        polygons = [Polygon(polygon) for polygon in shapefile.geometry]
    except Exception as e:
        logging.error(f"Error loading shapefile {shapefile_path}: {e}. Skipping...")
        return

    # Detect objects
    detected_boxes = detect_objects(satellite_image, model, device, confidence_threshold)

    # Match detected boxes to polygons using IoU
    matches = match_detected_boxes_to_polygons(detected_boxes, polygons, iou_threshold)

    # Visualize matches
    for detected_box, polygon in matches:
        x_min, y_min, x_max, y_max = map(int, detected_box)
        cv2.rectangle(satellite_image, (x_min, y_min), (x_max, y_max), (0, 255, 0), 2)

    # Save the result (as .png to preserve quality)
    output_filename = os.path.join(output_dir, f"aligned_{os.path.basename(image_path)}.png")
    cv2.imwrite(output_filename, satellite_image)
    logging.info(f"Processed {image_path} and saved result to {output_filename}")

    # Save visualization
    visualization_filename = os.path.join(output_dir, f"visualization_{os.path.basename(image_path)}.png")

    # Draw detected bounding boxes (green)
    for box_coords in detected_boxes:
        x_min, y_min, x_max, y_max = map(int, box_coords)
        cv2.rectangle(satellite_image, (x_min, y_min), (x_max, y_max), (0, 255, 0), 2)

    # Draw shapefile polygons (blue)
    for polygon in polygons:
        if polygon.is_empty:
            continue
        exterior_coords = np.array(polygon.exterior.coords, dtype=np.int32)
        cv2.polylines(satellite_image, [exterior_coords], isClosed=True, color=(255, 0, 0), thickness=2)

    cv2.imwrite(visualization_filename, satellite_image)
    logging.info(f"Saved visualization to {visualization_filename}")

# Step 7: Batch Processing (Hardcoded Paths)
def batch_process_images(output_dir, confidence_threshold=0.7, iou_threshold=0.1):
    """
    Process a single hardcoded satellite image and its corresponding shapefile.
    """
    logging.info("Starting processing with hardcoded paths...")

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Hardcoded paths
    image_path = "/home/mahdi/app/data/satellite_images/tile.tif"
    shapefile_path = "/home/mahdi/app/data/shapefiles/roof.shp"

    # Load Faster R-CNN model
    model, device = load_model()

    # Process the single image and shapefile pair
    process_image_and_shapefile(image_path, shapefile_path, model, device, output_dir, confidence_threshold, iou_threshold)

    logging.info("Processing completed.")

# Main Function
if __name__ == "__main__":
    # Configure logging
    configure_logging()

    # Define output directory
    output_dir = "/home/mahdi/app/data/output"

    # Define parameters
    confidence_threshold = 0.7
    iou_threshold = 0.1  # Adjust this threshold as needed

    # Run batch processing with hardcoded paths
    batch_process_images(output_dir, confidence_threshold, iou_threshold)