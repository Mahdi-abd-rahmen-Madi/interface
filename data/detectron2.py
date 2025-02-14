import os
import torch
import cv2
import numpy as np
import logging
import geopandas as gpd
from shapely.geometry import Polygon, box, shape
from detectron2.engine import DefaultPredictor
from detectron2.config import get_cfg
from detectron2.data import MetadataCatalog
from detectron2.utils.visualizer import Visualizer
from detectron2.structures import Boxes

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

# Step 2: Load Detectron2 Model
def load_detectron2_model(config_file, weights_file, confidence_threshold=0.7):
    """
    Load a pre-trained Detectron2 model for building detection.
    Args:
        config_file (str): Path to the configuration file.
        weights_file (str): Path to the pre-trained weights file.
        confidence_threshold (float): Minimum confidence score for detections.
    Returns:
        DefaultPredictor: Detectron2 predictor object.
    """
    logging.info("Loading Detectron2 model...")
    cfg = get_cfg()
    cfg.merge_from_file(config_file)
    cfg.MODEL.WEIGHTS = weights_file
    cfg.MODEL.ROI_HEADS.SCORE_THRESH_TEST = confidence_threshold
    cfg.MODEL.DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
    predictor = DefaultPredictor(cfg)
    logging.info(f"Model loaded successfully on {cfg.MODEL.DEVICE}.")
    return predictor

# Step 3: Detect Building Footprints
def detect_building_footprints(image, predictor):
    """
    Perform building footprint detection using Detectron2.
    Args:
        image (numpy.ndarray): Input image in BGR format.
        predictor: Detectron2 predictor object.
    Returns:
        list: List of detected building footprints as Shapely Polygons.
    """
    logging.info("Performing building footprint detection...")
    outputs = predictor(image)

    # Extract instance predictions
    instances = outputs["instances"].to("cpu")
    pred_masks = instances.pred_masks.numpy()  # Binary masks for each instance
    pred_boxes = instances.pred_boxes.tensor.numpy()  # Bounding boxes

    # Convert masks to Shapely Polygons
    footprints = []
    for mask, box in zip(pred_masks, pred_boxes):
        # Create a binary mask from the prediction
        binary_mask = mask.astype(np.uint8)

        # Find contours in the binary mask
        contours, _ = cv2.findContours(binary_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        # Convert contours to Shapely Polygons
        for contour in contours:
            polygon = Polygon(contour.reshape(-1, 2))
            if polygon.is_valid and not polygon.is_empty:
                footprints.append(polygon)

    logging.info(f"Detected {len(footprints)} building footprints.")
    return footprints

# Step 4: Align Shapefile Polygons Inside Detected Footprints
def align_shapefile_polygons(detected_footprints, shapefile_polygons):
    """
    Align shapefile polygons inside detected building footprints.
    Args:
        detected_footprints (list): List of detected building footprints as Shapely Polygons.
        shapefile_polygons (list): List of shapefile polygons as Shapely Polygons.
    Returns:
        list: List of tuples (detected_footprint, matched_polygon).
    """
    matches = []

    for footprint in detected_footprints:
        for shapefile_polygon in shapefile_polygons:
            # Check if the shapefile polygon is completely inside the detected footprint
            if shapefile_polygon.within(footprint):
                matches.append((footprint, shapefile_polygon))
                logging.debug(f"Matched shapefile polygon to detected footprint.")

    logging.info(f"Matched {len(matches)} shapefile polygons to detected footprints.")
    return matches

# Step 5: Process a Single Image and Shapefile Pair
def process_image_and_shapefile(image_path, shapefile_path, predictor, output_dir):
    """
    Process a single satellite image and its corresponding shapefile.
    Args:
        image_path (str): Path to the satellite image (.tif).
        shapefile_path (str): Path to the shapefile.
        predictor: Detectron2 predictor object.
        output_dir (str): Directory to save the output files.
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

    # Load shapefile
    try:
        shapefile = gpd.read_file(shapefile_path)
        shapefile_polygons = [Polygon(polygon) for polygon in shapefile.geometry]
    except Exception as e:
        logging.error(f"Error loading shapefile {shapefile_path}: {e}. Skipping...")
        return

    # Detect building footprints
    detected_footprints = detect_building_footprints(satellite_image, predictor)

    # Align shapefile polygons inside detected footprints
    matches = align_shapefile_polygons(detected_footprints, shapefile_polygons)

    # Visualize matches
    for footprint, polygon in matches:
        # Draw detected footprint (green)
        exterior_coords = np.array(footprint.exterior.coords, dtype=np.int32)
        cv2.polylines(satellite_image, [exterior_coords], isClosed=True, color=(0, 255, 0), thickness=2)

        # Draw shapefile polygon (blue)
        if not polygon.is_empty:
            exterior_coords = np.array(polygon.exterior.coords, dtype=np.int32)
            cv2.polylines(satellite_image, [exterior_coords], isClosed=True, color=(255, 0, 0), thickness=2)

    # Save the result (as .png to preserve quality)
    output_filename = os.path.join(output_dir, f"aligned_{os.path.basename(image_path)}.png")
    cv2.imwrite(output_filename, satellite_image)
    logging.info(f"Processed {image_path} and saved result to {output_filename}")

    # Save visualization
    visualization_filename = os.path.join(output_dir, f"visualization_{os.path.basename(image_path)}.png")
    cv2.imwrite(visualization_filename, satellite_image)
    logging.info(f"Saved visualization to {visualization_filename}")

# Step 6: Batch Processing (Hardcoded Paths)
def batch_process_images(output_dir, config_file, weights_file, confidence_threshold=0.7):
    """
    Process a single hardcoded satellite image and its corresponding shapefile.
    """
    logging.info("Starting processing with hardcoded paths...")

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Hardcoded paths
    image_path = "/home/mahdi/app/data/satellite_images/tile.tif"
    shapefile_path = "/home/mahdi/app/data/shapefiles/roof.shp"

    # Load Detectron2 model
    predictor = load_detectron2_model(config_file, weights_file, confidence_threshold)

    # Process the single image and shapefile pair
    process_image_and_shapefile(image_path, shapefile_path, predictor, output_dir)

    logging.info("Processing completed.")

# Main Function
if __name__ == "__main__":
    # Configure logging
    configure_logging()

    # Define output directory
    output_dir = "/home/mahdi/app/data/output"

    # Define Detectron2 configuration and weights
    config_file = "detectron2/configs/COCO-InstanceSegmentation/mask_rcnn_R_50_FPN_3x.yaml"
    weights_file = "path/to/pretrained/model.pth"  # Download a pre-trained model for buildings
    confidence_threshold = 0.7

    # Run batch processing with hardcoded paths
    batch_process_images(output_dir, config_file, weights_file, confidence_threshold)