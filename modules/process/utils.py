# utils.py

import os
import logging

def configure_logging():
    """
    Configure the logging system.
    Logs will be written to 'align_shapefiles.log' and printed to the console.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("align_shapefiles.log"),
            logging.StreamHandler()
        ]
    )

def check_dependencies():
    """
    Check if required dependencies are installed.
    """
    try:
        import geopandas
        import shapely
        import psycopg2
        import sqlalchemy
    except ImportError as e:
        logging.error(f"Missing dependency: {e}")
        raise SystemExit("Please install missing dependencies.")