def check_dependencies():
    """
    Check if required dependencies are installed.
    """
    try:
        import geopandas
        import sqlalchemy
        import psycopg2
        import geoalchemy2
        print("All required dependencies are installed.")
    except ImportError as e:
        raise ImportError(f"Missing dependency: {e}")