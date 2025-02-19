
This script `load.py` automates the process of loading shapefiles into a PostGIS database, ensuring data validation, proper indexing, and secure connection management using `.pgpass`. Below is a detailed breakdown of the functionality and implementation.

---

#### **1. Overview**

The script performs the following tasks:

1. Validates the existence of shapefiles.
2. Extracts layer names and validates geometries within the shapefiles.
3. Loads the shapefiles into a PostGIS database with SPGiST indexing, CRS validation, field length checks, and reserved word avoidance.
4. Grants ownership and privileges to a specified user.

The script uses Python's `subprocess` module to execute shell commands (e.g., `ogrinfo`, `ogr2ogr`, `psql`) and ensures logging for debugging and monitoring purposes.

---

#### **2. Key Components**

##### **a. Logging Configuration**

The script configures logging to provide detailed logs during execution:

```python
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
```

This ensures that all important events are logged with timestamps and severity levels (`INFO`, `DEBUG`, `ERROR`).

---

##### **b. Helper Functions**

1. **`run_command(command)`** : Executes a shell command and captures its output or errors.

```python
import subprocess

def run_command(command):
    try:
        logging.debug(f"Running command: {' '.join(command[:2])} [...]")  # Mask sensitive parts of the command
        result = subprocess.run(command, check=True, text=True, capture_output=True)
        logging.debug(result.stdout)
        return result.stdout
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed with return code {e.returncode}")
        logging.error(e.stderr)
        raise
```
2. **`validate_file(file_path)`** : Checks if a file exists at the given path.

```python
import os

def validate_file(file_path):
    if not os.path.isfile(file_path):
        logging.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")
```
3. **`get_layer_name(shapefile)`** : Extracts the layer name from a shapefile using `ogrinfo`.
   
```python
def get_layer_name(shapefile):
    cmd = ["ogrinfo", "-so", shapefile]
    output = run_command(cmd)
    for line in output.splitlines():
        if line.strip().startswith("1:"):
            layer_name = line.split(":")[1].strip().split(" ")[0]
            logging.info(f"Determined layer name: {layer_name}")
            return layer_name
    logging.error("Could not determine layer name from shapefile.")
    raise ValueError("Could not determine layer name from shapefile.")
```

4. **`validate_geometries(shapefile)`** : Validates geometries in the shapefile using `ogrinfo`.

```python
def validate_geometries(shapefile):
    cmd = ["ogrinfo", "-al", "-so", shapefile]
    output = run_command(cmd)
    if "Invalid geometry" in output:
        logging.error("Invalid geometries detected in the shapefile.")
        raise ValueError("Invalid geometries detected in the shapefile.")
    logging.info("All geometries are valid.")
```

---

##### **c. Loading Shapefiles into PostGIS**

The function `load_to_postgis` handles the entire process of loading a shapefile into a PostGIS database:

```python
def load_to_postgis(shapefile, layer_name, postgis_config):
    host = postgis_config.get("host", "localhost")
    port = postgis_config.get("port", "5432")
    dbname = postgis_config.get("dbname", "roofs")
    user = postgis_config.get("user", "mahdi")
    schema = postgis_config.get("schema", "public")
    table_name = os.path.splitext(os.path.basename(shapefile))[0]

    pg_connection = f"PG:host={host} port={port} dbname={dbname} user={user}"
    load_cmd = [
        "ogr2ogr",
        "-f", "PostgreSQL",
        "-overwrite",
        "-nln", f"{schema}.{table_name}",
        "-lco", "GEOMETRY_NAME=geom",
        "-lco", f"SCHEMA={schema}",
        "-nlt", "PROMOTE_TO_MULTI",
        "-t_srs", "EPSG:2154",
        pg_connection,
        shapefile
    ]
    logging.info(f"Loading {shapefile} into PostGIS...")
    run_command(load_cmd)
    logging.info(f"Successfully loaded {shapefile} into PostGIS as table {table_name}.")
```

---

##### **d. Additional PostGIS Operations**

1. **Creating an SPGiST Index** :

```python
create_index_cmd = [
    "psql",
    "-h", host,
    "-p", str(port),
    "-d", dbname,
    "-c",
    f'CREATE INDEX idx_{table_name}_geom ON "{schema}"."{table_name}" USING SPGiST (geom);'
]
run_command(create_index_cmd)
logging.info(f"SPGiST index created successfully.")
```

2. **Validating CRS** :

```python
validate_crs_cmd = [
    "psql",
    "-h", host,
    "-p", str(port),
    "-d", dbname,
    "-c",
    f"SELECT Find_SRID('{schema}', '{table_name}', 'geom');"
]
output = run_command(validate_crs_cmd)
if "2154" not in output:
    logging.error(f"CRS is not set to EPSG:2154 for table {table_name}.")
    raise ValueError(f"CRS is not set to EPSG:2154 for table {table_name}.")
logging.info(f"CRS validated as EPSG:2154 for table {table_name}.")
```

2. **Setting Ownership and Granting Privileges** :

```python
set_owner_cmd = [
    "psql",
    "-h", host,
    "-p", str(port),
    "-d", dbname,
    "-c",
    f'ALTER TABLE "{schema}"."{table_name}" OWNER TO mahdi;'
]
grant_privileges_cmd = [
    "psql",
    "-h", host,
    "-p", str(port),
    "-d", dbname,
    "-c",
    f'GRANT ALL PRIVILEGES ON TABLE "{schema}"."{table_name}" TO mahdi;'
]
run_command(set_owner_cmd)
run_command(grant_privileges_cmd)
logging.info(f"Ownership set to mahdi and all privileges granted.")
```

---

#### **3. Main Functionality**

The `main` function orchestrates the entire process:

```python
import glob
from concurrent.futures import ThreadPoolExecutor

def process_preprocessed_shapefile(input_shp, postgis_config):
    validate_file(input_shp)
    layer_name = get_layer_name(input_shp)
    validate_geometries(input_shp)
    load_to_postgis(input_shp, layer_name, postgis_config)

def main():
    postgis_config = {
        "host": "localhost",
        "port": "5432",
        "dbname": "roofs",
        "user": "mahdi",
        "schema": "public"
    }
    shapefile_pattern = "/home/mahdi/interface/data/output/aligned_results*.shp"
    shapefiles = glob.glob(shapefile_pattern)
    if not shapefiles:
        logging.error(f"No shapefiles found matching the pattern: {shapefile_pattern}")
        return
    logging.info(f"Found {len(shapefiles)} shapefiles to process.")

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_preprocessed_shapefile, shp, postgis_config) for shp in shapefiles]
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logging.error(f"Dataset processing failed: {e}")

if __name__ == "__main__":
    main()
```

---

#### **4. Connection Management via `.pgpass`**

The script avoids hardcoding passwords by leveraging PostgreSQL's `.pgpass` file. The `.pgpass` file is located in the home directory of the user running the script and contains entries in the format:


```
hostname:port:database:username:password
```

![[x1OVFpaEEc.gif]]

This ensures secure password management without exposing credentials in the code.

---

#### **5. Deployment Considerations**

To deploy this script in a cloud environment or expose it through an API:

1. **Cloud Deployment** :
    - Use containerization tools like Docker to package the script.
    - Deploy on platforms like AWS ECS, Google Kubernetes Engine, or Azure Container Instances.
2. **API Integration** :
    - Wrap the script in a Django or Flask API to expose RESTful endpoints.
    - Secure the API using authentication mechanisms like OAuth2 or API keys.
3. **Monitoring and Logging** :
    
    - Integrate with cloud-native logging solutions (e.g., AWS CloudWatch, Google Cloud Logging) for better visibility.


---

### **Example of API Implementation using Django : **

#### **1. Project Setup**

First, ensure you have Django installed. If not, install it using:

```bash
pip install django djangorestframework
```



Create a new Django project and app:

```bash
django-admin startproject shapefile_loader

cd shapefile_loader

python manage.py startapp api
```



Add `'rest_framework'` and `'api'` to your `INSTALLED_APPS` in `settings.py`:

```python
INSTALLED_APPS = [
	# ...
	'rest_framework',
	'api',

]
```


---

#### **2. Models (Optional)**

If you want to track the status of uploaded shapefiles, you can create a model to store metadata:

```python
# api/models.py
from django.db import models

class ShapefileUpload(models.Model):
    file_path = models.CharField(max_length=255)
    status = models.CharField(max_length=50, default="pending")
    message = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.file_path
```


Run migrations:

```bash
python manage.py makemigrations
python manage.py migrate
```

---

#### **3. Serializer**

Define a serializer for the `ShapefileUpload` model if you're tracking uploads:

```python
# api/serializers.py
from rest_framework import serializers
from .models import ShapefileUpload

class ShapefileUploadSerializer(serializers.ModelSerializer):
    class Meta:
        model = ShapefileUpload
        fields = ['id', 'file_path', 'status', 'message', 'created_at']
```

---
#### **4. Views**

Create a view to handle the shapefile processing request:

```python
# api/views.py
import os
import glob
import logging
from concurrent.futures import ThreadPoolExecutor
from django.http import JsonResponse
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .models import ShapefileUpload
from .serializers import ShapefileUploadSerializer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Existing functions from the original script (run_command, validate_file, etc.)
# Include all the helper functions here (e.g., run_command, validate_file, get_layer_name, etc.)

class LoadShapefileAPIView(APIView):
    def post(self, request, *args, **kwargs):
        shapefile_path = request.data.get('shapefile_path')
        postgis_config = request.data.get('postgis_config', {})

        if not shapefile_path or not postgis_config:
            return Response({"error": "Missing required parameters"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            # Validate the file exists
            validate_file(shapefile_path)

            # Get the layer name
            layer_name = get_layer_name(shapefile_path)

            # Validate geometries
            validate_geometries(shapefile_path)

            # Load into PostGIS
            load_to_postgis(shapefile_path, layer_name, postgis_config)

            # Optionally save to the database
            upload_record = ShapefileUpload.objects.create(
                file_path=shapefile_path,
                status="success",
                message="Shapefile loaded successfully"
            )

            serializer = ShapefileUploadSerializer(upload_record)
            return Response(serializer.data, status=status.HTTP_200_OK)

        except Exception as e:
            logging.error(f"Error processing shapefile: {e}")
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Optional: List all uploads
class ListShapefileUploadsAPIView(APIView):
    def get(self, request, *args, **kwargs):
        uploads = ShapefileUpload.objects.all()
        serializer = ShapefileUploadSerializer(uploads, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

```

---

#### **5. URLs**

Define the API endpoints in `urls.py`:

```python
# api/urls.py
from django.urls import path
from .views import LoadShapefileAPIView, ListShapefileUploadsAPIView

urlpatterns = [
    path('load-shapefile/', LoadShapefileAPIView.as_view(), name='load_shapefile'),
    path('list-uploads/', ListShapefileUploadsAPIView.as_view(), name='list_uploads'),
]
```

Include the app's URLs in the main project:

```python
# api/urls.py
from django.urls import path
from .views import LoadShapefileAPIView, ListShapefileUploadsAPIView

urlpatterns = [
    path('load-shapefile/', LoadShapefileAPIView.as_view(), name='load_shapefile'),
    path('list-uploads/', ListShapefileUploadsAPIView.as_view(), name='list_uploads'),
]
```
---

#### **6. Example Usage**

**POST Request to Load Shapefile:** Send a POST request to `/api/load-shapefile/` with the following JSON payload:

```json
{
  "shapefile_path": "/path/to/aligned_results.shp",
  "postgis_config": {
    "host": "localhost",
    "port": "5432",
    "dbname": "roofs",
    "user": "mahdi",
    "schema": "public"
  }
}
```

**Response on Success:**

```json
{
  "id": 1,
  "file_path": "/path/to/aligned_results.shp",
  "status": "success",
  "message": "Shapefile loaded successfully",
  "created_at": "2023-10-01T12:00:00Z"
}
```


**GET Request to List Uploads:** Send a GET request to `/api/list-uploads/` to retrieve all uploaded shapefiles:

```json
[
  {
    "id": 1,
    "file_path": "/path/to/aligned_results.shp",
    "status": "success",
    "message": "Shapefile loaded successfully",
    "created_at": "2023-10-01T12:00:00Z"
  },
  {
    "id": 2,
    "file_path": "/path/to/another_result.shp",
    "status": "failed",
    "message": "Invalid geometries detected",
    "created_at": "2023-10-01T12:10:00Z"
  }
]
```
---

#### **7. Deployment to Cloud Services**

To deploy this Django API to a cloud service:

1. **Containerization with Docker** : Create a `Dockerfile`:

```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
```

    Build and push the image to a container registry (e.g., Docker Hub, AWS ECR).
    
2. **Cloud Hosting** : Deploy the containerized application to platforms like:
    
    - **AWS ECS** or **EKS** for container orchestration.
    - **Google Cloud Run** for serverless deployment.
    - **Heroku** for simplicity.
3. **Database Configuration** : Use a managed PostgreSQL database (e.g., AWS RDS, Google Cloud SQL) and configure the `.pgpass` file securely.
    
4. **API Gateway** : Expose the API behind an API Gateway (e.g., AWS API Gateway, NGINX) for better security and monitoring.
    

---

#### **8. Security Considerations**

- **Environment Variables** : Store sensitive information like database credentials in environment variables instead of hardcoding them.
- **Secrets Management** : Use secrets management tools like AWS Secrets Manager or HashiCorp Vault.
- **Authentication** : Protect the API with token-based authentication (e.g., JWT) or OAuth2.
- **Rate Limiting** : Use Django Rest Framework's throttling mechanisms to prevent abuse.

---

This implementation provides a robust and scalable way to process and load shapefiles into PostGIS via a Django API, making it suitable for cloud deployment and integration with other systems.