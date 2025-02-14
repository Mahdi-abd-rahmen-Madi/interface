import subprocess

command = [
    "ogr2ogr", "Troof.shp", "roof.shp",
    "-dialect", "sqlite",
    "-sql", "SELECT ST_Translate(Geometry, -1.056 ,7.32) AS geometry, * FROM roof"
]

subprocess.run(command)