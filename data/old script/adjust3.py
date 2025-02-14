import subprocess

command = [
    "ogr2ogr", "Proof.shp", "roof.shp",
    "-dialect", "ogrsql"
    "-sql", "SELECT ST_Affine(geometry, 1, 0, 0, 1, -1.056, 7.32) AS geometry, * FROM roof"
]

subprocess.run(command)
