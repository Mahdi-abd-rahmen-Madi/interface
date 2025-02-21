import pandas as pd
import geopandas as gpd
from shapely import geometry
from shapely.ops import unary_union
import mercantile
from tqdm import tqdm
import os
import tempfile
import streamlit as st
from streamlit_folium import st_folium
import folium
import matplotlib.pyplot as plt

# Define AOI geometry
aoi_geom = {
    "coordinates": [
        [
            [2.34259459, 48.878840725470376],
            [2.3549457008283583, 48.87855841547037],
            [2.3545693208283587, 48.86985804547037],
            [2.342220060828359,	48.87014030547038],
            [2.34259459, 48.878840725470376],
        ]
    ],
    "type": "Polygon",
}
aoi_shape = geometry.shape(aoi_geom)
minx, miny, maxx, maxy = aoi_shape.bounds

output_fn = "example_building_footprints.geojson"
quad_keys = set()

# Get tiles intersecting the AOI
for tile in list(mercantile.tiles(minx, miny, maxx, maxy, zooms=9)):
    quad_keys.add(mercantile.quadkey(tile))
quad_keys = list(quad_keys)
print(f"The input area spans {len(quad_keys)} tiles: {quad_keys}")

# Load dataset links
df = pd.read_csv(
    "https://minedbuildings.z5.web.core.windows.net/global-buildings/dataset-links.csv", dtype=str
)

# Remove duplicate QuadKeys (if any)
df = df.drop_duplicates(subset="QuadKey", keep="first")

# Process each tile
idx = 0
combined_gdf = gpd.GeoDataFrame()
with tempfile.TemporaryDirectory() as tmpdir:
    tmp_fns = []
    for quad_key in tqdm(quad_keys):
        rows = df[df["QuadKey"] == quad_key]
        if rows.shape[0] == 1:
            url = rows.iloc[0]["Url"]
            print(f"Processing QuadKey {quad_key} with URL: {url}")  # Debugging
            df2 = pd.read_json(url, lines=True)
            df2["geometry"] = df2["geometry"].apply(geometry.shape)
            gdf = gpd.GeoDataFrame(df2, crs=4326)
            fn = os.path.join(tmpdir, f"{quad_key}.geojson")
            tmp_fns.append(fn)
            if not os.path.exists(fn):
                gdf.to_file(fn, driver="GeoJSON")
        elif rows.shape[0] > 1:
            raise ValueError(f"Multiple rows found for QuadKey: {quad_key}")
        else:
            raise ValueError(f"QuadKey not found in dataset: {quad_key}")

    # Merge all GeoJSON files into a single GeoDataFrame
    for fn in tmp_fns:
        gdf = gpd.read_file(fn)
        gdf = gdf[gdf.geometry.within(aoi_shape)]
        print(f"Downloaded {len(gdf)} geometries for QuadKey {quad_key}.")  # Debugging
        gdf['id'] = range(idx, idx + len(gdf))
        idx += len(gdf)
        combined_gdf = pd.concat([combined_gdf, gdf], ignore_index=True)

# Save the combined GeoDataFrame to a file
combined_gdf = combined_gdf.to_crs('EPSG:4326')
combined_gdf.to_file(output_fn, driver='GeoJSON')

# Load the saved GeoJSON file
try:
    combined_gdf = gpd.read_file("example_building_footprints.geojson")
except FileNotFoundError:
    print("Error: example_building_footprints.geojson not found. Please run the building footprint extraction code first.")
    exit()

# Check if the GeoDataFrame is empty
if combined_gdf.empty:
    print("No building footprints found within the AOI.")
    exit()

# Compute the centroid of the combined geometry
combined_geometry = unary_union(combined_gdf.geometry)
center = combined_geometry.centroid

# Create the Folium map
m = folium.Map(location=[center.y, center.x], zoom_start=12, tiles=None)

# Add Google Earth tile layer
folium.TileLayer(
    tiles='https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
    attr='Google',
    name='Google Earth',
    overlay=False,
    control=True,
    show=True,
).add_to(m)

# Add building footprints as a GeoJSON layer
folium.GeoJson(
    combined_gdf,
    name="buildings",
    style_function=lambda feature: {
        'color': 'red',
        'weight': 2,
        'fillOpacity': 0.3,
        'opacity': 0.7
    }
).add_to(m)

# Add LayerControl
folium.LayerControl().add_to(m)

# Save the map to an HTML file
m.save("building_footprints_map_google_earth.html")

# Display the map using Streamlit
st_folium(m, width=700, height=500)
print("Map displayed using Streamlit. HTML version saved to building_footprints_map_google_earth.html")