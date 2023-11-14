import h3
import rasterio
import geopandas as gpd
from rasterstats import gen_zonal_stats
from tqdm import tqdm
import numpy as np
from shapely.geometry import Polygon
import json

resolution = 8
geoTiff = 'geotiff.tif'
geoJsonFileName = "geojson-%s.geojson" % resolution

def hex_to_polygon(hex_id):
    vertices = h3.h3_to_geo_boundary(hex_id, geo_json=True)
    return Polygon(vertices)

with rasterio.open(geoTiff) as src:
    ndval = src.nodatavals[0]
    bounds = src.bounds
    affine = src.transform
    array = src.read(1)
    array = array.astype('float64')
    array[array == ndval] = np.nan
    crs = src.crs

    print("Opening", geoTiff)

bbox = [bounds.bottom, bounds.left, bounds.top, bounds.right]

hexbin_ids = list(h3.polyfill(
    {
        'type': 'Polygon',
        'coordinates': [[
            [bbox[0], bbox[1]],
            [bbox[2], bbox[1]],
            [bbox[2], bbox[3]],
            [bbox[0], bbox[3]],
            [bbox[0], bbox[1]]
        ]],
    },
    resolution
))

hexbin_polygons = [hex_to_polygon(hex_id) for hex_id in hexbin_ids]

print("Creating hexgrid...")

gdf = gpd.GeoDataFrame({'geometry': hexbin_polygons}, crs=crs.to_string())

# Add the 'mean_value' column to the GeoDataFrame
gdf['mean_value'] = np.nan

print(len(gdf), "Hexagons Generated")
print("H3 Resolution:", resolution)
print("Bounding Box:", bbox)

chunk_size = 10000
num_chunks = len(gdf) // chunk_size + 1

print("num_chunks",num_chunks)

combined_geojson = {"type": "FeatureCollection", "features": []}

for chunk_num in range(num_chunks):
    start_idx = chunk_num * chunk_size
    end_idx = min((chunk_num + 1) * chunk_size, len(gdf))

    chunk_gdf = gdf.iloc[start_idx:end_idx]

    with tqdm(total=len(chunk_gdf), desc=f"Processing Chunk {chunk_num+1}/{num_chunks}", unit="hexagon") as chunk_pbar:
        gen = gen_zonal_stats(vectors=chunk_gdf, raster=geoTiff, stats=['mean'])

        for i, v in zip(chunk_gdf.index, gen):
            gdf.at[i, 'mean_value'] = v['mean']
            chunk_pbar.update(1)

    chunk_geojson = json.loads(chunk_gdf.to_json())
    combined_geojson["features"].extend(chunk_geojson["features"])

# Write combined GeoJSON to file
with open(geoJsonFileName, 'w') as f:
    json.dump(combined_geojson, f)

def filter_shapes(geojson_data):
    features = geojson_data['features']
    features = [feature for feature in features if feature['properties']['mean_value'] is not None]

    geojson_data['features'] = features
    return geojson_data

def process_geojson(file_path):
    try:
        with open(file_path, 'r') as file:
            geojson_data = json.load(file)
        filtered_geojson = filter_shapes(geojson_data)
        with open(file_path, 'w') as file:
            json.dump(filtered_geojson, file)

        totalFeaturesWithValues = len(filtered_geojson['features'])
        allHexes = len(gdf) - totalFeaturesWithValues

        print(totalFeaturesWithValues, "Null Hexagons removed")
        print(allHexes, "Hexagons with values found")
        print("GeoJSON file generated as", geoJsonFileName)
    except Exception as e:
        print(f"Error: {e}")

process_geojson(geoJsonFileName)