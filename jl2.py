import h3
import rasterio
import geopandas as gpd
from rasterstats import gen_zonal_stats
from tqdm.auto import tqdm
import numpy as np
from shapely.geometry import Polygon, mapping
import json
import time
from joblib import Parallel, delayed

start_time = time.time()

resolution = 8
chunk_size = 1000
numChunkJobs = 4
geoTiff = '1kmtest.tif'
geoJsonFileName = "joblib-%s.geojson" % resolution


def hex_to_polygon(hex_id):
    vertices = h3.h3_to_geo_boundary(hex_id, geo_json=True)
    return Polygon(vertices)


# Open the GeoTIFF with an explicit geotransform
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

gdf = gpd.GeoDataFrame({'geometry': hexbin_polygons}, crs=crs.to_string())

# Add the 'mean_value' column to the GeoDataFrame
gdf['mean_value'] = np.nan

print(len(gdf), "Hexagons Generated")
print("H3 Resolution:", resolution)
print("Bounding Box:", bbox)


chunk_start_time = time.time()

totalHexagons = len(gdf)
numChunks = len(gdf) // chunk_size

combined_geojson = {"type": "FeatureCollection", "features": []}


def process_chunk(chunk_id, chunk_gdf):
    chunk_geojson = {"type": "FeatureCollection", "features": []}

    if (numChunks > 0):
        percentage = chunk_id / numChunks * 100
    else:
        percentage = 100

    chunk_end_time = time.time()
    chunk_elapsed_time = chunk_end_time - chunk_start_time
    chunk_elaspse_time_minutes = round(chunk_elapsed_time / 60,2)

    with tqdm(total=len(chunk_gdf), desc=f"Processing Chunk {chunk_id} of {numChunks} [{percentage:.2f}% at {chunk_elaspse_time_minutes}m]", position=0, leave=True) as pbarChunk:
        for i, hex_id in enumerate(chunk_gdf.index):
            poly_geojson = mapping(chunk_gdf.loc[hex_id, 'geometry'])
            stats_gen = gen_zonal_stats(poly_geojson, geoTiff, stats=['mean'],all_touched=True)
            stats_list = list(stats_gen)

            if stats_list and 'mean' in stats_list[0]:
                mean_value = stats_list[0]['mean']
            else:
                mean_value = None

            feature = {
                "type": "Feature",
                "geometry": poly_geojson,
                "properties": {"mean_value": mean_value}
            }

            chunk_geojson["features"].append(feature)
            pbarChunk.update(1)  # Update the inner progress bar

    return chunk_geojson


if __name__ == '__main__':
    # Run chunks in parallel using joblib
    with tqdm(total=numChunks, desc="Processing Chunks", position=numChunks) as pbar_chunks:
        for chunk_id, chunk_geojson in enumerate(
                Parallel(n_jobs=numChunkJobs)(
                        delayed(process_chunk)(i, gdf.iloc[start_idx:end_idx]) for i, (start_idx, end_idx) in
                        enumerate(zip(range(0, len(gdf), chunk_size),
                                        range(chunk_size, len(gdf) + chunk_size, chunk_size))))):
            combined_geojson["features"].extend(chunk_geojson["features"])
            pbar_chunks.update(1)  # Update the outer progress bar

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

    end_time = time.time()
    print("FINISHED BUILDING", totalHexagons, " HEXAGONS")
    elapsed_time = end_time - start_time
    elaspse_time_minutes = elapsed_time / 60

    print("Elapsed Seconds: ", round(elapsed_time, 3))
    print("Elapsed Minutes: ", round(elaspse_time_minutes, 2))
