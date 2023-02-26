import requests
import os
from dotenv import load_dotenv
import json
import geojson
import geopandas as gpd
from flask import Response

def get_sensor_summary(start_date, end_date):
    # load_dotenv()
    # url = os.getenv('ASTON_API_URL') + \
    #     "/sensor-summary/as-geojson?start="+start_date+"&end="+end_date+"&averaging_frequency=H&averaging_methods=mean"
    # response = requests.get(url)
    # response = response.json()    
 
    # temp local copy of data - avoiding api spam

    # with open("aston.json", "w") as f:
    #     f.write('%s' % json.dumps(response))

    f = open('aston.json', "r")
    response = geojson.loads(f.read())
    f.close()
    
    if len(response)>0:
        return format_sensor_summary_as_geojson(response)
    return Response(
        "No sensor readings found for this timeframe.",
        status=404,
    )


def format_sensor_summary_as_geojson(summary):
    collection=[]
    for sensor in summary:
        for feature in sensor["geojson"]["features"]:
            feature["properties"]["sensor_id"] = sensor["sensorid"] 
        collection.extend(sensor["geojson"]["features"])
    
    df = gpd.GeoDataFrame.from_features(collection)
    points = df.copy()
    # converting polygon -> point based on centre
    # https://gis.stackexchange.com/questions/216788/convert-polygon-feature-centroid-to-points-using-python
    points.geometry = points['geometry'].centroid
    points.crs = df.crs
    points = points.rename(columns = {'particulatePM10mean':'pm10'}) #TODO rename other cols
    return(points.to_json())
    