import requests
import os
from dotenv import load_dotenv
import json
import geojson
import geopandas as gpd

def get_sensor_summary(start_date, end_date):
    # load_dotenv()
    # url = os.getenv('ASTON_API_URL') + \
    #     "/sensor-summary/as-geojson?start="+start_date+"&end="+end_date+"&averaging_frequency=H&averaging_methods=mean"
    # response = requests.get(url)
    # response = response.json()    
 
    # temp local copy of data - avoiding api spam

    # with open("aston.json", "w") as f:
    #     f.write('%s' % json.dumps(response[0]["geojson"]))

    f = open('aston.json', "r")
    response = geojson.loads(f.read())
    f.close()

    df = gpd.GeoDataFrame.from_features(response)
    # converting polygon -> point based on centre
    # https://gis.stackexchange.com/questions/216788/convert-polygon-feature-centroid-to-points-using-python
    points = df.copy()
    points.geometry = points['geometry'].centroid
    points.crs = df.crs
    return(points.to_json())
