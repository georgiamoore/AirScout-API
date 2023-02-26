import requests
import os
from dotenv import load_dotenv
import json
from geojson import Feature, Point, FeatureCollection


def get_current_aq():
    load_dotenv()
    url = "https://api.waqi.info/feed/birmingham/?token=" + \
        os.getenv('WAQI_TOKEN')
    # could use /feed/geo::lat;:lng/?token=:token instead
    response = requests.get(url)
    response = response.json()
    location = Point((response["data"]["city"]["geo"]
                     [1], response["data"]["city"]["geo"][0]))
    # TODO reconsider preemptively returning this as a featurecollection
    #   overkill for a single feature but consistent w typing in frontend, 
    #   useful for adding multiple stations later?
    # TODO can also get daily average info from this route
    return FeatureCollection([Feature(geometry=location, properties={ 
        "idx": response["data"]["idx"],
        "latitude": response["data"]["city"]["geo"][0],
        "longitude": response["data"]["city"]["geo"][1],
        "no2": response["data"]["iaqi"]["no2"]["v"],
        "pm10": response["data"]["iaqi"]["pm10"]["v"],
        "pm25": response["data"]["iaqi"]["pm25"]["v"],
        "humidity": response["data"]["iaqi"]["h"]["v"],
        "pressure": response["data"]["iaqi"]["p"]["v"],
        "temp": response["data"]["iaqi"]["t"]["v"],
        "wind": response["data"]["iaqi"]["w"]["v"],
        "utc_date": response["data"]["time"]["s"]
    })])
    

def get_historical_aq():
    f = open('GeoObs.json', "r")
    data = json.loads(f.read())
    f.close()
    return data
