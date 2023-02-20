import requests
import os
from dotenv import load_dotenv
import json

def get_current_aq():
    load_dotenv()
    url = "https://api.waqi.info/feed/birmingham/?token=" + \
        os.getenv('WAQI_TOKEN')
    # could use /feed/geo::lat;:lng/?token=:token instead
    response = requests.get(url)
    response = response.json()
    idx = response["data"]["idx"]
    latitude = response["data"]["city"]["geo"][0]
    longitude = response["data"]["city"]["geo"][1]
    no2 = response["data"]["iaqi"]["no2"]["v"]
    pm10 = response["data"]["iaqi"]["pm10"]["v"]
    pm25 = response["data"]["iaqi"]["pm25"]["v"]
    # humidity = response["data"]["iaqi"]["h"]["v"]
    # pressure = response["data"]["iaqi"]["p"]["v"]
    # so2 = response["data"]["iaqi"]["so2"]["v"]
    # temp = response["data"]["iaqi"]["t"]["v"]
    # wind = response["data"]["iaqi"]["w"]["v"]
    utc_date = response["data"]["time"]["s"]
    return ({"utc_date": utc_date, "no2": no2, "pm10": pm10, "pm25": pm25, "latitude": latitude, "longitude": longitude, "idx": idx})


def get_historical_aq():
    f = open ('GeoObs.json', "r")
    data = json.loads(f.read())
    f.close()
    return data