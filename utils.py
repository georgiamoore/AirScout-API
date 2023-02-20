import pandas as pd
import csv, json
from geojson import Feature, FeatureCollection, Point

def import_waqi_daily_avg_csv(filename, latitude, longitude):
    # pd.read_csv(filename)
    # utc_date =
    # no2 = 
    # pm10 = 
    # pm25 = 

    features = []
    with open(filename, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for date, pm25, pm10, o3, no2, so2 in reader:
            latitude, longitude = map(float, (latitude, longitude))
            features.append(
                Feature(
                    geometry = Point((longitude, latitude)),
                    properties = {
                        'utc_date': date,
                        'pm25': pm25,
                        'pm10': pm10,
                        'o3': o3,
                        'no2': no2,
                        'so2': so2,
                    }
                )
            )

    collection = FeatureCollection(features)
    with open("GeoObs.json", "w") as f:
        f.write('%s' % collection)


import_waqi_daily_avg_csv('birmingham-a4540 roadside-air-quality.csv', 52.476145, -1.874978)
# import_waqi_daily_avg_csv('birmingham-ladywood-air-quality.csv', 52.481346, -1.918235)