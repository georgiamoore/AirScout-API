import psycopg2
import requests
import os
from dotenv import load_dotenv
import json
import geojson
import geopandas as gpd
from flask import Response
from geojson import Polygon
from .db import get_db
from .utils import convert_df_to_db_format


def get_sensor_summary(start_date, end_date):
    # load_dotenv()
    # url = os.getenv('ASTON_API_URL') + \
    #     "/sensor-summary/as-geojson?start="+start_date+"&end="+end_date+"&averaging_frequency=H&averaging_methods=mean"
    # response = requests.get(url)
    # response = response.json()

    # temp local copy of data - avoiding api spam

    # with open("aston.json", "w") as f:
    #     f.write('%s' % json.dumps(response))

    f = open("aston.json", "r")
    response = geojson.loads(f.read())
    f.close()

    if len(response) > 0:
        return format_sensor_summary_as_geojson(response)
    return Response(
        "No sensor readings found for this timeframe.",
        status=404,
    )


def format_sensor_summary_as_geojson(summary):
    collection = []
    for sensor in summary:
        for feature in sensor["geojson"]["features"]:
            feature["properties"]["sensor_id"] = sensor["sensorid"]
        collection.extend(sensor["geojson"]["features"])

    df = gpd.GeoDataFrame.from_features(collection)
    points = df.copy()
    # converting polygon -> point based on centre
    # https://gis.stackexchange.com/questions/216788/convert-polygon-feature-centroid-to-points-using-python
    points.geometry = points["geometry"].centroid
    points.crs = df.crs
    points = points.rename(
        columns={"particulatePM10mean": "pm10"}
    )  # TODO rename other cols
    return points.to_json()


def fetch_aston_readings(start_date, end_date):
    load_dotenv()
    url = (
        os.getenv("ASTON_API_URL")
        + "/sensor-summary/as-geojson?start="
        + start_date
        + "&end="
        + end_date
        + "&averaging_frequency=H&averaging_methods=mean"
    )
    response = requests.get(url)
    response = response.json()

    if len(response) > 0:
        # TODO combine this with add_defra_stations in defra.py
        conn = get_db()
        cursor = conn.cursor()
        collection = []

        # extracting features for each sensor from geojson
        for sensor in response:
            for feature in sensor["geojson"]["features"]:
                feature["properties"]["sensor_id"] = sensor["sensorid"]
            collection.extend(sensor["geojson"]["features"])

        df = gpd.GeoDataFrame.from_features(collection)
        sensors = df.groupby("sensor_id").first().reset_index()
        try:
            for _, row in sensors.iterrows():
                # add sensor if not already in db
                cursor.execute(
                    """
                SELECT EXISTS(SELECT 1 FROM public.aston_sensor WHERE sensor_id = %s)
                """,
                    (row["sensor_id"],),
                )
                exists = cursor.fetchone()[0]
                if not exists:
                    cursor.execute(
                        "INSERT INTO public.aston_sensor (sensor_id, sensor_location) VALUES (%s, ST_MakePoint(%s, %s))",
                        (
                            row["sensor_id"],
                            row["geometry"].centroid.x,
                            row["geometry"].centroid.y,
                        ),
                    )
                    conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            conn.rollback()
            cursor.close()
            # TODO fix error return format
            return "Error: %s" % error

        cols = {
            "datetime_UTC": "timestamp",
            "O3mean": "o3",
            "NOmean": "no",
            "NO2mean": "no2",
            "particulatePM1mean": "pm1",
            "particulatePM10mean": "pm10",
            "particulatePM2.5mean": "pm2.5",
            "ambPressuremean": "pressure",
            "ambHumiditymean": "humidity",
            "ambTempCmean": "temperature",
        }
        return convert_df_to_db_format(df, conn, cursor, "public.aston", cols)
    return Response(
        "No sensor readings found for this timeframe.",
        status=404,
    )
