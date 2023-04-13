import datetime
import psycopg2
import requests
import os
from dotenv import load_dotenv
import geopandas as gpd
from flask import Response
from .db import get_db
from .utils import convert_df_to_db_format

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
            "O3_mean": "o3",
            "NO_mean": "no",
            "NO2_mean": "no2",
            "particulatePM1_mean": "pm1",
            "particulatePM10_mean": "pm10",
            "particulatePM2.5_mean": "pm2.5",
            "ambPressure_mean": "pressure",
            "ambHumidity_mean": "humidity",
            "ambTempC_mean": "temperature",
        }
        return convert_df_to_db_format(df, conn, cursor, "public.aston", cols)
    print(
        "[%s] No Aston sensor readings found."
        % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    return Response(
        "No sensor readings found for this timeframe.",
        status=404,
    )
