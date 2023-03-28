import datetime
from pyaurn import importAURN
from .db import get_db
import numpy as np
import pandas as pd
from geojson import Feature, Point, FeatureCollection
import geopandas as gpd
from .utils import convert_df_to_db_format, get_feature_collection_between_timestamps


def convert_defra_to_feature_list(site, years, pollutant_list, latitude, longitude):
    df = importAURN(site, years, pollutant=pollutant_list)
    df = df.fillna('')
    df = df.rename(columns = {'PM10':'pm10'}) #TODO rename other cols
    df['date'] = df['date'].astype(str)
    location = Point((longitude, latitude))
    features = []
    for row in df.itertuples(index=False):
        features.append(Feature(geometry=location, properties={
            ""+col+"": row[i] for i, col in enumerate(df.columns.values)
        }))

    return features

# TODO this could(/should?) add all sites at once if no specific site param given
def fetch_defra_readings(sites, years):
    conn = get_db()
    cursor = conn.cursor()
    all_station_dfs = list(map(lambda site: filter_station_readings(site, years, cursor), sites))
    df = pd.concat(all_station_dfs)

    if len(df.index) > 0:
        return convert_df_to_db_format(df, conn, cursor, 'public.defra', {'date':'timestamp', 'code':'station_code', 'O3':'o3', 'NO':'no', 'NO2':'no2', 'NOXasNO2':'nox_as_no2', 'SO2':'so2', 'PM10':'pm10', 'PM2.5':'pm2.5', 'wd':'wind_direction', 'ws':'windspeed', 'temp':'temperature'})
    else:
        return "No new sensor readings were found."
   
def filter_station_readings(site, years, cursor):
    df = importAURN(site, years)
    # filtering df by last timestamp to only add new readings to db
    df.date = df.date.dt.tz_localize(tz='Europe/London')
    last_reading_timestamp = get_last_reading_timestamp_for_station(cursor, 'public.defra', site)
    df.drop(df[df.date <= last_reading_timestamp].index, inplace=True)

    # site name is not required in db due to station_code fk from defra_station table
    df.drop('site', axis=1, inplace=True)
    return df




def get_last_reading_timestamp_for_station(cursor, table_name, station_code):
    cursor.execute("SELECT timestamp FROM %s WHERE station_code = '%s' order by timestamp desc nulls last limit 1" % (table_name, station_code))
    row = cursor.fetchone()
    if not row:
        return datetime.datetime(year=2014, month=1, day=1)
    return row[0]


