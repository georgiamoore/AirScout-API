import datetime
from pyaurn import importAURN
from db import get_db
import numpy as np
import pandas as pd
from geojson import Feature, Point, FeatureCollection
import geopandas as gpd
from utils import convert_df_to_db_format

# TODO this is disgusting and needs to be parameterised/tidied up 
# was just to test request speed - but it works!
def get_historic_birr():
    # TODO use this to prevent hardcoding lat/long
    # (causes issues with retrieving pollutant list)
    # meta = importMeta()
    # valid_stations = meta[(meta['site_name'].str.contains('Birmingham')) & (meta['end_date']=='ongoing')]
    s1 = convert_defra_to_feature_list("BIRR", range(2023, 2024), ['O3', 'NO', 'NO2','NOXasNO2', 'PM10', 'PM2.5'], 52.476145,  -1.874978)
    # s2 = convert_to_feature_list("BMLD", range(2023, 2024), ['O3', 'NO', 'NO2','NOXasNO2', 'SO2', 'PM10', 'PM2.5'], 52.481346,  -1.918235)
    # s3 = convert_to_feature_list("BOLD", range(2023, 2024), ['NO', 'NO2','NOXasNO2'], 52.502436,  -2.003497)
    # TODO - returning all 3 sensors is too slow to be loaded by the frontend map
    # return FeatureCollection(np.concatenate((s1,s2,s3)).tolist()) 
    return FeatureCollection(s1)

def get_historic_bmld():
    s2 = convert_defra_to_feature_list("BMLD", range(2023, 2024), ['O3', 'NO', 'NO2','NOXasNO2', 'SO2', 'PM10', 'PM2.5'], 52.481346,  -1.918235)
    return FeatureCollection(s2)

def get_historic_bold():
    s3 = convert_defra_to_feature_list("BOLD", range(2023, 2024), ['NO', 'NO2','NOXasNO2'], 52.502436,  -2.003497)
    return FeatureCollection(s3)

def convert_defra_to_feature_list(site, years, pollutant_list, latitude, longitude):
    df = importAURN(site, years, pollutant=pollutant_list)
    df = df.fillna('')
    df = df.rename(columns = {'PM10':'pm10'}) #TODO rename other cols
    df['date'] = df['date'].astype(str)
    location = Point((longitude, latitude))
    features = []
    for row in df.itertuples(index=False):
        # print(df.columns.values) 
        # print(row["code"])
        features.append(Feature(geometry=location, properties={
            ""+col+"": row[i] for i, col in enumerate(df.columns.values)
        }))

    return features

# TODO this could(/should?) add all sites at once if no specific site param given
def fetch_defra_readings(sites, years):
    # testing params:
    #"BIRR", range(2021, 2022), ['O3', 'NO', 'NO2','NOXasNO2', 'PM10', 'PM2.5']
    
    conn = get_db()
    cursor = conn.cursor()
    all_station_dfs = list(map(lambda site: filter_station_readings(site, years, cursor), sites))
    df = pd.concat(all_station_dfs)
    # df = df.fillna('')

    if len(df.index) > 0:
        return convert_df_to_db_format(df, conn, cursor, 'public.defra', {'date':'utc_date', 'code':'station_code', 'O3':'o3', 'NO':'no', 'NO2':'no2', 'NOXasNO2':'nox_as_no2', 'SO2':'so2', 'PM10':'pm10', 'PM2.5':'pm25'})
    else:
        return "No new sensor readings were found."
   
def filter_station_readings(site, years, cursor):
    df = importAURN(site, years)
    # filter df by last timestamp to only add new readings to db
    last_reading_timestamp = get_last_reading_timestamp_for_station(cursor, 'public.defra', site)
    df.drop(df[df.date <= last_reading_timestamp].index, inplace=True)
    
    # site name is not required in db due to station_code fk from defra_station table
    df.drop('site', axis=1, inplace=True)
    return df



def get_last_reading_timestamp(cursor, table_name):
    cursor.execute("SELECT timestamp FROM %s order by timestamp desc limit 1" % table_name)
    row = cursor.fetchone()
    if not row:
        return datetime.datetime(year=2014, month=1, day=1)
    return row[0]

def get_last_reading_timestamp_for_station(cursor, table_name, station_code):
    cursor.execute("SELECT timestamp FROM %s WHERE station_code = '%s' order by timestamp desc limit 1" % (table_name, station_code))
    row = cursor.fetchone()
    if not row:
        return datetime.datetime(year=2014, month=1, day=1)
    return row[0]

def get_defra_features_between_timestamps(start_timestamp, end_timestamp, pollutants):
    pollutants_str = ", ".join(['ds."' + p + '"' for p in pollutants])
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT row_to_json(fc) FROM 
        ( SELECT 'FeatureCollection' As type, array_to_json(array_agg(f)) As features FROM
            ( SELECT 'Feature' As type, 
                ST_AsGeoJSON(ds.station_location)::json As geometry, 
                (
                    select row_to_json(t) 
                    from (select ds."reading_id", ds."station_code", ds."station_name", ds."timestamp", 
                        %s, ds."windspeed", ds."wind_direction", ds."temperature") t
                )
                As properties
            FROM (public.defra d inner join defra_station s using (station_code) ) As ds 
            WHERE ds.timestamp BETWEEN timestamp '%s' and timestamp '%s'   ) As f )  As fc;
        """ % (pollutants_str, start_timestamp, end_timestamp))
    # TODO handle undefined columns
    feature_collection = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return feature_collection

#TODO consider if these should consider the full time period (e.g. all of a month rather than truncated by week)
# would use end_timestamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0) for start of month
def get_chart_format(days, pollutants):
    end_timestamp = datetime.datetime.now() 
    if days is None:
        start_timestamp = end_timestamp - datetime.timedelta(365)
    elif int(days) == 1: 
        start_timestamp = get_start_of_prev_day(end_timestamp)
    else: 
        start_timestamp =  end_timestamp - datetime.timedelta(int(days))
    feature_collection = get_defra_features_between_timestamps(start_timestamp, end_timestamp, pollutants)
    df = gpd.GeoDataFrame.from_features(feature_collection)
    df = df[["timestamp", *pollutants]]
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df[df.columns.intersection(pollutants)] = df[df.columns.intersection(pollutants)].astype('float')

    if days is None:
        return { 
            'year': group_df(df, 'M', "%B %Y"),
            'month': group_df(df[(df['timestamp'] > end_timestamp-datetime.timedelta(30)) & (df['timestamp'] <= end_timestamp)], 'W', "%d-%m-%Y"),
            'week': group_df(df[(df['timestamp'] > end_timestamp-datetime.timedelta(7)) & (df['timestamp'] <= end_timestamp)], 'D', "%a %d"),
            'yesterday': group_df(df[(df['timestamp'] > get_start_of_prev_day(end_timestamp)) & (df['timestamp'] <= end_timestamp)], 'H', "%H:%M"),
        }
    if int(days) > 30:
        # grouping by month
        return group_df(df, 'M', "%B %Y")
    elif int(days) > 7:
        # group by week
        # TODO not sure if dates returned by this one are very clear?
        # revisit after recharts prototyping
        return group_df(df, 'W', "%d-%m-%Y")
    elif int(days) > 1:
        # group by day
        return group_df(df, 'D', "%a %d")
    elif int(days) == 1:
        # yesterday's data
        return group_df(df, 'H', "%H:%M")


def group_df(df, period, timestamp_format):
    g = df.set_index('timestamp')
    g = g.resample(period).mean()
    g.index = g.index.strftime(timestamp_format)
    return g.reset_index().to_json(orient='records')

# used to get a full day of data (DEFRA only updates at the end of a day)
def get_start_of_prev_day(end_timestamp):
    day_start = end_timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    return day_start - datetime.timedelta(hours=24)
