from pyaurn import importAURN
from db import get_db
import psycopg2
import numpy as np
import psycopg2.extras as extras
from geojson import Feature, Point, FeatureCollection
import datetime
year = datetime.date.today().year
# TODO this is disgusting and needs to be parameterised/tidied up 
# was just to test request speed - but it works!
def get_historic_birr():
    # TODO use this to prevent hardcoding lat/long
    # (causes issues with retrieving pollutant list)
    # meta = importMeta()
    # valid_stations = meta[(meta['site_name'].str.contains('Birmingham')) & (meta['end_date']=='ongoing')]
    s1 = convert_to_feature_list("BIRR", range(2023, 2024), ['O3', 'NO', 'NO2','NOXasNO2', 'PM10', 'PM2.5'], 52.476145,  -1.874978)
    # s2 = convert_to_feature_list("BMLD", range(2023, 2024), ['O3', 'NO', 'NO2','NOXasNO2', 'SO2', 'PM10', 'PM2.5'], 52.481346,  -1.918235)
    # s3 = convert_to_feature_list("BOLD", range(2023, 2024), ['NO', 'NO2','NOXasNO2'], 52.502436,  -2.003497)
    # TODO - returning all 3 sensors is too slow to be loaded by the frontend map
    # return FeatureCollection(np.concatenate((s1,s2,s3)).tolist()) 
    return FeatureCollection(s1)

def get_historic_bmld():
    s2 = convert_to_feature_list("BMLD", range(2023, 2024), ['O3', 'NO', 'NO2','NOXasNO2', 'SO2', 'PM10', 'PM2.5'], 52.481346,  -1.918235)
    return FeatureCollection(s2)

def get_historic_bold():
    s3 = convert_to_feature_list("BOLD", range(2023, 2024), ['NO', 'NO2','NOXasNO2'], 52.502436,  -2.003497)
    return FeatureCollection(s3)


def convert_to_feature_list(site, years, pollutant_list, latitude, longitude):
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

def db_format_testing():
    return convert_defra_to_db_format("BIRR", range(year, year+1), ['O3', 'NO', 'NO2','NOXasNO2', 'PM10', 'PM2.5'])
    # return convert_defra_to_db_format("BIRR", range(2021, 2022), ['O3', 'NO', 'NO2','NOXasNO2', 'PM10', 'PM2.5'])

def convert_defra_to_db_format(site, years, pollutant_list):
    df = importAURN(site, years, pollutant=pollutant_list)
    # df = df.fillna('')
    df = df.drop('site', axis=1)
    conn = get_db()
    cursor = conn.cursor()
   
    tuples = [tuple(x) for x in df.to_numpy()]
    print(tuples[0])

    # TODO avoid hardcoding column/table names here
    table_name = 'public.defra'
    cols = '"station_code", "timestamp", "O3", "NO", "NO2", "NOXasNO2", "PM10", "PM2.5", "windspeed", "wind_direction", "temperature"'
    
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table_name, cols)
   
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        cursor.close()
        return("Error: %s" % error)

    cursor.close()
    return "Sensor readings inserted successfully."

