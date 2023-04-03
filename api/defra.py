import datetime
from flask import Response
import pandas as pd
from .db import get_db
from .utils import convert_df_to_db_format, get_openair
import rpy2
from rpy2.robjects.pandas2ri import rpy2py
import psycopg2

def get_defra_daqi():
    openair = get_openair()
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT station_code FROM public.defra_station")
        stations = [s[0] for s in cursor.fetchall()]
        rpy2.robjects.pandas2ri.activate()
        daqi = rpy2py(openair.importAURN(
                site=stations,
                year=datetime.date.today().year,
                data_type="daqi",
                pollutant="all",
            ))

        filtered = daqi[daqi['code'].isin(stations)].sort_values('date', ascending=False).groupby('pollutant').first().reset_index()
        response = Response(response=filtered.to_json(orient='records'),
            status=200,
            mimetype="application/json")
        return(response)

    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        cursor.close()
        # TODO fix error return format
        return "Error: %s" % error
    
def fetch_defra_readings(years):
    openair = get_openair()
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT station_code FROM public.defra_station")
        stations = [s[0] for s in cursor.fetchall()]
        rpy2.robjects.pandas2ri.activate()
        results = rpy2py(openair.importAURN(
            site=stations,
            year=years,
            data_type="hourly",
            pollutant="all",
        ))
        # results = results.where(pd.notnull(results), None)
        

        if len(results.index) > 0:
            return convert_df_to_db_format(
                results,
                conn,
                cursor,
                "public.defra",
                {
                    "date": "timestamp",
                    "code": "station_code",
                    "nox": "nox_as_no2",
                    "wd": "wind_direction",
                    "ws": "windspeed",
                    "air_temp": "temperature",
                },
            )
        else:
            return "No new sensor readings were found."
    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        cursor.close()
        # TODO fix error return format
        return "Error: %s" % error

    
def fetch_defra_stations():
    openair = get_openair()
    rpy2.robjects.pandas2ri.activate()
    meta_df = rpy2py(openair.importMeta(source="aurn",  all = True))
    birmingham_stations = meta_df[meta_df['local_authority'] == 'Birmingham'].groupby('code').first().reset_index()
    # TODO combine this with fetch_aston_readings in aston.py
    conn = get_db()
    cursor = conn.cursor()
    try:
        for _, row in birmingham_stations.iterrows():
            # add sensor if not already in db
            cursor.execute(
                """
            SELECT EXISTS(SELECT 1 FROM public.defra_station WHERE station_code = %s)
            """,
                (row["code"],),
            )
            exists = cursor.fetchone()[0]
            if not exists:
                cursor.execute(
                    "INSERT INTO public.defra_station (station_code, station_name, station_location) VALUES (%s, %s, ST_MakePoint(%s, %s))",
                    (
                        row["code"],
                        row["site"],
                        row["longitude"],
                        row["latitude"],
                    ),
                )
                conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        cursor.close()
        # TODO fix error return format
        print(error)
        return "Error: %s" % error

    cursor.close()
    return "DEFRA stations updated successfully."



def get_last_reading_timestamp_for_station(cursor, table_name, station_code):
    cursor.execute(
        "SELECT timestamp FROM %s WHERE station_code = '%s' order by timestamp desc nulls last limit 1"
        % (table_name, station_code)
    )
    row = cursor.fetchone()
    if not row:
        return datetime.datetime(year=2014, month=1, day=1)
    return row[0]
