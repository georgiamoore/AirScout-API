import datetime
import numpy as np
import psycopg2
import psycopg2.extras as extras
from .db import get_db
import geopandas as gpd
import pandas as pd
from rpy2.robjects.packages import importr, quiet_require
from rpy2.rinterface_lib.embedded import RRuntimeError
from flask import Response, g, jsonify
from flask import current_app


def get_openair():
    if "openair" not in g:
        utils = importr("utils")
        try:
            g.openair = importr("openair")
        except RRuntimeError:
            utils.chooseCRANmirror(ind=1)
            utils.install_packages("openair")
            g.openair = importr("openair")
    return g.openair


def convert_df_to_db_format(df, conn, cursor, table_name, renamed_cols):
    print("[%s] Updating %s." % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), table_name))
    df = df.rename(columns=renamed_cols)
    # removing columns that don't exist in db
    cursor.execute("SELECT * FROM %s LIMIT 0" % (table_name,))
    db_cols = [desc[0] for desc in cursor.description]
    df = df[df.columns.intersection(db_cols)]
    cursor.execute("SELECT * FROM %s" % (table_name,))
    original_row_count = cursor.rowcount
    # print (cursor.rowcount)

    # convert df to list of tuples for bulk insert to db
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ", ".join(f'"{c}"' for c in df.columns.tolist())
    query = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT DO NOTHING" % (table_name, cols)

    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
        cursor.execute("SELECT * FROM %s" % (table_name,))
        updated_row_count = cursor.rowcount
        # print (cursor.rowcount)
        print(
            "[%s] %s rows inserted into %s."
            % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            updated_row_count - original_row_count,
            table_name)
        )
    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        cursor.close()
        # TODO fix error return format -> use Flask response
        return "Error: %s" % error

    cursor.close()
    return "Sensor readings inserted successfully."


def get_feature_collection_between_timestamps(
    start_timestamp,
    end_timestamp,
    columns,
    pollutants,
    reading_table,
    sensor_table,
    sensor_pkey_column,
    sensor_location_column,
):
    columns_str = ", ".join(['ds."' + c + '"' for c in columns])
    pollutants_str = ", ".join(['ds."' + p + '"' for p in pollutants])
    query = """
            SELECT row_to_json(fc) FROM 
            ( SELECT 'FeatureCollection' As type, array_to_json(array_agg(f)) As features FROM
                ( SELECT 'Feature' As type, 
                    ST_AsGeoJSON(ds.%s)::json As geometry, 
                    (
                        select row_to_json(t) 
                        from (select %s) t
                    )
                    As properties
                FROM (public.%s d inner join %s s using (%s) ) As ds 
                WHERE ds.timestamp BETWEEN timestamp '%s' and timestamp '%s'   ) As f )  As fc;
            """ % (
        sensor_location_column,
        ", ".join([columns_str, pollutants_str]),
        reading_table,
        sensor_table,
        sensor_pkey_column,
        start_timestamp,
        end_timestamp,
    )
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(query)
    except psycopg2.InterfaceError as err:
        print(err)
        # conn = get_db()
        conn = psycopg2.connect(
            host=current_app.config["HOST"],
            database=current_app.config["DATABASE"],
            user=current_app.config["USER"],
            password=current_app.config["PASSWORD"],
        )
        cursor = conn.cursor()
        cursor.execute(query)
    # TODO handle undefined columns
    feature_collection = cursor.fetchone()[0]
    # TODO could aggregate data if end-start > 1 day
    cursor.close()
    conn.close()
    return feature_collection


# TODO consider if these should consider the full time period (e.g. all of a month rather than truncated by week)
# would use end_timestamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0) for start of month
def get_chart_format(days, cols, pollutants):
    end_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
    if days is None:
        start_timestamp = end_timestamp - datetime.timedelta(365)
    elif int(days) == 1:
        start_timestamp = get_start_of_prev_day(end_timestamp)
    else:
        start_timestamp = end_timestamp - datetime.timedelta(int(days))
    defra_feature_collection = get_feature_collection_between_timestamps(
        start_timestamp,
        end_timestamp,
        cols + ["temperature", "reading_id"],
        pollutants + ["nox_as_no2", "so2"],
        "defra",
        "defra_station",
        "station_code",
        "station_location",
    )
    aston_feature_collection = get_feature_collection_between_timestamps(
        start_timestamp,
        end_timestamp,
        cols,
        pollutants,
        "aston",
        "aston_sensor",
        "sensor_id",
        "sensor_location",
    )

    if (
        defra_feature_collection["features"] is None
        and aston_feature_collection["features"] is None
    ):
        return "No sensor readings were found for this timeframe."
    defra_df = gpd.GeoDataFrame.from_features(defra_feature_collection)
    aston_df = gpd.GeoDataFrame.from_features(aston_feature_collection)
    df = pd.concat([defra_df, aston_df])
    pollutants = pollutants + [
        "nox_as_no2",
        "so2",
    ]  # todo this is an ugly way to handle different pollutant cols
    df = df[["timestamp", *pollutants]]
    df["timestamp"] = pd.to_datetime(
        df["timestamp"], utc=True, format="%Y-%m-%dT%H:%M:%S%z"
    )

    df[df.columns.intersection(pollutants)] = df[
        df.columns.intersection(pollutants)
    ].astype("float")

    if days is None:
        return {
            "year": group_df(df, "M", "%B %Y"),
            "month": group_df(
                df[
                    (df["timestamp"] > end_timestamp - datetime.timedelta(30))
                    & (df["timestamp"] <= end_timestamp)
                ],
                "W",
                "%d-%m-%Y",
            ),
            "week": group_df(
                df[
                    (df["timestamp"] > end_timestamp - datetime.timedelta(7))
                    & (df["timestamp"] <= end_timestamp)
                ],
                "D",
                "%a %d",
            ),
            "yesterday": group_df(
                df[
                    (df["timestamp"] > get_start_of_prev_day(end_timestamp))
                    & (df["timestamp"] <= end_timestamp)
                ],
                "H",
                "%H:%M",
            ),
        }

    if int(days) > 30:
        # grouping by month
        return group_df(df, "M", "%B %Y")
    elif int(days) > 7:
        # group by week
        # TODO not sure if dates returned by this one are very clear?
        # revisit after recharts prototyping
        return group_df(df, "W", "%d-%m-%Y")
    elif int(days) > 1:
        # group by day
        return group_df(df, "D", "%a %d")
    elif int(days) == 1:
        # yesterday's data
        return group_df(df, "H", "%H:%M")


def group_df(df, period, timestamp_format):
    g = df.set_index("timestamp")
    g = g.resample(period).mean()
    g.index = g.index.strftime(timestamp_format)
    g = g.ffill()
    result = g.reset_index().apply(lambda x: x.dropna().to_dict(), axis=1)
    result = [
        {k: v for k, v in d.items() if v != 0} for d in result.tolist()
    ]  # removing 0 values
    return result


# used to get a full day of data (DEFRA only updates at the end of a day)
def get_start_of_prev_day(end_timestamp):
    day_start = end_timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    return day_start - datetime.timedelta(hours=24)
