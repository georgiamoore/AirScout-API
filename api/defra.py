import datetime
import math
import sys
import pandas as pd
from .db import get_db
from .utils import convert_df_to_db_format, get_openair
import rpy2
from rpy2.robjects.pandas2ri import rpy2py
import psycopg2

# from https://uk-air.defra.gov.uk/air-pollution/daqi?view=more-info&pollutant=pm25#pollutant
daqi_ranges = {
    "pm10": [
        {"range": range(0, 17), "daqi": 1},
        {"range": range(17, 34), "daqi": 2},
        {"range": range(34, 51), "daqi": 3},
        {"range": range(51, 59), "daqi": 4},
        {"range": range(59, 67), "daqi": 5},
        {"range": range(67, 76), "daqi": 6},
        {"range": range(76, 84), "daqi": 7},
        {"range": range(84, 92), "daqi": 8},
        {"range": range(92, 101), "daqi": 9},
        {"range": range(101, sys.maxsize), "daqi": 10},
    ],
    "pm2.5": [
        {"range": range(0, 12), "daqi": 1},
        {"range": range(12, 24), "daqi": 2},
        {"range": range(24, 36), "daqi": 3},
        {"range": range(36, 42), "daqi": 4},
        {"range": range(42, 48), "daqi": 5},
        {"range": range(48, 54), "daqi": 6},
        {"range": range(54, 59), "daqi": 7},
        {"range": range(59, 65), "daqi": 8},
        {"range": range(65, 71), "daqi": 9},
        {"range": range(71, sys.maxsize), "daqi": 10},
    ],
    "o3": [
        {"range": range(0, 34), "daqi": 1},
        {"range": range(34, 67), "daqi": 2},
        {"range": range(67, 101), "daqi": 3},
        {"range": range(101, 121), "daqi": 4},
        {"range": range(121, 141), "daqi": 5},
        {"range": range(141, 161), "daqi": 6},
        {"range": range(161, 188), "daqi": 7},
        {"range": range(188, 214), "daqi": 8},
        {"range": range(214, 241), "daqi": 9},
        {"range": range(241, sys.maxsize), "daqi": 10},
    ],
    "no2": [
        {"range": range(0, 68), "daqi": 1},
        {"range": range(68, 135), "daqi": 2},
        {"range": range(135, 201), "daqi": 3},
        {"range": range(201, 268), "daqi": 4},
        {"range": range(268, 335), "daqi": 5},
        {"range": range(335, 401), "daqi": 6},
        {"range": range(401, 468), "daqi": 7},
        {"range": range(468, 535), "daqi": 8},
        {"range": range(535, 601), "daqi": 9},
        {"range": range(601, sys.maxsize), "daqi": 10},
    ],
    "so2": [
        {"range": range(0, 89), "daqi": 1},
        {"range": range(89, 178), "daqi": 2},
        {"range": range(178, 267), "daqi": 3},
        {"range": range(267, 355), "daqi": 4},
        {"range": range(355, 444), "daqi": 5},
        {"range": range(444, 533), "daqi": 6},
        {"range": range(533, 711), "daqi": 7},
        {"range": range(711, 888), "daqi": 8},
        {"range": range(888, 1065), "daqi": 9},
        {"range": range(1065, sys.maxsize), "daqi": 10},
    ],
}

# matches calculated pollutant mean value with DAQI index
def get_daqi_mapping(pollutant, value):
    for entry in daqi_ranges[pollutant]:
        if not math.isnan(value):
            if int(value) in entry["range"]:
                print(entry["range"])
                return entry["daqi"]
    return -1


def get_defra_daqi():
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT station_code FROM public.defra_station")

        # get past 24 hours of data
        end_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        start_timestamp = (end_timestamp - datetime.timedelta(1)).strftime(
            "%Y-%m-%d %H:%M:%S%z"
        )
        end_timestamp = end_timestamp.strftime("%Y-%m-%d %H:%M:%S%z")
        cursor.execute(
            "SELECT * FROM public.defra WHERE timestamp between timestamp '%s' and timestamp '%s'"
            % (start_timestamp, end_timestamp)
        )
        df = pd.DataFrame(cursor.fetchall())
        df.columns = [x.name for x in cursor.description]

    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        cursor.close()
        # TODO fix error return format -> use Flask response
        return "Error: %s" % error

    cursor.close()
    # filter by station, creating a new df for each
    station_dfs = [x for _, x in df.groupby("station_code")]

    station_info = []
    # for each station, generates pollutant mean values for relevant time period
    # then maps those mean values to DAQI
    # adds mean & DAQI for each pollutant to station object
    # adds station object to station_info array
    for station_df in station_dfs:
        # PM2.5 & PM10 24 hour mean
        pm25_mean = station_df["pm2.5"].mean()
        pm10_mean = station_df["pm10"].mean()

        # O3 8 hour mean
        o3_mean = (
            station_df.resample("8H", on="timestamp")
            .agg({"o3": "mean"})
            .mean()
            .values[0]
        )

        # NO2 hourly mean
        no2_mean = (
            station_df.resample("1H", on="timestamp")
            .agg({"no2": "mean"})
            .mean()
            .values[0]
        )

        # SO2 15 min mean
        so2_mean = (
            station_df.resample("15M", on="timestamp")
            .agg({"so2": "mean"})
            .mean()
            .values[0]
        )

        # TODO this should be tidied up to map object based on generic pollutant array
        # but it is 5th April and there are more important things to do right now !!!!
        means = [pm25_mean, pm10_mean, o3_mean, no2_mean, so2_mean]

        if not all(
            value is None or value == -1 for value in means
        ):  # excludes station if no data found
            station_info.append(
                {
                    station_df["station_code"].iloc[0]: {
                        key: value
                        for key, value in {
                            "pm2.5_mean": pm25_mean,
                            "pm2.5_daqi": get_daqi_mapping("pm2.5", pm25_mean),
                            "pm10_mean": pm10_mean,
                            "pm10_daqi": get_daqi_mapping("pm10", pm10_mean),
                            "o3_mean": o3_mean,
                            "o3_daqi": get_daqi_mapping("o3", o3_mean),
                            "no2_mean": no2_mean,
                            "no2_daqi": get_daqi_mapping("no2", no2_mean),
                            "so2_mean": so2_mean,
                            "so2_daqi": get_daqi_mapping("so2", so2_mean),
                        }.items()
                        if not math.isnan(value)
                        and value != -1  # excludes pollutant if no data found
                    }
                }
            )

    return station_info


def fetch_defra_readings(years):
    openair = get_openair()
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT station_code FROM public.defra_station")
        stations = [s[0] for s in cursor.fetchall()]
        rpy2.robjects.pandas2ri.activate()
        results = rpy2py(
            openair.importAURN(
                site=stations,
                year=years,
                data_type="hourly",
                pollutant="all",
            )
        )
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
    meta_df = rpy2py(openair.importMeta(source="aurn", all=True))
    # birmingham_stations = meta_df[meta_df['local_authority'] == 'Birmingham'].groupby('code').first().reset_index()
    west_mids_stations = (
        meta_df[meta_df["zone"] == "West Midlands"]
        .groupby("code")
        .first()
        .reset_index()
    )
    # TODO combine this with fetch_aston_readings in aston.py
    conn = get_db()
    cursor = conn.cursor()
    try:
        for _, row in west_mids_stations.iterrows():
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
