import datetime
import math
import sys
import pandas as pd
from .db import get_db
from .utils import convert_df_to_db_format
import psycopg2
from pyaurn import importAURN, importMeta
import warnings

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

daqi_measurement_periods = {
    "pm2.5": "24H",
    "pm10": "24H",
    "o3": "8H",
    "no2": "1H",
    "so2": "15M",
}

# matches calculated pollutant mean value with DAQI index
def get_daqi_mapping(pollutant, value):
    for entry in daqi_ranges[pollutant]:
        if not math.isnan(value):
            if int(value) in entry["range"]:
                return entry["daqi"]
    return -1


def generate_pollutant_means(df):
    means = {}
    for pollutant, measurement_period in daqi_measurement_periods.items():
        means[pollutant] = (
            df.resample(measurement_period, on="timestamp")
            .agg({pollutant: "mean"})
            .mean()
            .values[0]
        )
    return means


def get_past_48_hours():
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT station_code FROM public.defra_station")

        # get past 48 hours of data
        end_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        start_timestamp = (end_timestamp - datetime.timedelta(2)).strftime(
            "%Y-%m-%d %H:%M:%S%z"
        )
        end_timestamp = end_timestamp.strftime("%Y-%m-%d %H:%M:%S%z")
        cursor.execute(
            """
            SELECT public.defra.*, public.defra_station.station_name FROM public.defra 
            JOIN public.defra_station ON public.defra_station.station_code = public.defra.station_code 
            WHERE timestamp between timestamp '%s' and timestamp '%s'
            """
            % (start_timestamp, end_timestamp)
        )
        df = pd.DataFrame(cursor.fetchall())
        if df.empty:
            # TODO set function to 24 hrs, attempt rerun here with 48 hrs
            return "No data found for this time period"

        df.columns = [x.name for x in cursor.description]
        cursor.close()
        return df

    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        cursor.close()
        # TODO fix error return format -> use Flask response
        return "Error: %s" % error


def get_daqi_by_pollutant():
    df = get_past_48_hours()
    if type(df) == str or df.empty:
        return "No data found for this time period"

    # filter by station, creating a new df for each
    station_dfs = [x for _, x in df.groupby("station_code")]

    pollutants = {
        "pm10": {
            "mean": 0,
            "daqi": 0,
            "station_code": "",
            "station_name": "",
            "measurement_period": "",
        },
        "pm2.5": {
            "mean": 0,
            "daqi": 0,
            "station_code": "",
            "station_name": "",
            "measurement_period": "",
        },
        "o3": {
            "mean": 0,
            "daqi": 0,
            "station_code": "",
            "station_name": "",
            "measurement_period": "",
        },
        "no2": {
            "mean": 0,
            "daqi": 0,
            "station_code": "",
            "station_name": "",
            "measurement_period": "",
        },
        "so2": {
            "mean": 0,
            "daqi": 0,
            "station_code": "",
            "station_name": "",
            "measurement_period": "",
        },
    }

    # for each station, generates pollutant mean values for relevant time period
    # then maps those mean values to DAQI
    # keeps track of highest mean/daqi value and corresponding station information
    for station_df in station_dfs:
        means = generate_pollutant_means(station_df)

        if not all(
            value is None or value == -1 for value in means.values()
        ):  # excludes station if no data found
            for pollutant, mean in means.items():
                if (
                    not math.isnan(mean) and mean != -1
                ):  # excludes pollutant if no data found
                    if (
                        mean > pollutants[pollutant]["mean"]
                    ):  # finding highest mean value for each pollutant
                        pollutants[pollutant]["mean"] = mean
                        pollutants[pollutant]["daqi"] = get_daqi_mapping(
                            pollutant, mean
                        )
                        pollutants[pollutant]["station_code"] = station_df[
                            "station_code"
                        ].iloc[0]
                        pollutants[pollutant]["station_name"] = station_df[
                            "station_name"
                        ].iloc[0]
                        pollutants[pollutant][
                            "measurement_period"
                        ] = daqi_measurement_periods[pollutant]
    return pollutants


def get_daqi_by_station():
    df = get_past_48_hours()
    if type(df) == str or df.empty:
        return "No data found for this time period"

    # filter by station, creating a new df for each
    station_dfs = [x for _, x in df.groupby("station_code")]

    station_info = []
    # for each station, generates pollutant mean values for relevant time period
    # then maps those mean values to DAQI
    # adds mean & DAQI for each pollutant to station object
    # adds station object to station_info array
    for station_df in station_dfs:
        pollutants = ["pm2.5", "pm10", "o3", "no2", "so2"]
        means = generate_pollutant_means(station_df)

        if not all(
            value is None or value == -1 for value in means
        ):  # excludes station if no data found
            station_info.append(
                {
                    station_df["station_code"].iloc[0]: {
                        **{
                            key: value
                            for pollutant in pollutants
                            for key, value in {
                                f"{pollutant}_mean": means[pollutant],
                                f"{pollutant}_daqi": get_daqi_mapping(
                                    pollutant, means[pollutant]
                                ),
                            }.items()
                            if not math.isnan(value)
                            and value != -1  # excludes pollutant if no data found
                        },
                        "station_name": station_df["station_name"].iloc[0],
                    },
                },
            )

    return station_info


def fetch_defra_readings(years):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT station_code FROM public.defra_station")
        stations = [s[0] for s in cursor.fetchall()]
        warnings.filterwarnings(
            "ignore",
            message="Some data files were not able to be downloaded, check resulting DataFrame carefully",
        )
        warnings.filterwarnings("ignore", message="Resulting DataFrame is empty")
        all_station_dfs = list(
            map(lambda site: importAURN(site=site, years=years), stations)
        )
        results = pd.concat(all_station_dfs)

        if len(results.index) > 0:
            return convert_df_to_db_format(
                results,
                conn,
                cursor,
                "public.defra",
                {
                    "date": "timestamp",
                    "code": "station_code",
                    "O3": "o3",
                    "NO": "no",
                    "NO2": "no2",
                    "NOXasNO2": "nox_as_no2",
                    "SO2": "so2",
                    "PM10": "pm10",
                    "PM2.5": "pm2.5",
                    "wd": "wind_direction",
                    "ws": "windspeed",
                    "temp": "temperature",
                },
            )
        else:
            print(
                "[%s] No Defra sensor readings found."
                % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
            return "No new sensor readings were found."
    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        cursor.close()
        # TODO fix error return format
        return "Error: %s" % error


def fetch_defra_stations():
    meta_df = importMeta(source="aurn")
    # birmingham_stations = meta_df[meta_df['local_authority'] == 'Birmingham'].groupby('site_id').first().reset_index()
    west_mids_stations = (
        meta_df[meta_df["zone"] == "West Midlands"]
        .groupby("site_id")
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
                (row["site_id"],),
            )
            exists = cursor.fetchone()[0]
            if not exists:
                cursor.execute(
                    "INSERT INTO public.defra_station (station_code, station_name, station_location) VALUES (%s, %s, ST_MakePoint(%s, %s))",
                    (
                        row["site_id"],
                        row["site_name"],
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
