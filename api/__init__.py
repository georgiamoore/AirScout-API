from flask import Flask, jsonify, make_response, request
import os
from flask_restx import Api, Resource
from flask_cors import CORS
from dotenv import load_dotenv
from api.utils import (
    get_chart_format,
    get_feature_collection_between_timestamps,
    get_start_of_prev_day,
)
from .aston import *
from .defra import *
from config import config
import datetime
from datetime import timezone
from apscheduler.schedulers.background import BackgroundScheduler


year = datetime.date.today().year
load_dotenv()


def create_scheduler(app):
    def daily_fetch():
        with app.app_context():
            print(
                "[%s] Updating Aston and DEFRA data"
                % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
            end_timestamp = datetime.datetime.now().strftime("%d-%m-%Y")
            start_timestamp = (
                datetime.datetime.now() - datetime.timedelta(1)
            ).strftime("%d-%m-%Y")
            fetch_aston_readings(start_timestamp, end_timestamp)
            fetch_defra_readings([year, year + 1])
            # TODO add case for no new readings - rerun job in 1 hour

    sched = BackgroundScheduler(daemon=True)
    # should run daily at 00:05
    sched.add_job(
        daily_fetch,
        trigger="cron",
        hour="00",
        minute="05",
        next_run_time=datetime.datetime.now(tz=timezone.utc),
    )
    sched.start()


def create_app(app_environment=None):
    if app_environment is None:
        app = Flask(__name__)
        with app.app_context():
            app.config.from_object(config[os.getenv("FLASK_ENV", "dev")])
    else:
        app = Flask(__name__)
        with app.app_context():
            app.config.from_object(config[app_environment])

    with app.app_context():
        CORS(app)
        from .db import init_app

        init_app(app)
        api = Api(
            app, version="1.0", title="Air Quality API", description="Air Quality API"
        )
        app.config.SWAGGER_UI_DOC_EXPANSION = "list"
        create_scheduler(app)
    return app, api


app, api = create_app(os.getenv("FLASK_ENV", "dev"))
# TODO add api models for responses - see https://flask-restx.readthedocs.io/en/latest/quickstart.html#data-formatting


@api.route("/ping")
class Ping(Resource):
    def get(self):
        return jsonify(ping="pong")


@api.route("/aston")
class Aston(Resource):
    @api.doc(
        params={
            "days": {
                "description": "Determines how many previous days of data are fetched.",
                "in": "query",
                "type": "int",
            },
            "pollutants": {
                "description": "List of pollutants to return measurements for.",
                "in": "query",
                "type": "array",
                "collectionFormat": "multi",
                "items": {
                    "type": "string",
                    "enum": ["o3", "no", "no2", "pm1", "pm10", "pm2.5"],
                },
                "explode": "false",
            },
        }
    )
    def get(self):
        args = request.args
        pollutants = ["o3", "no", "no2", "pm1", "pm10", "pm2.5"]
        cols = ["sensor_id", "timestamp", "pressure", "humidity", "temperature"]
        if len(args.getlist("pollutants")) > 0:
            pollutants = args.getlist("pollutants")
            # TODO check validity of given list
        end_timestamp = datetime.datetime.now()
        days = args.get("days")
        if days is None:
            start_timestamp = get_start_of_prev_day(end_timestamp)
        else:
            start_timestamp = end_timestamp - datetime.timedelta(int(days))
        return {
            "source": "aston",
            "data": get_feature_collection_between_timestamps(
                start_timestamp,
                end_timestamp,
                cols,
                pollutants,
                "aston",
                "aston_sensor",
                "sensor_id",
                "sensor_location",
            ),
        }

    @api.doc(
        params={
            "start_timestamp": {
                "description": "Start timestamp for data to be fetched.",
                "in": "query",
                "type": "string",
                "example": "01-12-2020",
            },
            "end_timestamp": {
                "description": "End timestamp for data to be fetched.",
                "in": "query",
                "type": "string",
                "example": "02-12-2020",
            },
        }
    )
    def put(self):  # updating data
        args = request.args
        end_timestamp = args.get("end_timestamp")
        if end_timestamp is None:
            end_timestamp = datetime.datetime.now().strftime("%d-%m-%Y")
        start_timestamp = args.get("start_timestamp")
        if start_timestamp is None:
            start_timestamp = datetime.datetime.strptime(
                end_timestamp, "%d-%m-%Y"
            ) - datetime.timedelta(
                1
            )  # fetches previous day's data by default
            start_timestamp = start_timestamp.strftime("%d-%m-%Y")
        return fetch_aston_readings(start_timestamp, end_timestamp)


@api.route("/daqi")
class DAQI(Resource):
    def get(self):
        return get_defra_daqi()


@api.route("/defra")
class DEFRA(Resource):
    # todo should days be restricted to 1 day/week/month/year?
    @api.doc(
        params={
            "days": {
                "description": "Determines how many previous days of data are fetched.",
                "in": "query",
                "type": "int",
            },
            "pollutants": {
                "description": "List of pollutants to return measurements for.",
                "in": "query",
                "type": "array",
                "collectionFormat": "multi",
                "items": {
                    "type": "string",
                    "enum": ["o3", "no", "no2", "nox_as_no2", "pm10", "pm2.5", "so2"],
                },
                "explode": "false",
            },
        }
    )
    def get(self):
        args = request.args
        pollutants = ["o3", "no", "no2", "nox_as_no2", "pm10", "pm2.5", "so2"]
        cols = [
            "reading_id",
            "station_code",
            "station_name",
            "timestamp",
            "windspeed",
            "wind_direction",
            "temperature",
        ]
        if len(args.getlist("pollutants")) > 0:
            pollutants = args.getlist("pollutants")
            # TODO check validity of given list - exclude invalid pollutants here or handle later on? (likely both)
        # end_timestamp not entirely needed for now but could be useful for custom time periods later
        end_timestamp = datetime.datetime.now()
        days = args.get("days")
        if days is None:
            start_timestamp = get_start_of_prev_day(end_timestamp)
        else:
            start_timestamp = end_timestamp - datetime.timedelta(int(days))

        return {
            "source": "defra",
            "data": get_feature_collection_between_timestamps(
                start_timestamp,
                end_timestamp,
                cols,
                pollutants,
                "defra",
                "defra_station",
                "station_code",
                "station_location",
            ),
        }

    def put(self):
        return fetch_defra_readings([year, year + 1])


@api.route("/stats")
class Stats(Resource):
    @api.doc(
        params={
            "days": "An integer used to determine how many days of data are fetched."
        }
    )
    def get(self):
        args = request.args
        # todo default should be combined stats from all sources
        source = args.get("source", "defra")
        pollutants = ["o3", "no", "no2", "pm10", "pm2.5"]
        if len(args.getlist("pollutants")) > 0:
            pollutants = args.getlist("pollutants")
            # TODO add check for invalid pollutants
        days = args.get("days")
        # todo come back to this once finished prototyping w/ recharts on frontend
        type = args.get("type", "line")
        # ^ ideally should be able to use this api route to get all stats charts -> this param should be like line/bar/pie/calendar etc
        # TODO parameterise this if using this route for aston stats
        # or create some method to combine defra and aston stats ?
        cols = [
            # "reading_id",
            # "station_code",
            # "station_name",
            "timestamp",
            # "windspeed",
            # "wind_direction",
        ]
        return get_chart_format(days, cols, pollutants)


# TODO made obsolete after openair package change
@api.route("/rebuild_defra")
class Utility(Resource):
    #     # WIP utility route for recreating defra db
    #     def get(self):
    #         sites = ["BIRR", "BMLD", "BOLD"]  # default settings

    #         args = request.args
    #         if len(args.getlist("sites")) > 0:
    #             sites = args.getlist("sites")

    #         return fetch_defra_readings(sites, range(year - 1, year + 1))
    def put(self):
        return fetch_defra_stations()
