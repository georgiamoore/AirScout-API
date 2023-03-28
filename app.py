from flask import Flask, request
import os
from flask_cors import CORS
from dotenv import load_dotenv
from aston import *
from waqi import *
from plume import *
from defra import *
from config import config
import datetime
year = datetime.date.today().year
load_dotenv()

def create_app(app_environment=None):
    if app_environment is None:
        app = Flask(__name__)
        app.config.from_object(config[os.getenv('FLASK_ENV', 'dev')])
    else:
        app = Flask(__name__)
        app.config.from_object(config[app_environment])
    CORS(app)
    from db import init_app
    init_app(app)

    return app

app = create_app(os.getenv('FLASK_ENV', 'dev'))

# @app.route("/")
# def hello_world():
#     return "<p>Hello, World!</p>"

@app.route('/aston')
def get_aston_readings():
    return {'source':'aston', 'data': get_sensor_summary('14-02-2023','26-02-2023')}

@app.route('/update_aston')
def update_aston_readings():
    return fetch_aston_readings('20-03-2023','23-03-2023')

@app.route('/update_defra')
def update_defra_readings():
    sites = ["BIRR", "BMLD", "BOLD"] # default settings
    
    args = request.args
    if len(args.getlist('sites')) > 0:
        sites = args.getlist('sites')
  
    # TODO fix pollutant list keyerror when adding BOLD station

    # todo parameterise years
    # todo parameterise pollutant list (broken on change to python 3.11)
    return fetch_defra_readings(sites, range(year, year+1))

# todo should days be restricted to 1 day/week/month/year?
@app.route('/defra')
def get_defra_readings():
    args = request.args
    pollutants = ['O3', 'NO', 'NO2', 'NOXasNO2', 'PM10', 'PM2.5', 'SO2']
    if len(args.getlist('pollutants')) > 0:
        pollutants = args.getlist('pollutants')
        # TODO check validity of given list - exclude invalid pollutants here or handle later on? (likely both)
    # end_timestamp not entirely needed for now but could be useful for custom time periods later
    end_timestamp = datetime.datetime.now() 
    days = args.get('days')
    if days is None:
        start_timestamp = get_start_of_prev_day(end_timestamp)
    else:
        start_timestamp = end_timestamp - datetime.timedelta(int(days))
    return {'source':'defra', 'data': get_defra_features_between_timestamps(start_timestamp, end_timestamp, pollutants)}


@app.route('/stats')
def get_stats():
    args = request.args
    source = args.get('source', 'defra') # todo default should be combined stats from all sources
    pollutants = ['O3', 'NO', 'NO2', 'NOXasNO2', 'PM10', 'PM2.5', 'SO2']
    if len(args.getlist('pollutants')) >0:
            pollutants = args.getlist('pollutants')
    days = args.get('days')
    type = args.get('type', 'line') # todo come back to this once finished prototyping w/ recharts on frontend
    # ^ ideally should be able to use this api route to get all stats charts -> this param should be like line/bar/pie/calendar etc

    
    return get_chart_format(days, pollutants)

#WIP utility route for recreating defra db
@app.route('/rebuild_defra')
def rebuild_defra_db():
    sites = ["BIRR", "BMLD", "BOLD"] # default settings
    
    args = request.args
    if len(args.getlist('sites')) > 0:
        sites = args.getlist('sites')

    return fetch_defra_readings(sites, range(year-1, year+1))

