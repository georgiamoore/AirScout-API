from flask import Flask, jsonify, request
import os
from flask_cors import CORS
from dotenv import load_dotenv
from aston import *
from waqi import *
from plume import *
from defra import *
from db import *
import datetime
year = datetime.date.today().year
load_dotenv()

app = Flask(__name__)
CORS(app)

init_app(app)

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"
    


@app.route('/waqi')
def get_waqi():
    return jsonify(get_current_aq())

@app.route('/waqi-archive')
def get_waqi_archive():
    return get_historical_aq()

@app.route('/plume')
def get_plume():
    plume = get_readings_in_bbox()
    return jsonify(plume)

@app.route('/aston')
def get_aston():
    return get_sensor_summary('14-02-2023','26-02-2023')

@app.route('/fetch_defra')
def fetch_defra():
    return fetch_defra_readings("BIRR", range(year, year+1), ['O3', 'NO', 'NO2','NOXasNO2', 'PM10', 'PM2.5'])

# todo period should be only day/week/month/year
@app.route('/defra')
def get_defra():
    args = request.args
    # period = args.get('period')
    # end_timestamp not entirely needed for now but could be useful for custom time periods later
    end_timestamp = datetime.datetime.now() 
    start_timestamp =  end_timestamp - datetime.timedelta(days = 1)
    return get_defra_features_by_timestamp(start_timestamp, end_timestamp)


@app.route('/defra_birr')
def get_defra_birr():
    return get_historic_birr()

@app.route('/defra_bmld')
def get_defra_bmld():
    return get_historic_bmld()

@app.route('/defra_bold')
def get_defra_bold():
    return get_historic_bold()



# @app.route('/plume', methods=['POST'])
# def add_plume():
#     plume.append(request.get_json())
#     return '', 204

# todo routes for aggregated data by timeframe

