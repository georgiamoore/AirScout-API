from flask import Flask, jsonify
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

@app.route('/defra')
def get_defra():
    # return db_format_testing("BIRR", range(year, year+1), ['O3', 'NO', 'NO2','NOXasNO2', 'PM10', 'PM2.5'])
    return db_format_testing("BIRR", range(2022, 2023), ['O3', 'NO', 'NO2','NOXasNO2', 'PM10', 'PM2.5'])

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

