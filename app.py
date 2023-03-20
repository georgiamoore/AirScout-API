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

@app.route('/update_defra')
def update_defra_readings():
    sites = ["BIRR", "BMLD"] # default settings
    
    args = request.args
    if len(args.getlist('sites')) >0:
        sites = args.getlist('sites')
  
    # TODO fix pollutant list keyerror when adding BOLD station

    # todo parameterise years
    # todo parameterise pollutant list
    return fetch_defra_readings(sites, range(year, year+1), ['O3', 'NO', 'NO2','NOXasNO2', 'PM10', 'PM2.5'])

# todo should days be restricted to 1 day/week/month/year?
@app.route('/defra')
def get_defra_readings():
    days = request.args.get('days', 1)
    # end_timestamp not entirely needed for now but could be useful for custom time periods later
    end_timestamp = datetime.datetime.now() 
    start_timestamp =  end_timestamp - datetime.timedelta(int(days))
    return get_defra_features_between_timestamps(start_timestamp, end_timestamp)

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

