from flask import Flask, jsonify
from flask_cors import CORS
import psycopg2
import os
from dotenv import load_dotenv
from waqi import *

load_dotenv()

app = Flask(__name__)
CORS(app)

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"
    

def db_conn():
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        database=os.getenv('POSTGRES_DATABASE'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'))

    cur = conn.cursor()

    # Execute a query
    cur.execute("SELECT * FROM public.plume_sensor LIMIT 5")

    # Retrieve query results
    records = cur.fetchall()
    print(records)

@app.route('/waqi')
def get_waqi():
    return jsonify(get_current_aq())

@app.route('/waqi-archive')
def get_waqi_archive():
    return get_historical_aq()

# plume = [
#     {'id': '1', 'utcDate': 5000, 'no2': 1, 'voc': 1, 'pm1': 1,
#         'pm10': 1, 'pm25': 1, 'latitude': 1, 'longitude': 1}
# ]


# @app.route('/plume')
# def get_plume():
#     return jsonify(plume)


# @app.route('/plume', methods=['POST'])
# def add_plume():
#     plume.append(request.get_json())
#     return '', 204

# todo routes for aggregated data by timeframe

