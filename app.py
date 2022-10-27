from flask import Flask

app = Flask(__name__)

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"
    
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
