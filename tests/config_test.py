import datetime
from flask import current_app
import mock
import pandas as pd
import psycopg2
import pytest
from unittest.mock import patch
import requests
import requests_mock

from api import app
from api import create_app

import os

from api.db import get_db


# @pytest.fixture
# def app():
#     # app, api = create_app(os.getenv("FLASK_ENV", "test"))

#     yield app


@pytest.fixture
def client():
    with app.test_client() as client:
        os.environ["FLASK_ENV"] = "test"
        app.config.update(
            {
                "TESTING": True,
            }
        )
        yield client


#
# @pytest.fixture()
# def client(app):
#     app.config.update({
#         "TESTING": True,
#     })
#     return app.test_client()


def test_api_ping(client):
    res = client.get("/ping")
    assert res.json == {"ping": "pong"}


# def test_put_aston(client, mocker):
#     res = client.put("/aston")
#     assert res.json == {"ping": "pong"}


# def test_get_aston(client, requests_mock):
def test_get_aston(client, mocker):
    #     mocker.patch('api.aston.call_aston_api', return_value={"ping": "pong"})
    #     end_timestamp = datetime.datetime.now().strftime("%d-%m-%Y")
    #     start_timestamp = (
    #                 datetime.datetime.now() - datetime.timedelta(1)
    #             ).strftime("%d-%m-%Y")
    #     url = (
    #         os.getenv("ASTON_API_URL")
    #         + "/sensor-summary/as-geojson?start="
    #         + start_timestamp
    #         + "&end="
    #         + end_timestamp
    #         + "&averaging_frequency=H&averaging_methods=mean"
    #     )

    #     # mock_get.return_value.status_code = 200
    #     # mock_get.return_value.json.return_value = {"ping": "pong"}
    # requests_mock.get(url, json={"ping": "pong"})
    mocker.patch("api.aston.requests.get", return_value={"ping": "pong"})
    res = client.get("/aston")
    assert res.json == {"ping": "pong"}


def test_simple(requests_mock):
    requests_mock.get("http://test.com", text="data")
    assert "data" == requests.get("http://test.com").text


def test_put_defra(client, mocker):
    with app.app_context():
        conn = psycopg2.connect(
            host=current_app.config["HOST"],
            database=current_app.config["DATABASE"],
            user=current_app.config["USER"],
            password=current_app.config["PASSWORD"],
        )
        cursor = conn.cursor()
        cursor.execute("DELETE FROM public.defra")
        mocked_AURN_data = {
            "date": ["2023-01-01 00:00:00", "2023-01-01 01:00:00", "2023-01-01 02:00:00"],
            "O3": [61, 65, 66],
            "NO": [9, 6, 6],
            "NO2": [12, 9, 9],
            "NOXasNO2": [26, 20, 18],
            "PM10": [9, 7, 8],
            "PM2.5": [7, 4, 4],
            "wd": [228, 229, 228],
            "ws": [6.8, 7.3, 6.9],
            "temp": [7.7, 7.3, 6.9],
            "site": [
                "Birmingham A4540 Roadside",
                "Birmingham A4540 Roadside",
                "Birmingham A4540 Roadside",
            ],
            "code": ["BIRR", "BIRR", "BIRR"],
        }
        mocker.patch("api.defra.importAURN", return_value=pd.DataFrame(mocked_AURN_data))
        res = client.get("/defra")
        conn = psycopg2.connect(
            host=current_app.config["HOST"],
            database=current_app.config["DATABASE"],
            user=current_app.config["USER"],
            password=current_app.config["PASSWORD"],
        )
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM public.defra")
        assert cursor.fetchall() == []
        assert res.json == {"ping": "pong"}


def test_get_defra(client, mocker):
    # mocker.patch(
    #     'api.aston.requests.get',
    #     return_value={"ping": "pong"}
    # )
    res = client.get("/defra")
    assert res.json == {"ping": "pong"}
