#!/bin/sh
export FLASK_APP=./api/__init__.py
pipenv run flask --debug run -h 0.0.0.0