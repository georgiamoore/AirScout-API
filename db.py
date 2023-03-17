from flask import current_app
from flask import g
import psycopg2
import os

def get_db():
    if "conn" not in g:
        g.conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            database=os.getenv('POSTGRES_DATABASE'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'))
    return g.conn

def close_db(e=None):
    db = g.pop("conn", None)

    if db is not None:
        db.close()

def init_app(app):
    app.teardown_appcontext(close_db)