from flask import current_app
from flask import g
import psycopg2

def get_db():
    if "conn" not in g:
        g.conn = psycopg2.connect(
            host=current_app.config['HOST'],
            database=current_app.config['DATABASE'],
            user=current_app.config['USER'],
            password=current_app.config['PASSWORD'])
    return g.conn

def close_db(e=None):
    db = g.pop("conn", None)

    if db is not None:
        db.close()

def init_app(app):
    app.teardown_appcontext(close_db)