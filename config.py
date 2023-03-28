import os

# TODO implement these config files
# can then access secrets using this syntax (as in db.py):
# from flask import current_app
# current_app.config['HOST']
# format from https://stackoverflow.com/questions/68869525/how-to-deploy-a-flask-application-with-a-config-file


class ProdConfig:
    # Database configuration
    # API_TOKEN = os.environ.get('PROD_MARKET_STACK_API_KEY_SECRET')
    HOST = os.getenv("POSTGRES_HOST")


class DevConfig:
    # Database configuration
    HOST = os.getenv("POSTGRES_HOST")
    DATABASE = os.getenv("POSTGRES_DATABASE")
    USER = os.getenv("POSTGRES_USER")
    PASSWORD = os.getenv("POSTGRES_PASSWORD")


class TestConfig:
    # Database configuration
    HOST = os.getenv("POSTGRES_HOST")


config = {"dev": DevConfig, "test": TestConfig, "prod": ProdConfig}
