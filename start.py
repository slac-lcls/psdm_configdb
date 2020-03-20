from flask import Flask, current_app
import logging
import os
import sys
import json
import requests
import uuid
from datetime import datetime
import pytz


from context import app
from services.ws_service import ws_service_blueprint

__author__ = 'mshankar@slac.stanford.edu'


# Initialize application.
app = Flask("configdb_server")
# Set the expiration for static files
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 300;
app.secret_key = "All Flask apps seem to need a secret key"
app.debug = bool(os.environ.get('DEBUG', "False"))

if app.debug:
    print("Sending all debug messages to the console")
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)

logger = logging.getLogger(__name__)


# Register routes.
app.register_blueprint(ws_service_blueprint, url_prefix='/ws')

if __name__ == '__main__':
    print("Please use gunicorn for development as well.")
    sys.exit(-1)
