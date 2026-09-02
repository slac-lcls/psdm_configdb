from flask import Flask, current_app
import logging
import os
import sys
import uuid
from datetime import datetime


from context import app
from services.ws_service import ws_service_blueprint

__author__ = 'mshankar@slac.stanford.edu'

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logging.getLogger("pymongo").setLevel(logging.WARNING)

# Initialize application.
app = Flask("configdb_server")
# Set the expiration for static files
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 300;
app.secret_key = "All Flask apps seem to need a secret key"
app.debug = False


# Register routes.
app.register_blueprint(ws_service_blueprint, url_prefix='/ws')

if __name__ == '__main__':
    print("Please use gunicorn for development as well.")
    sys.exit(-1)
