'''
The web service endpoints for the config db.
'''
import os
import json
import logging
import uuid
from datetime import datetime
from functools import wraps

import requests
from flask import Blueprint, jsonify, request, url_for, Response, send_file, abort

import context


__author__ = 'mshankar@slac.stanford.edu'

ws_service_blueprint = Blueprint('ws_service_api', __name__)

logger = logging.getLogger(__name__)

def logAndAbort(error_msg):
    logger.error(error_msg)
    return Response(error_msg, status=500)

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, float) and not math.isfinite(o):
            return str(o)
        elif isinstance(o, datetime):
            # Use var d = new Date(str) in JS to deserialize
            # d.toJSON() in JS to convert to a string readable by datetime.strptime(str, '%Y-%m-%dT%H:%M:%S.%fZ')
            return o.isoformat()
        return json.JSONEncoder.default(self, o)


@ws_service_blueprint.route("/<configroot>/get_hutches/", methods=["GET"])
def svc_get_hutches(configroot):
    """
    Get a list of hutches available in the config db
    """
    cdb = context.configdbclient.get_database(configroot)
    return JSONEncoder().encode([v['hutch'] for v in cdb.counters.find()])
