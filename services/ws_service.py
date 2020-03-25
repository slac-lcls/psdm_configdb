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
from pymongo import DESCENDING

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

@ws_service_blueprint.route("/<configroot>/get_aliases/", methods=["GET"])
def svc_get_aliases(configroot):
    """
    Return a list of all aliases in the hutch.
    """
    hutch = request.args.get("hutch", None)
    cdb = context.configdbclient.get_database(configroot)
    if hutch is None:
        # FiXME revisit default
        hc = cdb['tst']
    else:
        hc = cdb[hutch]
    xx = [v['_id'] for v in hc.aggregate([{"$group": 
                                              {"_id" : "$alias"}}])]
    return JSONEncoder().encode(xx)

@ws_service_blueprint.route("/<configroot>/get_devices/<alias>/", methods=["GET"])
def svc_get_devices(configroot, alias):
    """
    Return a list of devices in the specified hutch.
    """
    hutch = request.args.get("hutch", "tst")    # FiXME revisit default
    logger.debug("svc_get_devices: hutch=%s, alias=%s" % (hutch, alias))

    cdb = context.configdbclient.get_database(configroot)
    hc = cdb[hutch]

    # get key from alias
    d = hc.find({'alias' : alias}, session=None).sort('key', DESCENDING).limit(1)[0]
    key = d['key']

    c = hc.find_one({"key": key})
    xx = [l['device'] for l in c["devices"]]
    return JSONEncoder().encode(xx)

@ws_service_blueprint.route("/<configroot>/get_configuration/<alias>/<device>/", methods=["GET"])
def svc_get_configuration(configroot, alias, device):
    """
    Get the configuration for the specified device in the specified hutch
    """
    hutch = request.args.get("hutch", "tst")    # FiXME revisit default
    logger.debug("svc_get_configuration: hutch=%s, alias=%s, device=%s" % (hutch, alias, device))

    cdb = context.configdbclient.get_database(configroot)
    hc = cdb[hutch]

    # get key from alias
    d = hc.find({'alias' : alias}, session=None).sort('key', DESCENDING).limit(1)[0]
    key = d['key']

    c = hc.find_one({"key": key})
    cfg = None
    for l in c["devices"]:
        if l['device'] == device:
            cfg = l['configs']
            break
    if cfg is None:
        raise ValueError("get_configuration: No device %s!" % device)

    cname = cfg[0]['collection']
    r = cdb[cname].find_one({"_id" : cfg[0]['_id']})
    return JSONEncoder().encode(r['config'])
