'''
The web service endpoints for the config db.
'''
import os
import json
import logging
import sys
import uuid
from datetime import datetime
from functools import wraps

import requests
from flask import Blueprint, jsonify, request, url_for, Response, send_file, abort
from pymongo import DESCENDING, ReturnDocument

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

@ws_service_blueprint.route("/<configroot>/get_aliases/<hutch>/", methods=["GET"])
def svc_get_aliases(configroot, hutch):
    """
    Return a list of all aliases in the hutch.
    """
    cdb = context.configdbclient.get_database(configroot)
    hc = cdb[hutch]
    xx = [v['_id'] for v in hc.aggregate([{"$group": 
                                              {"_id" : "$alias"}}])]
    return JSONEncoder().encode(xx)

@ws_service_blueprint.route("/<configroot>/get_devices/<hutch>/<alias>/", methods=["GET"])
def svc_get_devices(configroot, hutch, alias):
    """
    Return a list of devices in the specified hutch.
    """
    logger.debug("svc_get_devices: hutch=%s, alias=%s" % (hutch, alias))

    cdb = context.configdbclient.get_database(configroot)
    hc = cdb[hutch]

    # get key from alias
    d = hc.find({'alias' : alias}, session=None).sort('key', DESCENDING).limit(1)[0]
    key = d['key']

    c = hc.find_one({"key": key})
    xx = [l['device'] for l in c["devices"]]
    return JSONEncoder().encode(xx)

@ws_service_blueprint.route("/<configroot>/get_configuration/<hutch>/<alias>/<device>/", methods=["GET"])
def svc_get_configuration(configroot, hutch, alias, device):
    """
    Get the configuration for the specified device in the specified hutch
    """
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

    if "detType:RO" in r['config'].keys():
        logger.debug("svc_get_configuration: detType:RO=%s" % r['config']['detType:RO'])

    return JSONEncoder().encode(r['config'])

@ws_service_blueprint.route("/<configroot>/print_configs/<hutch>/", methods=["GET"])
def svc_print_configs(configroot, hutch):
    """
    Print all of the configurations for the hutch (to a string).
    """
    logger.debug("svc_print_configs: hutch=%s" % hutch)

    cdb = context.configdbclient.get_database(configroot)
    hc = cdb[hutch]

    outstring = ""
    for v in hc.find():
        outstring += "%s\n" % v
    return JSONEncoder().encode(outstring)

# Return highest + 1 key for all aliases in the hutch.
def get_key(cdb, hutch):
    try:
        d = cdb.counters.find_one_and_update({'hutch': hutch},
                                                  {'$inc': {'seq': 1}},
                                                  session=None,
                                                  return_document=ReturnDocument.AFTER)
        return d['seq']
    except:
        raise NameError('Failed to get key for hutch: '+hutch)

# Return the current entry (with the highest key) for the specified alias.
def get_current(configroot, alias, hutch):
    cdb = context.configdbclient.get_database(configroot)
    hc = cdb[hutch]
    try:
        return hc.find({"alias": alias}, session=None).sort('key', DESCENDING).limit(1)[0]
    except:
        raise NameError('Failed to get current key for alias/hutch:'+alias+' '+hutch)


@ws_service_blueprint.route("/<configroot>/add_alias/<hutch>/<alias>/", methods=["GET"])
def svc_add_alias(configroot, hutch, alias):
    """
    Create a new alias in the hutch, if it doesn't already exist.
    """
    logger.debug("svc_add_alias: hutch=%s, alias=%s" % (hutch, alias))

    cdb = context.configdbclient.get_database(configroot)
    hc = cdb[hutch]

    session = None
    if hc.find_one({'alias': alias}, session=session) is None:
        logger.debug("svc_add_alias: alias not found")

        d = cdb.counters.find_one_and_update({'hutch': hutch},
                                                  {'$inc': {'seq': 1}},
                                                  session=session,
                                                  return_document=ReturnDocument.AFTER)
        kn = d['seq']
        hc.insert_one({
            "date": datetime.utcnow(),
            "alias": alias, "key": kn,
            "devices": []}, session=session)
    else:
        logger.debug("svc_add_alias: alias already exists")

    return JSONEncoder().encode("OK")


# Create a new device_configuration if it doesn't already exist!
@ws_service_blueprint.route("/<configroot>/add_device_config/<cfg>/", methods=["GET"])
def svc_add_device_config(configroot, cfg):
    session = None
    cdb = context.configdbclient.get_database(configroot)
    # Validate name?
    if cdb[cfg].count_documents({}) != 0:
        return JSONEncoder().encode("Device config '%s' already exists" % cfg)

    try:
        cdb.create_collection(cfg)
    except:
        pass
    cdb[cfg].insert_one({'config': {}}, session=session)
    cfg_coll = cdb.device_configurations
    cfg_coll.insert_one({'collection': cfg}, session=session)
    return JSONEncoder().encode("OK")


# Save a device configuration and return an object ID.  Try to find it if 
# it already exists! Value should be a typed json dictionary.
def save_device_config(cdb, cfg, value):
    session = None
    if cdb[cfg].count_documents({}, session=session) == 0:
        raise NameError("save_device_config: No documents found for %s." % cfg)
    try:
        d = cdb[cfg].find_one({'config': value}, session=session)
        return d['_id']
    except:
        pass

    r = cdb[cfg].insert_one({'config': value}, session=session)
    return r.inserted_id


@ws_service_blueprint.route("/<configroot>/modify_device/<hutch>/<alias>/<device>/", methods=["GET"])
def svc_modify_device(configroot, hutch, alias, device):
    """
    Modify the current configuration for a specific device, adding it if
    necessary.  device is the device name and POST value is a json dictionary for the
    configuration.  Return the new configuration key if successful and
    raise an error if we fail.
    """
    logger.debug("svc_modify_device: hutch=%s, alias=%s, device=%s" % (hutch, alias, device))

    # get POST data
    value = request.get_json(silent=False)

    if value is None:
        return JSONEncoder().encode("ERROR no POST data")
    elif not "detType:RO" in value.keys():
        return JSONEncoder().encode("ERROR no detType set")

    # set device name
    value['detName:RO'] = device
    logger.debug("svc_modify_device: value=%s" % value)

    cdb = context.configdbclient.get_database(configroot)
    hc = cdb[hutch]

    c = get_current(configroot, alias, hutch)
    if c is None:
        return JSONEncoder().encode("ERROR %s is not a configuration name!" % alias)

    session = None
    collection = value["detType:RO"]
    cfg = {'_id': save_device_config(cdb, collection, value),
           'collection': collection}
    del c['_id']
    for l in c['devices']:
        if l['device'] == device:
            if l['configs'] == [cfg]:
                raise ValueError("modify_device error: No config values changed.")
            c['devices'].remove(l)
            break
    kn = get_key(cdb, hutch)
    c['key'] = kn
    c['devices'].append({'device': device, 'configs': [cfg]})
    c['devices'].sort(key=lambda x: x['device'])
    c['date'] = datetime.utcnow()
    hc.insert_one(c, session=session)

    return JSONEncoder().encode(kn)
