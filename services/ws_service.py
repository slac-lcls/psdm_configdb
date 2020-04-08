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
from pymongo import ASCENDING, DESCENDING, ReturnDocument
from typed_json.typed_json import * 

import context
import numpy


__author__ = 'mshankar@slac.stanford.edu'

ws_service_blueprint = Blueprint('ws_service_api', __name__)

logger = logging.getLogger(__name__)

_version = { 'major': 1, 'minor': 0, 'micro': 0 }

# generic response
def response(status_code, success, msg, value):
    rv = { 'status_code': status_code,
           'success':     success,
           'msg':         msg,
           'value':       value }
    return JSONEncoder().encode(rv)

# OK response
def ok_response(*, status_code=200, success=True, msg='OK', value=[]):
    return response(status_code, success, msg, value)

# ERROR response
def error_response(*, status_code=500, success=False, msg='ERROR', value=[]):
    return response(status_code, success, msg, value)

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, float) and not math.isfinite(o):
            return str(o)
        elif isinstance(o, datetime):
            return o.isoformat()
        elif isinstance(o, numpy.ndarray):
            return o.tolist()
        return json.JSONEncoder.default(self, o)


@ws_service_blueprint.route("/<configroot>/get_version/", methods=["GET"])
def svc_get_version(configroot):
    """
    Get version as dictionary
    """
    return ok_response(value = _version)

@ws_service_blueprint.route("/<configroot>/get_hutches/", methods=["GET"])
def svc_get_hutches(configroot):
    """
    Get a list of hutches available in the config db
    """
    cdb = context.configdbclient.get_database(configroot)
    xx = [v['hutch'] for v in cdb.counters.find()]
    return ok_response(value = xx)

@ws_service_blueprint.route("/<configroot>/get_device_configs/", methods=["GET"])
def svc_get_device_configs(configroot):
    """
    Return a list of all device configurations.
    """
    cdb = context.configdbclient.get_database(configroot)
    cfg_coll = cdb.device_configurations
    xx = [v['collection'] for v in cfg_coll.find()]
    return ok_response(value = xx)

@ws_service_blueprint.route("/<configroot>/get_aliases/<hutch>/", methods=["GET"])
def svc_get_aliases(configroot, hutch):
    """
    Return a list of all aliases in the hutch.
    """
    cdb = context.configdbclient.get_database(configroot)
    hc = cdb[hutch]
    xx = [v['_id'] for v in hc.aggregate([{"$group": 
                                              {"_id" : "$alias"}}])]
    return ok_response(value = xx)

@ws_service_blueprint.route("/<configroot>/get_devices/<hutch>/<alias>/", methods=["GET"])
def svc_get_devices(configroot, hutch, alias):
    """
    Return a list of devices in the specified hutch.
    """
    logger.debug("svc_get_devices: hutch=%s, alias=%s" % (hutch, alias))
    try:
        cdb = context.configdbclient.get_database(configroot)
        hc = cdb[hutch]

        # get key from alias
        d = hc.find({'alias' : alias}, session=None).sort('key', DESCENDING).limit(1)[0]
        key = d['key']

        c = hc.find_one({"key": key})
        xx = [l['device'] for l in c["devices"]]
    except Exception as ex:
        return error_response(msg = "get_devices: %s" % ex, value = [])

    return ok_response(value = xx)

@ws_service_blueprint.route("/<configroot>/get_configuration/<hutch>/<alias>/<device>/", methods=["GET"])
def svc_get_configuration(configroot, hutch, alias, device):
    """
    Get the configuration for the specified device in the specified hutch
    """
    logger.debug("svc_get_configuration: hutch=%s, alias=%s, device=%s" % (hutch, alias, device))

    cdb = context.configdbclient.get_database(configroot)
    hc = cdb[hutch]

    # get key from alias
    try:
        d = hc.find({'alias' : alias}, session=None).sort('key', DESCENDING).limit(1)[0]
    except IndexError:
        return error_response(msg = "get_configuration: No alias %s!" % alias)

    key = d['key']

    c = hc.find_one({"key": key})
    cfg = None
    for l in c["devices"]:
        if l['device'] == device:
            cfg = l['configs']
            break
    if cfg is None:
        return error_response(msg = "get_configuration: No device %s!" % device)

    cname = cfg[0]['collection']
    r = cdb[cname].find_one({"_id" : cfg[0]['_id']})

    return ok_response(value = r['config'])

@ws_service_blueprint.route("/<configroot>/print_device_configs/<name>/", methods=["GET"])
def svc_print_device_configs(configroot, name):
    """
    Print all of the device configurations, or all of the configurations
    for a specified device (to a string).
    For all device configurations, specify name='device_configurations'.
    """
    logger.debug("svc_print_device_configs: name=%s" % name)

    cdb = context.configdbclient.get_database(configroot)

    outstring = ""
    for v in cdb[name].find():
        outstring += "%s\n" % v
    return ok_response(value = outstring)

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
    return ok_response(value = outstring)

# Return the highest key for the specified alias, or highest + 1 for all
# aliases in the hutch if not specified.
def get_key(cdb, hutch, alias=None):
    session = None
    logger.debug("get_key: hutch=%s alias=%s" % (hutch, alias))
    try:
        if isinstance(alias, str) or (sys.version_info.major == 2 and
                                      isinstance(alias, unicode)):
            d = cdb[hutch].find({'alias' : alias}, session=session).sort('key', DESCENDING).limit(1)[0]
            return d['key']
        else:
            d = cdb.counters.find_one_and_update({'hutch': hutch},
                                                      {'$inc': {'seq': 1}},
                                                      session=session,
                                                      return_document=ReturnDocument.AFTER)
            return d['seq']
    except:
        if alias is None:
            raise NameError('Failed to get key for hutch:'+hutch)
        else:
            raise NameError('Failed to get key for alias/hutch:'+alias+'/'+hutch)

@ws_service_blueprint.route("/<configroot>/get_key/<hutch>/", methods=["GET"])
def svc_get_key(configroot, hutch):
    """
    Return the highest key for the specified alias, or highest + 1 for all
    aliases in the hutch if not specified.
    """
    alias = request.args.get("alias", None)

    cdb = context.configdbclient.get_database(configroot)

    try:
        kk = get_key(cdb, hutch, alias)
    except Exception as ex:
        return error_response(msg = "get_key: %s" % ex)

    return ok_response(value = kk)

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

    return ok_response()


# Create a new device_configuration if it doesn't already exist!
@ws_service_blueprint.route("/<configroot>/add_device_config/<cfg>/", methods=["GET"])
def svc_add_device_config(configroot, cfg):
    session = None
    cdb = context.configdbclient.get_database(configroot)
    # Validate name?
    if cdb[cfg].count_documents({}) != 0:
        return error_response(msg = "Device config '%s' already exists" % cfg)

    try:
        cdb.create_collection(cfg)
    except:
        pass
    cdb[cfg].insert_one({'config': {}}, session=session)
    cfg_coll = cdb.device_configurations
    cfg_coll.insert_one({'collection': cfg}, session=session)
    return ok_response()


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


@ws_service_blueprint.route("/<configroot>/modify_device/<hutch>/<alias>/", methods=["GET"])
def svc_modify_device(configroot, hutch, alias):
    """
    Modify the current configuration for a specific device, adding it if
    necessary.  device is the device name and POST value is a json dictionary for the
    configuration.  Return the new configuration key if successful and
    raise an error if we fail.
    """

    # get POST data
    value = request.get_json(silent=False)

    if value is None:
        return error_response(msg = "No POST data")
    elif not "detType:RO" in value.keys():
        return error_response(msg = "No detType set")
    elif not "detName:RO" in value.keys():
        return error_response(msg = "No detName set")

    device = value['detName:RO']
    logger.debug("svc_modify_device: hutch=%s, alias=%s, device=%s" % (hutch, alias, device))

    cdb = context.configdbclient.get_database(configroot)
    hc = cdb[hutch]

    try:
        c = get_current(configroot, alias, hutch)
    except Exception as ex:
        return error_response(msg = "%s" % ex)

    if c is None:
        return error_response(msg = "%s is not a configuration name!" % alias)

    session = None
    collection = value["detType:RO"]
    cfg = {'_id': save_device_config(cdb, collection, value),
           'collection': collection}
    del c['_id']
    for l in c['devices']:
        if l['device'] == device:
            if l['configs'] == [cfg]:
                return error_response(msg = "modify_device: No config values changed.")
            c['devices'].remove(l)
            break
    try:
        kn = get_key(cdb, hutch)
    except Exception as ex:
        return error_response(msg = "%s" % ex)
    c['key'] = kn
    c['devices'].append({'device': device, 'configs': [cfg]})
    c['devices'].sort(key=lambda x: x['device'])
    c['date'] = datetime.utcnow()
    hc.insert_one(c, session=session)

    return ok_response(value = kn)


@ws_service_blueprint.route("/<configroot>/create_collections/<hutch>/", methods=["GET"])
def svc_create_collections(configroot, hutch):
    """
    Create hutch.
    """
    logger.debug("svc_create_collections: hutch=%s" % hutch)

    cdb = context.configdbclient.get_database(configroot)
    try:
        cdb.create_collection("device_configurations")
    except:
        pass
    try:
        cdb.create_collection("counters")
    except:
        pass
    try:
        cdb.create_collection(hutch)
    except:
        pass
    try:
        if not cdb.counters.find_one({'hutch': hutch}):
            cdb.counters.insert_one({'hutch': hutch, 'seq': -1})
    except:
        pass

    return ok_response()

@ws_service_blueprint.route("/<configroot>/get_history/<hutch>/<alias>/<device>/", methods=["GET"])
def svc_get_history(configroot, hutch, alias, device):
    """
    Get the history of the device configuration for the variables 
    in plist.  The variables are dot-separated names with the first
    component being the the device configuration name.
    """
    # get POST data
    plist = request.get_json(silent=False)
    if plist is None:
        return error_response(msg = "get_history: no POST data", value = [])

    logger.debug("svc_get_history: hutch=%s alias=%s device=%s plist=%s" %
                 (hutch, alias, device, plist))

    cdb = context.configdbclient.get_database(configroot)
    hc = cdb[hutch]
    pipeline = [
        {"$unwind": "$devices"},
        {"$match": {'alias': alias, 'devices.device': device}},
        {"$sort":  {'key': ASCENDING}}
    ]
    l = []
    for c in list(hc.aggregate(pipeline)):
        d = {'date': c['date'], 'key': c['key']}
        cfg = c['devices']['configs'][0]
        r = cdb[cfg['collection']].find_one({"_id" : cfg["_id"]})
        cl = cdict(r['config'])
        for p in plist:
            d[p] = cl.get(p)
        l.append(d)

    return ok_response(value = l)
