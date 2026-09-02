'''
The web service endpoints for the config db.
'''
import os
import json
import logging
import sys
import uuid
from datetime import datetime

import requests
from bson.objectid import ObjectId
from flask import Blueprint, jsonify, request, url_for, Response, send_file, abort
from pymongo import ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import WriteError

from typed_json.typed_json import cdict

import context
import numpy


__author__ = 'mshankar@slac.stanford.edu'

ws_service_blueprint = Blueprint('ws_service_api', __name__)

logger = logging.getLogger(__name__)

_version = { 'major': 2, 'minor': 0, 'micro': 0 }

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

    if alias.isdecimal():
        key = int(alias)
    else:
        # get key from alias
        try:
            d = hc.find({'alias' : alias}, session=None).sort('key', DESCENDING).limit(1)[0]
            key = d['key']
        except IndexError:
            return error_response(msg = "get_configuration: No alias %s!" % alias)

    c = hc.find_one({"key": key})
    if c is None:
        return error_response(msg = "get_configuration: No key %s!" % key)

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
@context.security.authentication_required
@context.security.authorization_required("config_edit")
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
@context.security.authentication_required
@context.security.authorization_required("config_edit")
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
# Hutch is included for authentication.
@ws_service_blueprint.route("/<configroot>/add_device_config/<hutch>/<cfg>/", methods=["GET"])
@context.security.authentication_required
@context.security.authorization_required("config_edit")
def svc_add_device_config(configroot, hutch, cfg):
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
@context.security.authentication_required
@context.security.authorization_required("config_edit")
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
@context.security.authentication_required
@context.security.authorization_required("config_edit")
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

@ws_service_blueprint.route("/<configroot>/rename_device/<hutch>/<alias>/<device>/", methods=["GET"])
@context.security.authentication_required
@context.security.authorization_required("config_edit")
def svc_rename_device(configroot, hutch, alias, device):
    """
    Rename the specified device.
    Pass in the new name as the query parameter newname
    We clone the softlinked document and update the detName:RO
    We then make a copy of the current config document with a new key and update device name and _id to point to the cloned document in the previous step
    """
    newname = request.args.get("newname", None)
    if not newname:
        return error_response(msg = "Please specify the new name as query parameter newname")
    
    logger.debug("svc_rename_device: hutch=%s, alias=%s, device=%s newname=%s" % (hutch, alias, device, newname))

    cdb = context.configdbclient.get_database(configroot)
    hc = cdb[hutch]

    try:
        c = get_current(configroot, alias, hutch)
    except Exception as ex:
        return error_response(msg = "%s" % ex)

    if c is None:
        return error_response(msg = "%s is not a configuration name!" % alias)

    if hc.count_documents({"alias": alias, "key": c["key"], "devices.device": device}) <= 0:
        return error_response(msg = "The current configuration for hutch=%s, alias=%s already does not have a device named %s" % (hutch, alias, device))

    if hc.count_documents({"alias": alias, "key": c["key"], "devices.device": newname}) > 0:
        return error_response(msg = "The current configuration for hutch=%s, alias=%s already has a device named %s" % (hutch, alias, newname))
    
    # Follow the soft link to clone the existing document and change the detName:RO
    cfg = next(x for x in c["devices"] if x["device"] == device)
    thelink = cfg["configs"][0]
    cname = thelink['collection']
    r = cdb[cname].find_one({"_id" : thelink['_id']})
    if r is None:
        return error_response(msg = "The current device config %s in the collection %s does not point to a valid document" % (thelink['_id'], cname))
    
    r["config"]["detName:RO"] = newname
    del r["_id"]
    newdocid = cdb[cname].insert_one(r).inserted_id
    logger.info("svc_rename_device: hutch=%s, alias=%s, device=%s newname=%s Newly inserted doc with new detName:RO %s" % (hutch, alias, device, newname, newdocid))
    # Copy the current key and change the name and softlink id
    session = None
    cfg["device"] = newname
    thelink["_id"] = newdocid

    d = cdb.counters.find_one_and_update({'hutch': hutch},
                                         {'$inc': {'seq': 1}},
                                         session=session,
                                         return_document=ReturnDocument.AFTER)
    kn = d['seq']
    newcdocid = hc.insert_one({
        "date": datetime.utcnow(),
        "alias": alias, 
        "key": kn,
        "devices": c["devices"]},
        session=session,
        ).inserted_id
    logger.info("svc_rename_device: hutch=%s, alias=%s, device=%s newname=%s Newly inserted config doc %s" % (hutch, alias, device, newname, newcdocid))

    return ok_response(value = True)

@ws_service_blueprint.route("/<configroot>/remove_device/<hutch>/<alias>/<device>/", methods=["GET"])
@context.security.authentication_required
@context.security.authorization_required("config_edit")
def svc_remove_device(configroot, hutch, alias, device):
    """
    Remove the specified device from the current configuration.
    We make a copy of the current config document with a new key remove the device from the devices.
    """
    logger.debug("svc_remove_device: hutch=%s, alias=%s, device=%s" % (hutch, alias, device))

    cdb = context.configdbclient.get_database(configroot)
    hc = cdb[hutch]

    try:
        c = get_current(configroot, alias, hutch)
    except Exception as ex:
        return error_response(msg = "%s" % ex)

    if c is None:
        return error_response(msg = "%s is not a configuration name!" % alias)

    if hc.count_documents({"alias": alias, "key": c["key"], "devices.device": device}) <= 0:
        return error_response(msg = "The current configuration for hutch=%s, alias=%s already does not have a device named %s" % (hutch, alias, device))


    modifieddevices = list(filter(lambda x : x["device"] != device, c["devices"]))
    session = None
    d = cdb.counters.find_one_and_update({'hutch': hutch},
                                         {'$inc': {'seq': 1}},
                                         session=session,
                                         return_document=ReturnDocument.AFTER)
    kn = d['seq']
    newcdocid = hc.insert_one({
        "date": datetime.utcnow(),
        "alias": alias, 
        "key": kn,
        "devices": modifieddevices},
        session=session,
        ).inserted_id
    logger.info("svc_remove_device: hutch=%s, alias=%s, device=%s Newly inserted config doc %s" % (hutch, alias, device, newcdocid))

    return ok_response(value = True)


@ws_service_blueprint.route("/<configroot>/test_edit_privilege/<hutch>/test", methods=["GET"])
@context.security.authentication_required
@context.security.authorization_required("config_edit")
def svc_test_edit_privilege(configroot, hutch):
    return ok_response(value = True)


@ws_service_blueprint.route("/print_headers", methods=["GET"])
def svc_print_headers():
    print(request.headers)
    return ok_response(value = True)

# --- Endpoints below are for interfacing with DRP algorithm configuration --- #

def uint64_to_int64(val):
    """Convert unsigned 64-bit int (or list/tuple/dict) to signed 2's complement."""
    if isinstance(val, int) and not isinstance(val, bool):
        return val - (1 << 64) if val >= (1 << 63) else val
    elif isinstance(val, list):
        return [uint64_to_int64(v) for v in val]
    elif isinstance(val, tuple):
        return tuple(uint64_to_int64(v) for v in val)
    elif isinstance(val, dict):
        return {k: uint64_to_int64(v) for k, v in val.items()}
    return val


def int64_to_uint64(val):
    """Convert signed 2's complement int (or list/tuple/dict) back to unsigned 64-bit int."""
    if isinstance(val, int) and not isinstance(val, bool):
        return val + (1 << 64) if val < 0 else val
    elif isinstance(val, list):
        return [int64_to_uint64(v) for v in val]
    elif isinstance(val, tuple):
        return tuple(int64_to_uint64(v) for v in val)
    elif isinstance(val, dict):
        return {k: int64_to_uint64(v) for k, v in val.items()}
    return val

def is_uint64_spec(spec):
    """Check if a parameter specification defines UINT64 (or array of UINT64)."""
    if not isinstance(spec, dict):
        return False

    p_type = spec.get("type")
    p_ref = spec.get("$ref", "")

    if p_type == "UINT64" or p_ref.endswith("UINT64"):
        return True

    if p_type == "array":
        items = spec.get("items", {})
        return is_uint64_spec(items)

    return False

def schema_aware_uint64_convert(params, schema, to_signed = True):
    """Convert parameters defined is UINT64 to a signed representation.

    MongoDB supports only signed integers. Unsigned (UINT64) XTC2 parameters are
    converted to their 2's complement signed version for storage to avoid errors
    if they are large. On retrieval, the inverse process is applied to get back
    an unsigned. Only UINT64 parameters, as specified in the schema, are converted.

    Args:
        params (dict): The parameter set.

        schema (dict): The XTC2 types schema.

        to_signed (bool): If True, convert UINT64 (>= 2^63) to 2's complement INT64.
            If False, convert INT64 (< 0) back to unsigned UINT64.
    """
    if not isinstance(params, dict) or not isinstance(schema, dict):
        return params

    properties = schema.get("properties", schema)
    converted = dict(params)
    for key, val in params.items():
        if key in properties and is_uint64_spec(properties[key]):
            if to_signed:
                converted[key] = uint64_to_int64(val)
            else:
                converted[key] = int64_to_uint64(val)

    return converted


@ws_service_blueprint.route("/<configroot>/get_algorithms/", methods=["GET"])
def svc_get_algorithms(configroot):
    """
    Return a list of registered algorithms and their versions.

    The collection `alg_registry` has the registry of documents in this format:
        { "name": "MyAlgName", "versions": ["v1", "v2"] }

    Args:
        configroot: Database name
    """
    cdb = context.configdbclient.get_database(configroot)

    algs = list(cdb.alg_registry.find({}, {"_id": 0}))

    return ok_response(value=algs)

@ws_service_blueprint.route("/<configroot>/get_algorithm/<alg>/<ver>/schema/", methods=["GET"])
def svc_get_algorithm_schema(configroot, alg, ver):
    """
    Return the parameter schema for a specific version of an algorithm.

    Args:
        configroot: Database name

        alg: Name of the DRP algorithm

        ver: Version of the DRP algorithm
    """
    cdb = context.configdbclient.get_database(configroot)

    try:
        coll_name: str = f"drp_alg_{alg}_{ver}"

        schema_doc = cdb[coll_name].find_one({"_id": "_schema"})
        if not schema_doc:
            return error_response(
                msg=f"Requested algorithm ({alg}) version {ver} does not have a schema!",
                status_code=404,
            )

        return ok_response(value=schema_doc.get("json_schema", {}))
    except Exception:
        return error_response(
            msg=f"Requested algorithm ({alg}) version {ver} does not exist!",
            status_code=404,
        )

@ws_service_blueprint.route("/<configroot>/get_algorithm/<alg>/<ver>/presets/", methods=["GET"])
def svc_get_algorithm_presets(configroot, alg, ver):
    """
    Return the set of presets parameter sets for the requested algorithm version.

    Args:
        configroot: Database name

        alg: Name of the DRP algorithm

        ver: Version of the DRP algorithm
    """
    cdb = context.configdbclient.get_database(configroot)

    try:
        coll_name: str = f"drp_alg_{alg}_{ver}"

        presets = list(
            cdb[coll_name].find(
                {"$and": [{"preset_name": {"$ne": ""}}, {"_id": {"$ne": "_schema"}}]}
            )
        )
        if not presets:
            return error_response(
                msg=f"Requested algorithm ({alg}) version {ver} does not have presets!",
                status_code=404,
            )

        schema_doc = cdb[coll_name].find_one({"_id": "_schema"}) or {}
        schema = schema_doc.get("json_schema", {})
        for preset in presets:
            if "parameters" in preset:
                preset["parameters"] = schema_aware_uint64_convert(
                    preset["parameters"], schema, to_signed=False
                )
            # The ObjectId field is not JSON serializable
            preset["_id"] = str(preset["_id"])

        return ok_response(value=presets)
    except Exception:
        return error_response(
            msg=f"Requested algorithm ({alg}) version {ver} does not exist!",
            status_code=404,
        )

@ws_service_blueprint.route("/<configroot>/get_algorithm/<alg>/<ver>/params/", methods=["GET"])
def svc_get_algorithm_params(configroot, alg, ver):
    """
    Return a specific parameter set for the requested algorithm version.

    This endpoint can return 2 payload types depending on request configuration:
        1. No ID or presets requested -> Latest parameters uploaded.
        2. A specific parameter set by ID - This is included in `params_id` in
           the payload.

    Args:
        configroot: Database name

        alg: Name of the DRP algorithm

        ver: Version of the DRP algorithm
    """
    # This endpoint allows passing argument to retrieve ID
    # OR, no argument for latest doc. Hence choice of kwargs here
    req_args = request.get_json(silent=True, force=True) or {}

    params_id = req_args.get("params_id")

    cdb = context.configdbclient.get_database(configroot)

    try:
        coll_name: str = f"drp_alg_{alg}_{ver}"
        if params_id is not None:
            params = cdb[coll_name].find_one({"_id": ObjectId(params_id)})
        else:
            params = (
                cdb[coll_name]
                .find({"_id": {"$ne": "_schema"}})
                .sort("created_at", DESCENDING)
                .limit(1)[0]
            )

        if not params:
            if params_id is not None:
                return error_response(
                    msg=f"Unable to find requested parameters! (Id: {params_id})",
                    status_code=404,
                )
            else:
                return error_response(
                    msg="Unable to find latest parameters! Maybe no sets registered?",
                    status_code=404,
                )

        schema_doc = cdb[coll_name].find_one({"_id": "_schema"}) or {}
        schema = schema_doc.get("json_schema", {})

        if "parameters" in params:
            params["parameters"] = schema_aware_uint64_convert(
                params["parameters"], schema, to_signed=False
            )

        # Sanitize the ObjectId
        params["_id"] = str(params["_id"])

        return ok_response(value=params)
    except Exception as err:
        return error_response(
            msg=f"Unable to find requested parameters! Error: {err}", status_code=404
        )

@ws_service_blueprint.route("/<configroot>/get_algorithm/<alg>/<ver>/metadata/", methods=["GET"])
def svc_get_algorithm_metadata(configroot, alg, ver):
    """
    Retrieve any metadata stored as associated to an algorithm.

    This may include GUI plugins, e.g., that are used to help configure the algorithm.

    Args:
        configroot: Database name

        alg: Name of the DRP algorithm

        ver: Version of the DRP algorithm
    """
    cdb = context.configdbclient.get_database(configroot)
    coll_name = f"drp_alg_{alg}_{ver}"
    doc = cdb[coll_name].find_one({"_id": "_schema"}, {"_id": 0})

    if not doc:
        return error_response(msg=f"Metadata for {alg} {ver} not found!", status_code=404)

    return ok_response(value=doc)

def make_schema_mongo_compatible(alg_ver_schema):
    """
    The mongoDB JSON schema requires some modifications vs a standard schema.
    This function converts an algorithm schema into the MongoDB format to be
    used at the database layer for validation. The original schema can also
    be stored unmodified.

    Args:
        alg_ver_schema (dict[str, Any]): The parameter schema in normal JSON
            schema format -- generally coming from DAQ code via request.

    Returns:
        converted (dict[str, Any]): The schema with any modifications required
            for use as a mongoDB validator.
    """
    BSON_TYPE_MAP = {
        "UINT8": "int",
        "UINT16": "int",
        "UINT32": ["int", "long"],
        "UINT64": ["int", "long"],
        "INT8": "int",
        "INT16": "int",
        "INT32": "int",
        "INT64": ["int", "long"],
        "FLOAT": "double",
        "DOUBLE": "double",
        "CHARSTR": "string",
        "string": "string",
        "number": "double",
        "integer": ["int", "long"],
        "BOOL": "bool",
        "boolEnum": "bool",
        "boolean": "bool",
        "array": "array",
        "object": "object",
    }

    def convert_spec(spec):
        if not isinstance(spec, dict):
            return spec

        converted = {}
        for key, value in spec.items():
            if key == "type":
                converted["bsonType"] = BSON_TYPE_MAP.get(value, value)
            elif key == "$ref":
                underlying_type = value.rstrip("/").rsplit("/", 1)[-1]
                converted["bsonType"] = BSON_TYPE_MAP.get(underlying_type, underlying_type)
            elif key == "items" and isinstance(value, dict):
                converted["items"] = convert_spec(value)
            else:
                converted[key] = value

        return converted

    properties = alg_ver_schema.get("properties", alg_ver_schema)
    converted_properties = {
        prop_name: convert_spec(prop_spec)
        for prop_name, prop_spec in properties.items()
    }

    ret = {
        "bsonType": "object",
        "properties": converted_properties,
    }

    req = alg_ver_schema.get("required", [])
    if req:
        ret["required"] = req

    return ret

def construct_alg_db_schema(alg_ver_schema):
    """
    Construct a schema that validates entries in an algorithm version collection.

    The version of the algorithm should come with a schema to validate its own
    parameter sets. The collection that holds these, though, is slightly broader
    and includes additional keys that the backend uses. This function therefore updates
    the algorithm schema it receives with the appropriate keys for the rest of
    the keys.

    Args:
        alg_ver_schema (dict[str, Any]): The parameter schema in normal JSON
            schema format -- generally coming from DAQ code via request.

    Returns:
        converted (dict[str, Any]): The combined schema for the parameters of the
            algorithm and the schema document itself.
    """
    schema_for_schema_doc = {
        "bsonType": "object",
        "properties": {
            "_id": {"enum": ["_schema"]},
            "schema_version": {"bsonType": "string"},
            "gui_plugin": {
                "type": "object",
                "properties": {
                    "plugin_type": { "type": "string" },
                    "module": { "type": "string" },
                    "entry_point": { "type": "string" },
                    "label": { "type": "string" },
                },
            },
            "json_schema": {"bsonType": "object"},
        },
        "required": ["_id", "schema_version", "json_schema"],
    }

    schema_for_params_doc = {
        "bsonType": "object",
        "properties": {
            "preset_name": {"bsonType": "string"},
            "created_by": {"bsonType": "string"},
            "created_at": {"bsonType": "date"},
            "soname": {"bsonType": "string"},
            "parameters": make_schema_mongo_compatible(alg_ver_schema),
        },
        "required": ["created_at", "parameters"],
    }

    # In a versioned algorithm collection we either have:
    # A schema document itself:
    #
    # { "_id": "_schema", "schema_version": "v1", "json_schema": { ... }, }
    #
    # OR, each actual parameter set (which conforms to the above)
    #
    # { "preset_name": "", "created_by": "", created_at": "", "parameters": { ... }, }

    full_schema = {
        "$jsonSchema": {
            "oneOf": [schema_for_schema_doc, schema_for_params_doc],
        },
    }

    return full_schema

@ws_service_blueprint.route("/<configroot>/new_algorithm/<alg>/<ver>/", methods=["POST"])
@context.security.authentication_required
@context.security.authorization_required("config_edit")
def svc_add_new_algorithm(configroot, alg, ver):
    """
    Add a new algorithm for the first time.

    Args:
        configroot: Database name

        alg: Name of the DRP algorithm

        ver: Version of the DRP algorithm
    """
    req_args = request.get_json(silent=False) or {}
    preset_name = req_args.get("preset_name", "Default")
    schema_version = req_args.get("schema_version", "1.0.0")
    gui_plugin = req_args.get("gui_plugin", None)
    defaults = req_args.get("defaults", {})
    schema = req_args.get("schema", {})
    opr = req_args.get("opr", "tstopr")
    soname = req_args.get("soname", "")

    set_dict = {
        "schema_version": schema_version,
        "soname": soname,
        "json_schema": schema,
    }
    if gui_plugin:
        set_dict["gui_plugin"] = gui_plugin

    if not schema:
        return error_response(msg="Schema is required for all algorithms!")

    cdb = context.configdbclient.get_database(configroot)

    coll_name: str = f"drp_alg_{alg}_{ver}"

    existing_reg = cdb.alg_registry.find_one({"name": alg, "versions": ver})
    if existing_reg or coll_name in cdb.list_collection_names():
        return error_response(
            msg=f"Algorithm '{alg}' version '{ver}' is already registered! Algorithm versions are immutable.",
            status_code=400,
        )

    try:
        cdb.create_collection(
            coll_name,
            validator=construct_alg_db_schema(alg_ver_schema=schema),
            validationAction="error",
        )
    except Exception as err:
        return error_response(msg=f"Unable to create algorithm collection: {err}")

    try:
        cdb[coll_name].update_one(
            {"_id": "_schema"},
            {"$set": set_dict},
            upsert=True,
        )
    except Exception as err:
        try:
            cdb.drop_collection(coll_name)
        except Exception:
            pass

        msg = f"Failed to register algorithm due to failure in schema insert: {err}"
        logger.error(msg)
        return error_response(msg=msg, status_code=400)

    defaults_id = None
    if defaults:
        try:
            defaults_converted = schema_aware_uint64_convert(
                defaults, schema, to_signed=True
            )
            res = cdb[coll_name].insert_one(
                {
                    "preset_name": preset_name,
                    "created_by": opr,
                    # Expected as a date object, not string.
                    "created_at": datetime.utcnow(),
                    "soname": soname,
                    "parameters": defaults_converted,
                }
            )
            defaults_id = str(res.inserted_id)
        except Exception as err:
            try:
                cdb.drop_collection(coll_name)
            except Exception:
                pass

            msg = f"Failed to register defaults: {err}. Registration rolled back!"
            logger.error(msg)
            return error_response(msg=msg, status_code=400)

    try:
        cdb.alg_registry.update_one(
            {"name": alg},
            {"$addToSet": {"versions": ver}},
            upsert=True,
        )
    except Exception as err:
        try:
            cdb.drop_collection(coll_name)
        except Exception:
            pass

        msg = f"Could not make alg_registry entry: {err}. Registration rolled back!"
        logger.error(msg)
        return error_response(msg=msg, status_code=400)

    return ok_response(value={"collection": coll_name, "params_id": defaults_id})

@ws_service_blueprint.route("/<configroot>/add_algorithm_params/<alg>/<ver>/", methods=["POST"])
@context.security.authentication_required
@context.security.authorization_required("config_edit")
def svc_add_algorithm_params(configroot, alg, ver):
    """
    Add a new parameter set for a version of an algorithm.

    Args:
        configroot: Database name

        alg: Name of the DRP algorithm

        ver: Version of the DRP algorithm
    """
    req_args = request.get_json(silent=False) or {}
    preset_name = req_args.get("preset_name", "")
    params = req_args.get("parameters", {})
    soname = req_args.get("soname")

    opr = req_args.get("opr", "tstopr")

    if not params:
        return error_response(msg="Must include parameters in document!")

    cdb = context.configdbclient.get_database(configroot)

    coll_name = f"drp_alg_{alg}_{ver}"

    schema_doc = cdb[coll_name].find_one({"_id": "_schema"}) or {}
    if not soname:
        soname = schema_doc.get("soname", "")

    schema = schema_doc.get("json_schema", {})
    params_converted = schema_aware_uint64_convert(params, schema, to_signed=True)
    doc = {
        "preset_name": preset_name,
        "created_by": opr,
        # Expected as a date object, not string.
        "created_at": datetime.utcnow(),
        "soname": soname,
        "parameters": params_converted,
    }
    try:
        res = cdb[coll_name].insert_one(doc)
        params_id = str(res.inserted_id)

        return ok_response(value={"collection": coll_name, "params_id": params_id})
    except WriteError as err:
        details = getattr(err, "details", {})
        info = details.get("errInfo", {}).get("details", {})

        logger.error(f"Parameter validation failed: {info}")
        return error_response(
            msg=f"Parameter document failed validation! Details: {info}",
            status_code=400,
        )
    except Exception as err:
        return error_response(
            msg=f"Failed to insert document for unknown reason: {err}",
            status_code=500,
        )

@ws_service_blueprint.route("/<configroot>/update_algorithm_metadata/<alg>/<ver>/", methods=["POST"])
@context.security.authentication_required
@context.security.authorization_required("config_edit")
def svc_update_algorithm_metadata(configroot, alg, ver):
    """
    Update the metadata for an algorithm without changing the underlying parameters.

    Metadata includes information such as any GUI plugins associated to the algorithm
    which should be loaded to help configure them.
    """
    req_args = request.get_json(silent=False) or {}

    cdb = context.configdbclient.get_database(configroot)
    coll_name = f"drp_alg_{alg}_{ver}"

    # Build update payload
    update_fields = {}
    if "gui_plugin" in req_args:
        update_fields["gui_plugin"] = req_args["gui_plugin"]
    if "schema_version" in req_args:
        update_fields["schema_version"] = req_args["schema_version"]

    if not update_fields:
        return error_response(msg="No metadata fields provided to update!")

    cdb[coll_name].update_one(
        {"_id": "_schema"},
        {"$set": update_fields},
        upsert=True
    )

    return ok_response(value=f"Updated metadata for {coll_name}")

@ws_service_blueprint.route("/<configroot>/remove_algorithm/<alg>/<ver>/", methods=["POST", "DELETE"])
@context.security.authentication_required
@context.security.authorization_required("config_edit")
def svc_remove_algorithm(configroot, alg, ver):
    """
    Remove a specific version of an algorithm, or pass ver='all' to delete all versions.

    Args:
        configroot: Database name

        alg: Name of the DRP algorithm

        ver: Version of the DRP algorithm. If `all` will drop every algorithm version.
    """
    cdb = context.configdbclient.get_database(configroot)

    if ver == "all":
        alg_doc = cdb.alg_registry.find_one({"name": alg})
        if alg_doc and "versions" in alg_doc:
            for v in alg_doc["versions"]:
                cdb.drop_collection(f"drp_alg_{alg}_{v}")

        cdb.alg_registry.delete_one({"name": alg})
        return ok_response(value=f"Removed algorithm '{alg}' and all versions.")

    # To drop single version, remove the collection and then pull from registry
    coll_name = f"drp_alg_{alg}_{ver}"
    cdb.drop_collection(coll_name)

    cdb.alg_registry.update_one({"name": alg}, {"$pull": {"versions": ver}})

    # If versions now empty, remove the entire algorithm from the registry
    cdb.alg_registry.delete_one({"name": alg, "versions": []})

    return ok_response(value=f"Removed version '{ver}' of algorithm '{alg}'.")
