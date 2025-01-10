import os
import json
import logging
import urllib
import string
from functools import wraps
from flask import abort

from pymongo import MongoClient

from flask_authnz import FlaskAuthnz, MongoDBRoles, UserGroups


logger = logging.getLogger(__name__)

__author__ = 'mshankar@slac.stanford.edu'

# Application context.
app = None

def __read_from_file__(envname, varname, theenv):
    if envname in os.environ and os.path.exists(os.environ[envname]):
        with open(os.environ[envname], "r") as f:
            theenv[varname] = urllib.parse.quote(f.read().strip())
    else:
        raise Exception(f"File for {varname} not found")

configdbenv = {}
__read_from_file__("CONFIGDB_USER_FILE", "CONFIGDB_USER", configdbenv)
__read_from_file__("CONFIGDB_PWD_FILE",  "CONFIGDB_PWD",  configdbenv)
__read_from_file__("CONFIGDB_HOSTS_FILE",  "CONFIGDB_HOSTS",  configdbenv)

CONFIGDB_URL_TMPL = string.Template(os.environ["CONFIGDB_URL_TMPL"])
CONFIGDB_URL = CONFIGDB_URL_TMPL.substitute(configdbenv)
configdbclient = MongoClient(host=CONFIGDB_URL, tz_aware=True)

roledbclient = configdbclient
ROLEDB_URL_TMPL = os.environ.get("ROLEDB_URL_TMPL", None)
if ROLEDB_URL_TMPL:
    logger.info("Using a different database for the roles")
    ROLEDB_URL_TMPL = string.Template(ROLEDB_URL_TMPL)
    roledbenv = {}
    __read_from_file__("ROLEDB_USER_FILE", "ROLEDB_USER", roledbenv)
    __read_from_file__("ROLEDB_PWD_FILE",  "ROLEDB_PWD",  roledbenv)
    __read_from_file__("ROLEDB_HOSTS_FILE",  "ROLEDB_HOSTS",  roledbenv)
    ROLEDB_URL = ROLEDB_URL_TMPL.substitute(roledbenv)
    roledbclient = MongoClient(host=ROLEDB_URL, tz_aware=True)

usergroups = UserGroups()
roleslookup = MongoDBRoles(roledbclient, usergroups)

instrument2operator_uids = { x["_id"].lower() : x.get("params", {}).get("operator_uid", x["_id"].lower()+"opr") for x in roledbclient["site"]["instruments"].find({}, {"_id": 1, "params.operator_uid": 1})}
print(instrument2operator_uids)

class ConfigDBAuthnz(FlaskAuthnz):
    """
    Change the way authorization works for the ConfigDB.
    """
    def __init__(self, roles_dal, application_name):
        super().__init__(roles_dal, application_name)

    def authorization_required(self, *params):
        '''
        Decorator for configDB  authorization.
        We first look for the hutch parameter.
        If this is the hutch operator, then we let you in.
        If you have the specified privilege for this hutch, then we let you in.
        '''
        if len(params) < 1:
            raise Exception("Application privilege not specified when specifying the authorization")
        priv_name = params[0]
        if priv_name not in self.priv2roles:
            raise Exception("Please specify an appropriate application privilege for the authorization_required decorator " + ",".join(self.priv2roles.keys()))
        def wrapper(f):
            @wraps(f)
            def wrapped(*args, **kwargs):
                hutch_name = kwargs.get('hutch', None)
                logger.info("Looking to authorize %s for app %s for privilege %s for hutch %s" % (self.get_current_user_id(), self.application_name, priv_name, hutch_name))
                if self.get_current_user_id() == instrument2operator_uids.get(hutch_name.lower(), hutch_name.lower() + "opr"):
                    logger.debug("Letting the hutch operator for hutch %s thru %s", hutch_name, self.get_current_user_id())
                    return f(*args, **kwargs)
                if not self.check_privilege_for_experiment(priv_name, "", hutch_name):
                    abort(403)
                return f(*args, **kwargs)
            return wrapped
        return wrapper

security = ConfigDBAuthnz(roleslookup, "LogBook")
