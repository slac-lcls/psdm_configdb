import os
import json
import logging
from functools import wraps
from flask import abort

from pymongo import MongoClient

from flask_authnz import FlaskAuthnz, MongoDBRoles, UserGroups


logger = logging.getLogger(__name__)

__author__ = 'mshankar@slac.stanford.edu'

# Application context.
app = None

MONGODB_URL=os.environ.get("MONGODB_URL", None)
configdbclient = MongoClient(host=MONGODB_URL, tz_aware=True)
ROLEDB_URL=os.environ.get("ROLEDB_URL", None)
roledbclient = configdbclient
if ROLEDB_URL:
    logger.info("Using a different database for the roles")
    roledbclient = MongoClient(host=ROLEDB_URL, tz_aware=True)

usergroups = UserGroups()
roleslookup = MongoDBRoles(roledbclient, usergroups)

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
                if self.get_current_user_id() == hutch_name.lower() + "opr":
                    logger.debug("Letting the hutch operator for hutch %s thru %s", hutch_name, self.get_current_user_id())
                    return f(*args, **kwargs)
                if not self.check_privilege_for_experiment(priv_name, "", hutch_name):
                    abort(403)
                return f(*args, **kwargs)
            return wrapped
        return wrapper

security = ConfigDBAuthnz(roleslookup, "LogBook")
