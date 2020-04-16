import os
import json
import logging

from pymongo import MongoClient

from flask_authnz import FlaskAuthnz, MongoDBRoles, UserGroups


logger = logging.getLogger(__name__)

__author__ = 'mshankar@slac.stanford.edu'

# Application context.
app = None

MONGODB_URL=os.environ.get("MONGODB_URL", None)
configdbclient = MongoClient(host=MONGODB_URL, tz_aware=True)

usergroups = UserGroups()
roleslookup = MongoDBRoles(configdbclient, usergroups)
security = FlaskAuthnz(roleslookup, "LogBook")
