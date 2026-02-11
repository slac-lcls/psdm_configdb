#!/bin/bash

export PYTHONPATH="modules/flask_authnz:${PYTHONPATH}"
export ACCESS_LOG_FORMAT='%(h)s %(l)s %({REMOTE-USER}i)s %(t)s "%(r)s" "%(q)s" %(s)s %(b)s %(D)s'
export LOG_LEVEL=${LOG_LEVEL:-"INFO"}

[ -z "$SERVER_IP_PORT" ] && export SERVER_IP_PORT="0.0.0.0:5000"

exec gunicorn start:app -b ${SERVER_IP_PORT} --reload \
       --log-level=${LOG_LEVEL} --capture-output --enable-stdio-inheritance \
       --worker-class gthread --workers 8 --worker-connections 2048 --max-requests 100000 --timeout 600 \
       --access-logfile - --access-logformat "${ACCESS_LOG_FORMAT}"
