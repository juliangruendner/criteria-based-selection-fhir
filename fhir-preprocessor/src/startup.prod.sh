#!/bin/bash

python3 TaskWorkerRun.py &
uwsgi --ini /opt/refInt/uwsgi.ini