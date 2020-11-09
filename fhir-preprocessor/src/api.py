#!/usr/bin/env python3
# Use monkey patch for reloader to not break with background thread (see https://github.com/miguelgrinberg/Flask-SocketIO/issues/567#issuecomment-337120425) 
import eventlet
eventlet.monkey_patch()
import json
from flask import Flask
from flask_restful_swagger_2 import Api
from flask_cors import CORS
from resources.filtertaskResource import FilterJobs, FilterJob
from resources.aggregationtaskResource import AggregationTasks, AggregationTask
import configuration
import os
from rdb.rdb import create_base_db

app = Flask(__name__)
CORS(app)  # this will allow cross-origin requests
api = Api(app, add_api_spec_resource=True, api_version='0.0', api_spec_url='/api/swagger')  # Wrap the Api and add /api/swagger endpoint

api.add_resource(FilterJobs, '/filterTasks', endpoint='filterTasks')
api.add_resource(FilterJob, '/filterTasks/<task_id>', endpoint='filterTask')
api.add_resource(AggregationTasks, '/aggregationTasks', endpoint='aggregationTasks')
api.add_resource(AggregationTask, '/aggregationTasks/<task_id>', endpoint='aggregationTask')


create_base_db()

if __name__ == '__main__':
    # set false in production mode
    app.run(debug=True, host=configuration.WSHOST, port=configuration.WSPORT)
