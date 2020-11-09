from flask_restful import reqparse
from flask_restful_swagger_2 import swagger, Resource
from flask import request
from lib import psqlDbConnection
from bson.objectid import ObjectId
from cerberus import Validator
from lib.FilterTask import FilterTask
from lib.FilterTaskPsql import FilterTaskPsql
import json
import logging
from datetime import datetime
from lib.brainApiAccess import BrainApiAccess
from lib.RefIntLogger import RefIntLogger


log = RefIntLogger()


RESOURCE_CONFIG_SCHEMA = {
    'resource': {'required': True, 'type': 'string'},                    # Name of resource, e.g. "Condition"
    'name': {'required': True, 'type': 'string'},                        # Path to look for actual value of a resource, e.g. "category/coding/code"
    'resource_val_path': {'type': 'list', 'schema': {'type': 'string'}}  # Order to sort retrieved values after, necessary for sorting for newest value
}

def resource_config_validator(value):
    v = Validator(RESOURCE_CONFIG_SCHEMA)
    if v.validate(value):
        return value
    else:
        raise ValueError(json.dumps(v.errors))


executionTypes = ["sync", "async"]
EXECUTION_TYPE_STR = "Allowed types: " + str(executionTypes)

parser = reqparse.RequestParser(bundle_errors=True)
parser.add_argument('execution_type', type=str, required=True, help=EXECUTION_TYPE_STR, location='args')


class FilterJobs(Resource):

    def __init__(self):
        super(FilterJobs, self).__init__()

    @swagger.doc({
        "description": "Start a Crawler Job for a single patient and wait until it's finished.",
        "tags": ["crawler"],
        "responses": {
            "200": {
                "description": "Retrieved a json with a URL to the generated CSV and the exit status of the Crawler.",
                "schema": {
                    "type": "object",
                    "properties": {
                        "csv_url": {
                            "type": "string"
                        },
                        "crawler_status": {
                            "type": "string"
                        }
                    },
                    "example": {
                        "csv_url": "URL",
                        "crawler_status": "One of [success, error]"
                    }
                }
            }
        }
    })
    @BrainApiAccess()
    def post(self):

        args = parser.parse_args()
        execution_type = args["execution_type"]
        filter_task_id = str(ObjectId())

        connection = psqlDbConnection.get_db_connection()

        if execution_type == 'sync':
            request_vals = request.get_json()
            filter_task = FilterTaskPsql(filter_task_id, "running", request_vals['loinc'], request_vals['include'],
                                         request_vals['exclude'], request_vals['feature_set'])

            pgCur = connection.cursor()
            insertSql = "INSERT INTO filter_tasks (data) VALUES (%s::jsonb)"

            log.debug("Executing synchronous task, id:" + filter_task_id)
            pgCur.execute(insertSql, (json.dumps(filter_task.__dict__),))
            connection.commit()
            filter_task.execute_task()

        else:
            filter_task_id = str(ObjectId())
            request_vals = request.get_json()
            filter_task = FilterTaskPsql(filter_task_id, "new", request_vals['loinc'], request_vals['include'],
                                         request_vals['exclude'], request_vals['feature_set'])

            filter_task.queued_time = str(datetime.now())

            pgCur = connection.cursor()
            insertSql = "INSERT INTO filter_tasks (data) VALUES (%s::jsonb)"
            pgCur.execute(insertSql, (json.dumps(filter_task.__dict__),))
            connection.commit()

        task = {"task_id": filter_task_id}
        return task

    def get(self):

        connection = psqlDbConnection.get_db_connection()
        pgCur = connection.cursor()
        sql = "SELECT data from filter_tasks"
        pgCur.execute(sql)
        task = pgCur.fetchall()

        if task is None:
            return "no tasks found", 400

        task = [r[0] for r in task]

        return task

    def delete(self):

        try:
            connection = psqlDbConnection.get_db_connection()
            pgCur = connection.cursor()
            sql = "SELECT data->>'task_id' from filter_tasks"
            pgCur.execute(sql)

            tasks = pgCur.fetchall()

            if tasks is None:
                return "no tasks found", 400

            tasks = [r[0] for r in tasks]

            for task_id in tasks:

                sql = "DELETE from filter_tasks where data->>'task_id' = %s"
                pgCur.execute(sql, (task_id,))
                
                tables_to_drop = ['pat', 'featureset', 'result']
                for to_drop in tables_to_drop:
                    sql = "DROP table IF EXISTS task_" + task_id + "_" + to_drop
                    pgCur.execute(sql, (task_id,))

            connection.commit()

        except Exception as e:
            connection.close()
            return str(e.args[0])

        return "tasks deleted", 200

class FilterJob(Resource):

    @BrainApiAccess()
    def __init__(self):
        super(FilterJob, self).__init__()

    @swagger.doc({
        "description": "Retrieve a single Crawler Job.",
        "tags": ["crawler"],
        "parameters": [
            {
                "name": "task_id",
                "in": "path",
                "type": "string",
                "description": "The ID of the FilterTask to be retrieved.",
                "required": True
            }
        ],
        "responses": {
            "200": {
                "description": "Retrieved Filter Task as json."
            }
        }
    })
    def get(self, task_id):

        connection = psqlDbConnection.get_db_connection()
        pgCur = connection.cursor()
        sql = "SELECT data from filter_tasks where data->>'task_id' = %s  "
        pgCur.execute(sql, (task_id,))
        task = pgCur.fetchone()

        if task is None:
            return "no task with this task id found", 400

        return task[0]

    def delete(self, task_id):

        connection = psqlDbConnection.get_db_connection()
        pgCur = connection.cursor()
        sql = "DELETE FROM filter_tasks WHERE data->>'task_id' = %s  "

        try:
            pgCur.execute(sql, (task_id,))
        
            tables_to_drop = ['pat', 'featureset', 'result']

            for to_drop in tables_to_drop:
                sql = "DROP table IF EXISTS task_" + task_id + "_" + to_drop
                pgCur.execute(sql, (task_id,))

            connection.commit()

        except Exception as e:
            connection.close()
            return str(e.args[0])

        return "task deleted", 200
