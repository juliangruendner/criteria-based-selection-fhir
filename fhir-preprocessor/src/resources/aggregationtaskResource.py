from flask_restful_swagger_2 import swagger, Resource
from flask import Response
from lib import psqlDbConnection
import logging
from lib.brainApiAccess import BrainApiAccess
import csv
import io

logger = logging.getLogger(__name__)


class AggregationTasks(Resource):

    def __init__(self):
        super(AggregationTasks, self).__init__()

    @BrainApiAccess()
    def post(self):

        return "not implemented", 501


class AggregationTask(Resource):

    @BrainApiAccess()
    def __init__(self):
        super(AggregationTask, self).__init__()

    @swagger.doc({
        "description": "Retrieve a feature set for specific filter task",
        "tags": ["crawler"],
        "parameters": [
            {
                "name": "task_id",
                "in": "path",
                "type": "string",
                "description": "The ID of the filter task of the feature set to be retrieved.",
                "required": True
            }
        ],
        "responses": {
            "200": {
                "description": "Retrieved feature set as csv"
            }
        }
    })
    def get(self, task_id):

        sql = "SELECT * FROM task_" + task_id + "_featureset"
        connection = psqlDbConnection.get_db_connection()
        pgCur = connection.cursor()

        try:
            pgCur.execute(sql)
        except Exception as e:
            connection.close()
            return str(e.args[0])

        colnames = [desc[0] for desc in pgCur.description]
        item = pgCur.fetchone()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(colnames)

        while item is not None:
            item = list(item)
            writer.writerow(item)
            item = pgCur.fetchone()

        resp = Response(output.getvalue(), mimetype='text/csv')
        return resp

    def delete(self, task_id):

        return "not implemented", 501
