import importlib
import configuration
from fhirclient import client
from lib import psqlDbConnection
from bson.objectid import ObjectId
from datetime import datetime
import logging
from resources import aggregationtaskResource
from fhirclient.models import bundle
import json
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

settings = {
    'app_id': 'ketos_data',
    'api_base': configuration.HAPIFHIR_URL,
}

server = client.FHIRClient(settings=settings)


class FeatureTask():

    def __init__(self, task_id, task_status, feature_set, filter_task_id):
        self.task_id = str(ObjectId())
        self.task_status = task_status
        self.feature_set = feature_set
        self.filter_task_id = filter_task_id
        self.feature_task_id = None
        self.status = "new"
        self.queued_time = None
        self.start_time = None
        self.url = "http://"+configuration.HOSTEXTERN+":"+str(configuration.WSPORT)+ "/aggregationTasks/" + self.task_id

    def queue_task(self):
        self.status = "queued"
        self.queued_time = str(datetime.now())
        filterTask = vars(self)
        filterTask['_id'] = filterTask['task_id']
        return filterTask

    def execute_task(self):

        # get patients search
        collection_base = "task_" + self.filter_task_id + "_base"

        # get patient ids from observation
        sql = "select distinct data->'subject'->>'reference' from " + collection_base + ";"

        pgCur = psqlDbConnection.get_db_connection().cursor()
        pgCur.execute(sql)

        pat_ids = []
        item = pgCur.fetchone()

        while item is not None:
            pat_ids.append(item[0].split('/')[1])
            item = pgCur.fetchone()

        collection_patient = "task_" + self.filter_task_id + "_pat"

        pat_final = []
        chunk_size = 1000
        for i in range(0, len(pat_ids), chunk_size):
            pat_final.append(pat_ids[i:i + chunk_size])

        for cur_pat_ids in pat_final:
            params = [{"key": "_id", "values": cur_pat_ids}]
            self.search_resource_by_params("Patient", params, collection_patient)

        self.create_feature_set()

        return

    def build_feature_from_val_path(self, feature):
        sql_feature = feature['resource'] + ".data"
        valPath = feature['resource_val_path'].split(".")
        index = 0
        while index < (len(valPath) - 1):

                if valPath[index].isdigit():
                    sql_feature = sql_feature + "->" + valPath[index]
                else:
                    sql_feature = sql_feature + "->'" + valPath[index] + "'"

                index = index + 1

        sql_feature = sql_feature + "->>'" + valPath[index] + "'"

        return sql_feature

    def process_feature(self, feature):

        sql_feature = ""

        if 'computed' in feature:

            if feature['computed']['operation'] == "diffYearsDate":

                sql_feature = ("ROUND(ABS( (DATE_PART ('day'," +
                               "TO_TIMESTAMP(" + self.build_feature_from_val_path(feature['computed']['field_1']) + ",'YYYY-MM-DD HH24:MI:SS') - " +
                               "TO_TIMESTAMP(" + self.build_feature_from_val_path(feature['computed']['field_2']) + ",'YYYY-MM-DD HH24:MI:SS')" +
                               "))) / 365)"
                               )

                sql_feature = sql_feature + " as " + feature['name'] + " , "
        else:
            sql_feature = sql_feature + self.build_feature_from_val_path(feature) + " as " + feature['name'] + " , "

        return sql_feature

    def create_feature_set(self):

        feature_set = self.feature_set
        task_id = self.filter_task_id

        sqlSelectBegin = "Select "
        sqlFrom = ("from task_" + task_id + "_base as Observation" +
                   " join task_" + task_id + "_pat as Patient" +
                   " on Observation.data ->'subject' ->> 'reference' = concat('Patient/',Patient.data ->> 'id')"
                   )

        sql_features = ""

        for feature in feature_set:
            sql_features = sql_features + self.process_feature(feature)

        sql_features = sql_features[0:-2]

        sql_feature_task = sqlSelectBegin + sql_features + sqlFrom

        collection = "task_" + task_id + "_featureset"
        connection = psqlDbConnection.get_db_connection()
        pgCur = connection.cursor()
        pgCur.execute("CREATE TABLE IF NOT EXISTS " + collection + " as " + sql_feature_task)
        connection.commit()

    def id_getter(self, x):
        return x.split("/")[1]

    def search_and_filter(self):

        # get distinct pat ids from base search
        collection = "task_" + self.task_id + "_base"
        connection = psqlDbConnection.get_db_connection()
        pgCur = connection.cursor()
        pgCur.execute("select distinct data->'subject'->>'reference' from " + collection + " ;")

        pat_ids = []
        item = pgCur.fetchone()

        while item is not None:
            pat_ids.append(item[0].split('/')[1])
            item = pgCur.fetchone()

        # TODO: change to "patient.reference"   ALSO check if whole url or id

        # get first exclude for base search based on pat_id
        collection = "task_" + self.task_id + "_excl"

        # TODO: change to filtering all
        # params = [{"key":"subject", "values":pat_ids},{"key":"code", "values":self.exclude[0]['codes']} ]
        # TODO: filter search for relevant encounters (check performance gain) - might need to send via body not request params
        params = [{"key": "code", "values": self.exclude[0]['codes']}]
        self.search_resource_by_params("Condition", params, collection)

        self.filter(self.exclude[0])

        # TODO refactor to own function
        # filter based on codes

    def process_search_results(self, search_result, collection):

        # TODO: Add collection here
        # TODO: Check if FHIR can resolve the dependencies

        result_list = []

        next_page = True

        while next_page:
            for entry_elem in search_result.entry:
                ret_element = entry_elem.resource
                element = ret_element.as_json()
                result_list.append(element)

            if len(search_result.link) < 2 or search_result.link[1].relation != "next":
                next_page = False
                break

            res = server.server.request_json(search_result.link[1].url)
            search_result = bundle.Bundle(res)
        connection = psqlDbConnection.get_db_connection()
        pgCur = connection.cursor()
        pgCur.execute("CREATE TABLE " + collection + " (ID serial NOT NULL PRIMARY KEY, data jsonb NOT NULL);")

        entry_list = []
        for entry in result_list:
            entry_list.append((json.dumps(entry),))

        insertSql = "INSERT INTO " + collection + "(data) VALUES %s"
        execute_values(pgCur, insertSql, entry_list, "(%s::jsonb)")

        connection.commit()

    def search_resource_by_params(self, resource_name, params, collection):
        # Dynamically load module for resource
        # key_path = get_resource_config(resource_name, resource_configs).get('key_path')
        # key = get_resource_config(resource_name, resource_configs).get('key')

        try:
            resource = getattr(importlib.import_module("fhirclient.models." + resource_name.lower()), resource_name)

        except Exception:
            logger.error("Resource " + resource_name + " does not exist", exc_info=1)
            raise

        try:

            serverSearchParams = {}
            for param in params:
                key = param['key']
                values = param['values']
                values = ','.join(map(str, values))  # convert value list to comma separated for search
                serverSearchParams[key] = values

            search = resource.where(serverSearchParams)
            search_result = search.perform(server.server)

        except Exception:
            logger.error("Search failed", exc_info=1)
            raise

        if(search_result.entry is None or len(search_result.entry) == 0):
            logger.info("No values found for search on resource " + resource_name)
            return

        self.process_search_results(search_result, collection)
