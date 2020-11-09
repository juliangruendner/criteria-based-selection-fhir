import importlib
import configuration
from fhirclient import client
from psycopg2.extras import execute_values
from lib.FeatureTask import FeatureTask
from lib import psqlDbConnection
from bson.objectid import ObjectId
from datetime import datetime
import logging
from fhirclient.models import bundle
import json
from resources import aggregationtaskResource

logger = logging.getLogger(__name__)

settings = {
    'app_id': 'ketos_data',
    'api_base': configuration.HAPIFHIR_URL,
}

server = client.FHIRClient(settings=settings)


class FilterTask():

    def __init__(self, task_id, task_status, base_search, include, exclude, feature_set):
        self.task_id = task_id
        self.base_search = base_search
        self.include = include
        self.exclude = exclude
        self.feature_set = feature_set
        self.status = task_status
        self.queued_time = None
        self.start_time = None
        self.error_message = None
        self.gen_filter_query = None

        self.url = "http://"+configuration.HOSTEXTERN+":"+str(configuration.WSPORT)+ "/aggregationTasks/" + self.task_id

    def queue_task(self):
        self.status = "queued"
        self.queued_time = str(datetime.now())
        filterTask = vars(self)
        filterTask['_id'] = filterTask['task_id']
        return filterTask

    def get_encs(self, collection_source, collection_target):
        connection = psqlDbConnection.get_db_connection()
        pgCur = connection.cursor()
        pgCur.execute("select distinct data->'encounter'->>'reference' from " + collection_source + " ;")

        enc_ids = []
        item = pgCur.fetchone()

        while item is not None:
            enc_ids.append(item[0].split('/')[1])
            item = pgCur.fetchone()

        enc_final = []
        chunk_size = 1000
        for i in range(0, len(enc_ids), chunk_size):
            enc_final.append(enc_ids[i:i + chunk_size])

        for cur_enc_ids in enc_final:
            params = [{"key": "_id", "values": cur_enc_ids}]
            self.search_resource_by_params("Encounter", params, collection_target)

    def execute_task(self):

        print("Task is now being executed enjoy")
        connection = psqlDbConnection.get_db_connection()
        pgCur = connection.cursor()
        sql = "UPDATE filter_tasks set data = %s where data->>'task_id' = %s"
        self.status = 'running'
        pgCur.execute(sql, (json.dumps(self.__dict__), self.task_id))
        connection.commit()

        # get base search - as in Observations or resources to start search and filter from
        collection_source = "task_" + self.task_id + "_base"
        params = [{"key": "code", "values": self.base_search}]
        self.search_resource_by_params("Observation", params, collection_source)

        # get distinct enc ids from base search
        collection_target = "task_" + self.task_id + "_enc"
        self.get_encs(collection_source, collection_target)

        for exclude in self.exclude:
            self.search_and_filter(exclude)

        feat_task_id = str(ObjectId())
        feature_task = FeatureTask(feat_task_id, "running", self.feature_set, self.task_id)
        feature_task.execute_task()

        pgCur = connection.cursor()
        sql = "UPDATE filter_tasks set data = %s where data->>'task_id' = %s"
        self.status = 'done'
        pgCur.execute(sql, (json.dumps(self.__dict__), self.task_id))
        connection.commit()

    def filter(self, exclude):

        task_string = "task_" + self.task_id
        base = task_string + "_base"
        excl = task_string + "_excl"
        enc = excl + "_enc"

        interval_m = exclude['time_interval']
        if interval_m > 0:
            sql_delete = "delete from " + base + " where data->>'id' in "
            sql_filter_select = (" (select base.data ->> 'id'   "
                                 " from " + excl + " as excl left join " + enc + " as enc"
                                 " on excl.data ->'encounter' ->> 'reference' = concat('Encounter/',enc.data ->> 'id')"
                                 " join " + base + " as base"
                                 " on excl.data -> 'subject'->>'reference' = base.data -> 'subject' ->> 'reference'"
                                 " WHERE"
                                 " (TO_TIMESTAMP(base.data ->> %s, 'YYYY-MM-DD HH24:MI:SS') >="
                                 " TO_TIMESTAMP(enc.data -> 'period'->> 'start', 'YYYY-MM-DD HH24:MI:SS') - interval '%s month'"
                                 " AND"
                                 " TO_TIMESTAMP(base.data ->> %s, 'YYYY-MM-DD HH24:MI:SS') <="
                                 " TO_TIMESTAMP(enc.data -> 'period'->> 'end', 'YYYY-MM-DD HH24:MI:SS') + interval '%s month' )"
                                 " OR"
                                 " (TO_TIMESTAMP(base.data ->> %s, 'YYYY-MM-DD HH24:MI:SS') >="
                                 " TO_TIMESTAMP(excl.data -> 'period'->> 'start', 'YYYY-MM-DD HH24:MI:SS') - interval '%s month'"
                                 " AND"
                                 " TO_TIMESTAMP(base.data ->> %s, 'YYYY-MM-DD HH24:MI:SS') <="
                                 " TO_TIMESTAMP(excl.data -> 'period'->> 'end', 'YYYY-MM-DD HH24:MI:SS') + interval '%s month' )"
                                 )
        else:
            sql_delete = "delete from " + base + " where data->'subject'->>'reference' in "
            sql_filter_select = (" (select distinct excl.data -> 'subject' ->> 'reference'"
                                 " from " + excl + " as excl"
                                 " where 1=1"
                                 ""
                                 )

        if 'value_restriction' in exclude:
            operator = exclude['value_restriction']['operator']
            compare_to = exclude['value_restriction']['compare_to']
            sql_filter_select = sql_filter_select + " and TO_NUMBER(excl.data->'valueQuantity'->>'value', '99.9') " + operator + " " + str(compare_to)

        dateField = exclude['dateField']

        sql = sql_delete + sql_filter_select + " );"
        connection = psqlDbConnection.get_db_connection()
        pgCur = connection.cursor()
        pgCur.execute(sql, (dateField, interval_m, dateField, interval_m, dateField, interval_m, dateField, interval_m))

        connection.commit()

    def id_getter(self, x):
        return x.split("/")[1]

    def search_and_filter(self, exclude):

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

        collection = "task_" + self.task_id + "_excl"

        # TODO: filter search for relevant encounters (check performance gain) - might need to send via body not request params
        params = [{"key": "code", "values": exclude['codes']}]
        resources_found = self.search_resource_by_params(exclude['resource'], params, collection)

        if resources_found:
            collection_source = "task_" + self.task_id + "_excl"
            collection_target = "task_" + self.task_id + "_excl_enc"
            self.get_encs(collection_source, collection_target)
            self.filter(exclude)

    def process_search_results(self, search_result, collection):

        # TODO: Check if FHIR can resolve the dependencies, so less calls necessary

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
        pgCur.execute("CREATE TABLE IF NOT EXISTS " + collection + " (ID serial NOT NULL PRIMARY KEY, data jsonb NOT NULL);")
        pgCur.execute("TRUNCATE TABLE " + collection + " ;")

        insertSql = "INSERT INTO " + collection + "(data) VALUES %s"

        entry_list = []
        for entry in result_list:
            entry_list.append((json.dumps(entry),))

        execute_values(pgCur, insertSql, entry_list, "(%s::jsonb)")
        connection.commit()
        return True

    def search_resource_by_params(self, resource_name, params, collection):

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
                values = ','.join(map(str, values))
                serverSearchParams[key] = values

            search = resource.where(serverSearchParams)
            search_result = search.perform(server.server)

        except Exception:
            logger.error("Search failed", exc_info=1)
            raise

        if(search_result.entry is None or len(search_result.entry) == 0):
            logger.info("No values found for search on resource " + resource_name)
            return False

        return self.process_search_results(search_result, collection)
