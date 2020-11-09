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
from lib.FeatureTask import FeatureTask
import sys
from lib.RefIntLogger import RefIntLogger
log = RefIntLogger()

settings = {
    'app_id': 'ketos_data',
    'api_base': configuration.HAPIFHIR_URL,
}

server = client.FHIRClient(settings=settings)


class FeatureTaskPsql(FeatureTask):

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

        log.debug("Task," + self.filter_task_id + ":" + "Begin feature task for filter task")

        # get patients search
        collection_base = "task_" + self.filter_task_id + "_result"

        # get patient ids from observation
        sql = "select distinct data->'subject'->>'reference' from " + collection_base + ";"

        pgCur = psqlDbConnection.get_db_connection().cursor()

        pgCur.execute(sql)

        pat_ids = []
        item = pgCur.fetchone()

        while item is not None:
            pat_ids.append(item[0].split('/')[1])
            item = pgCur.fetchone()

        collection_patient = "task_" + self.filter_task_id + "_pat "   

        connection = psqlDbConnection.get_db_connection()
        pgCur = connection.cursor()
        sql = "create table " + collection_patient
        sql = sql + ''' 
          (
              id         SERIAL,
              fhir_id    varchar(64) NOT NULL,
              type       varchar(64) NOT NULL,
              data       jsonb       NOT NULL,
              created_at timestamp   NOT NULL DEFAULT NOW(),
              last_updated_at timestamp   NOT NULL DEFAULT NOW(),
              is_deleted      boolean     NOT NULL DEFAULT FALSE
          )
        '''

        pgCur.execute(sql)
        connection.commit()

        pat_ids_final = (",".join(map(lambda x: "'" + x + "'", pat_ids)))

        select_pats = "SELECT * from resources where type = 'Patient' and fhir_id in (" + pat_ids_final + ")"

        fhir_connection = psqlDbConnection.get_fhir_db_connection()
        fhir_pgCur = fhir_connection.cursor()
        
        fhir_pgCur.execute(select_pats)
        item = fhir_pgCur.fetchmany(10000)

        connection = psqlDbConnection.get_db_connection()
        pgCur = connection.cursor()
        insertSql = "INSERT INTO " + collection_patient + " VALUES %s"

        while len(item) > 0:
            item_list_insert = []
            for cur_item in item:
                lst = list(cur_item)
                lst[3] = json.dumps(lst[3])
                cur_item = tuple(lst)
                item_list_insert.append(cur_item)

            execute_values(pgCur, insertSql, item_list_insert, "(%s, %s, %s, %s::jsonb, %s, %s, %s)")
            item = fhir_pgCur.fetchmany(10000)

        connection.commit()
        #for cur_pat_ids in pat_final:
        #    params = [{"key": "_id", "values": cur_pat_ids}]
        #    self.search_resource_by_params("Patient", params, collection_patient)

        log.debug("Task," + self.filter_task_id + ":" + "Begin creation of feature set for filter task")
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
                               "TO_TIMESTAMP(" + self.build_feature_from_val_path(feature['computed']['field_1']) + ",'YYYY-MM-DDTHH24:MI:SS') - " +
                               "TO_TIMESTAMP(" + self.build_feature_from_val_path(feature['computed']['field_2']) + ",'YYYY-MM-DDTHH24:MI:SS')" +
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
        sqlFrom = ("from task_" + task_id + "_result as Observation" +
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




