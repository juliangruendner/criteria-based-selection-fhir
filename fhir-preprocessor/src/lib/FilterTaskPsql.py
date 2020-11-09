import importlib
import datetime
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
import sqlparse
from resources import aggregationtaskResource
from lib.FilterTask import FilterTask
from lib.FeatureTaskPsql import FeatureTaskPsql
from lib.RefIntLogger import RefIntLogger

log = RefIntLogger()

settings = {
    'app_id': 'ketos_data',
    'api_base': configuration.HAPIFHIR_URL,
}

server = client.FHIRClient(settings=settings)

class FilterTaskPsql(FilterTask):

    def get_filter_prefilter(self, constraint):

        codes = "(" + ",".join(map(lambda x: "'" + x + "'", constraint['codes'])) + ")"

        sql_filter_select = "type = '" + constraint['resource'] + "' AND " + " data -> 'code' -> 'coding' -> 0 ->> 'code'"

        if constraint['search_type'] == 'begins':
            sql_filter_select = sql_filter_select + " like '" + constraint['codes'][0] + "%%'"
        else:
            sql_filter_select = sql_filter_select + " in" + codes

        if 'value_restriction' in constraint:
            operator = constraint['value_restriction']['operator']
            compare_to = constraint['value_restriction']['compare_to']
            sql_filter_select = sql_filter_select + " AND TO_NUMBER( data->'valueQuantity'->>'value', '99.9') " + operator + " " + str(compare_to)

        return sql_filter_select

    def build_selection_from_val_path(self, constraint_type, selection):
        sql_feature = constraint_type + ".data"
        valPath = selection.split(".")
        index = 0
        while index < (len(valPath) - 1):
            if valPath[index].isdigit():
                sql_feature = sql_feature + " -> " + valPath[index]
            else:
                sql_feature = sql_feature + " -> '" + valPath[index] + "'"

            index = index + 1

        sql_feature = sql_feature + " ->> '" + valPath[index] + "'"

        return sql_feature

    def get_filter_statement(self, constraint, constraint_type):
        interval_m = constraint['time_interval']
        sql_filter_select = ""
        params = ()

        if interval_m > 0:
            dateField = self.build_selection_from_val_path(constraint_type, constraint['dateField'])
            sql_filter_select = (
                                " ((TO_TIMESTAMP(base.data ->> %s, 'YYYY-MM-DDTHH24:MI:SS') >="
                                " TO_TIMESTAMP(enc.data -> 'period'->> 'start', 'YYYY-MM-DDTHH24:MI:SS') - interval '%s days'"
                                " AND"
                                " TO_TIMESTAMP(base.data ->> %s, 'YYYY-MM-DDTHH24:MI:SS') <="
                                " TO_TIMESTAMP(enc.data -> 'period'->> 'end', 'YYYY-MM-DDTHH24:MI:SS') + interval '%s days' )"
                                " OR"
                                " (TO_TIMESTAMP(base.data ->> %s, 'YYYY-MM-DDTHH24:MI:SS') >="
                                " TO_TIMESTAMP(" + dateField + ", 'YYYY-MM-DDTHH24:MI:SS') - interval '%s days'"
                                " AND"
                                " TO_TIMESTAMP(base.data ->> %s, 'YYYY-MM-DDTHH24:MI:SS') <="
                                " TO_TIMESTAMP(" + dateField + ", 'YYYY-MM-DDTHH24:MI:SS') + interval '%s days' ))"
                                " AND "
                                )

            dateFieldBase = 'effectiveDateTime'

            params = (dateFieldBase, interval_m, dateFieldBase, interval_m, dateFieldBase, interval_m, dateFieldBase, interval_m)

        codes = "(" + ",".join(map(lambda x: "'" + x + "'", constraint['codes'])) + ")"
        sql_filter_select = sql_filter_select + " " + constraint_type + ".data -> 'code' -> 'coding' -> 0 ->> 'code'" 

        if constraint['search_type'] == 'begins':
            sql_filter_select = sql_filter_select + " like '" + constraint['codes'][0] + "%%'"
        else:
            sql_filter_select = sql_filter_select + " in" + codes

        if 'value_restriction' in constraint:
            operator = constraint['value_restriction']['operator']
            compare_to = constraint['value_restriction']['compare_to']
            sql_filter_select = sql_filter_select + " AND TO_NUMBER(" + constraint_type + ".data->'valueQuantity'->>'value', '99.9') " + operator + " " + str(compare_to)

        return {"sql": sql_filter_select, "params": params}

    def execute_task(self):

        log.debug("Task," + self.task_id + ": " + "Begin filter Task")

        connection = psqlDbConnection.get_db_connection()
        pgCur = connection.cursor()
        sql = "UPDATE filter_tasks set data = %s where data->>'task_id' = %s"
        self.status = 'running'
        self.start_time = str(datetime.now())
        pgCur.execute(sql, (json.dumps(self.__dict__), self.task_id))
        connection.commit()

        result_collection = "task_" + self.task_id + "_result"

        # TODO - Check if it should not rather be select * from base
        prefilter_base = (" WITH base as "
                          " ( SELECT * from resources where type = 'Observation' and resources.data -> 'code' -> 'coding' -> 0 ->> 'code' = '" + self.base_search[0] + "' ),"
                          " enc as (SELECT * from resources where type = 'Encounter'),"
                          )

        params = ()
        include_select = ""
        include_where = ""
        exclude_select = ""
        exclude_where = ""
        prefilter_excl = ""
        prefilter_excl_where = ""
        prefilter_incl = ""
        prefilter_incl_where = ""

        first_constraint = True

        if len(self.include) > 0:
            first_incl = True
            for constraint in self.include:
                cur_constraint = self.get_filter_statement(constraint, "incl")
                params = params + cur_constraint['params']

                if not first_incl:
                    include_where = include_where + "AND"
                    prefilter_incl_where = prefilter_incl_where + "AND"

                first_incl = False
                  
                include_where = include_where + " (" + cur_constraint['sql'] + ") "
                prefilter_incl_where = prefilter_incl_where + " (" + self.get_filter_prefilter(constraint) + ")"

            include_select = (" select distinct base.fhir_id"
                          " from base"
                          " left join incl on base.data -> 'subject' ->> 'reference' = incl.data -> 'subject' ->> 'reference'"
                          " left join enc on incl.data -> 'encounter' ->> 'reference' = concat('Encounter/', enc.data ->> 'id')"
                          " where"
                          )

            prefilter_incl = "incl as ( select * from resources WHERE " + prefilter_incl_where + ")"
            first_constraint = False
        
        if len(self.exclude) > 0:
            
            first_exlcude = True
            for exclude in self.exclude:
                cur_exclude = self.get_filter_statement(exclude, "excl")
                params = params + cur_exclude['params']

                if not first_exlcude:
                    exclude_where = exclude_where + "OR"
                    prefilter_excl_where = prefilter_excl_where + "OR"

                first_exlcude = False
                  
                exclude_where = exclude_where + " (" + cur_exclude['sql'] + ") "
                prefilter_excl_where = prefilter_excl_where + " (" + self.get_filter_prefilter(exclude) + ")"

            exclude_select = (" select distinct base.fhir_id"
                          " from base"
                          " left join excl on base.data -> 'subject' ->> 'reference' = excl.data -> 'subject' ->> 'reference'"
                          " left join enc on excl.data -> 'encounter' ->> 'reference' = concat('Encounter/', enc.data ->> 'id')"
                          " where"
                          )

            prefilter_excl = "excl as ( select * from resources WHERE " + prefilter_excl_where + ")"
            if first_constraint is False:
                prefilter_excl = "," + prefilter_excl

        base = "SELECT * from base WHERE"

        first = True

        if len(self.include) > 0:
            base = base + " base.fhir_id IN"
            first = False
            base = base + "(" + include_select + include_where + ") "

        if len(self.exclude) > 0:
            if first:
                base = base + " base.fhir_id NOT IN"
                first = False
            else:
                base = base + " AND base.fhir_id NOT IN"

            base = base + "(" + exclude_select + exclude_where + ")"

        if first:
            base_where = " type = 'Observation' and base.data -> 'code' -> 'coding' -> 0 ->> 'code' = '" + self.base_search[0] + "'"
        else:
            base_where = " AND type = 'Observation' and base.data -> 'code' -> 'coding' -> 0 ->> 'code' = '" + self.base_search[0] + "'"

        sql = prefilter_base + prefilter_incl + prefilter_excl + base + base_where

        log.debug("Task," + self.task_id + ":" + "begin execution of filter sql on fhir server")
        fhir_connection = psqlDbConnection.get_fhir_db_connection()
        fhir_pgCur = fhir_connection.cursor()
        sql_statement = fhir_pgCur.mogrify(sql, params)
        self.gen_filter_query = sql_statement.decode("UTF-8")
        log.debug("Task," + self.task_id + " sql params:" + str(params))
        log.debug("Task," + self.task_id + " sql: \n #########\n " + sqlparse.format(sql_statement, reindent=True, keyword_case='upper') + "\n #########\n ")

        try:
            fhir_pgCur.execute(sql, params)

        except Exception as e:
            log.error("TASK WORKER: ERROR executing task, stack trace: " + str(e))
            connection = psqlDbConnection.get_db_connection()
            log.error("TASK WORKER: after db connection")
            pgCur = connection.cursor()
            sql = "UPDATE filter_tasks set data = %s where data->>'task_id' = %s"
            self.status = 'ERROR'
            self.error_message = str(e)
            pgCur.execute(sql, (json.dumps(self.__dict__), self.task_id))
            connection.commit()

        item = fhir_pgCur.fetchmany(10000)
        connection = psqlDbConnection.get_db_connection()
        pgCur = connection.cursor()

        sql = "create table " + result_collection
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
        log.debug("Task," + self.task_id + ":" + "Finished execution of filter sql on fhir server")

        connection = psqlDbConnection.get_db_connection()
        pgCur = connection.cursor()
        insertSql = "INSERT INTO " + result_collection + " VALUES %s"

        while len(item) > 0:
            item_list_insert = []
            for cur_item in item:
                lst = list(cur_item)
                lst[3] = json.dumps(lst[3])
                cur_item = tuple(lst)
                item_list_insert.append(cur_item)

            execute_values(pgCur, insertSql, item_list_insert, "(%s, %s, %s, %s::jsonb, %s ,%s, %s)")
            item = fhir_pgCur.fetchmany(10000)

        connection.commit()

        feat_task_id = str(ObjectId())
        feature_task = FeatureTaskPsql(feat_task_id, "running", self.feature_set, self.task_id)
        try:
            feature_task.execute_task()
        except Exception as e:
            log.error("TASK WORKER: ERROR executing task, stack trace: " + str(e))
            connection = psqlDbConnection.get_db_connection()
            log.error("TASK WORKER: after db connection")
            pgCur = connection.cursor()
            sql = "UPDATE filter_tasks set data = %s where data->>'task_id' = %s"
            self.status = 'ERROR'
            self.error_message = str(e)
            pgCur.execute(sql, (json.dumps(self.__dict__), self.task_id))
            connection.commit()
            return
        
        pgCur = connection.cursor()
        sql = "UPDATE filter_tasks set data = %s where data->>'task_id' = %s"
        self.status = 'done'
        pgCur.execute(sql, (json.dumps(self.__dict__), self.task_id))
        connection.commit()
