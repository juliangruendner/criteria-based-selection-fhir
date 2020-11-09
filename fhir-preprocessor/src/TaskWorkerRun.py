import threading
import json
import time
from lib import psqlDbConnection
from lib.FilterTaskPsql import FilterTaskPsql
from datetime import datetime
import sys
import psycopg2
import configuration
from lib.RefIntLogger import RefIntLogger
from concurrent.futures import ThreadPoolExecutor


log = RefIntLogger()
initialized = False


def execute_task(task):

    try:
        filter_task = FilterTaskPsql(task['task_id'], "running", task['base_search'], task['include'], task['exclude'], task['feature_set'])
        log.info("TASK WORKER: Executing task with id: " + task['task_id'])
        filter_task.execute_task()

    except Exception as e:
        log.error("TASK WORKER: ERROR executing task, stack trace: " + str(e))
        connection = psqlDbConnection.get_db_connection()
        log.error("TASK WORKER: after db connection")
        pgCur = connection.cursor()
        sql = "UPDATE filter_tasks set data = %s where data->>'task_id' = %s"
        filter_task.status = 'ERROR'
        filter_task.error_message = str(e)
        pgCur.execute(sql, (json.dumps(filter_task.__dict__), filter_task.task_id))
        connection.commit()
        return


def main():
    executor = ThreadPoolExecutor(max_workers=4)

    connection = psqlDbConnection.get_db_connection()

    while True:
        pgCur = connection.cursor()
        sql = "SELECT data FROM filter_tasks WHERE data->>'status' = 'new'"
        pgCur.execute(sql)
        task = pgCur.fetchone()

        if(task is None):
            time.sleep(10)
            continue

        task = task[0]
        sql = "UPDATE filter_tasks set data = %s where data->>'task_id' = %s"
        task['status'] = 'queued'
        pgCur.execute(sql, (json.dumps(task), task['task_id']))
        connection.commit()

        log.debug("Async execution of task = " + str(task))

        executor.submit(execute_task(task))


if __name__ == "__main__":
    # execute only if run as a script
    main()
