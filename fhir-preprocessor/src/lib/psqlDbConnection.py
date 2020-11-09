import psycopg2
import configuration

def get_db_connection():

    connection = None

    try:
        connection = psycopg2.connect(host=configuration.POSTGRES_HOST,
                                      database=configuration.POSTGRES_DB,
                                      user=configuration.POSTGRES_USER,
                                      password=configuration.POSTGRES_PASSWORD,
                                      connect_timeout=120)

    except psycopg2.OperationalError:
        print("ERROR CONNECTING TO DB")
        return connection

    return connection

def get_fhir_db_connection():

    connection = None

    try:
        connection = psycopg2.connect(host=configuration.FHIR_POSTGRES_HOST,
                                      database=configuration.FHIR_POSTGRES_DB,
                                      user=configuration.FHIR_POSTGRES_USER,
                                      password=configuration.FHIR_POSTGRES_PASSWORD,
                                      connect_timeout=120)

    except psycopg2.OperationalError:
        print("ERROR CONNECTING TO DB")
        return connection

    return connection
