from flask_sqlalchemy import SQLAlchemy
from lib import psqlDbConnection
import sqlalchemy.types as types
import configuration


db = SQLAlchemy()


class LowerCaseText(types.TypeDecorator):
    '''Converts strings to lower case on the way in.'''

    impl = types.Text

    def process_bind_param(self, value, dialect):
        return value.lower()


def connect_to_db(app):
    """Connect the database to Flask app."""

    # Configure to use PostgreSQL database
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + config.POSTGRES_USER + ':' + config.POSTGRES_PASSWORD + '@'+ config.POSTGRES_HOST + ':5432/' + config.POSTGRES_DB
    db.app = app
    db.init_app(app)


def create_base_db():

    connection = psqlDbConnection.get_db_connection()
    pgCur = connection.cursor()
    tables = ["filter_tasks (ID serial NOT NULL PRIMARY KEY, data jsonb NOT NULL);", "feature_tasks (ID serial NOT NULL PRIMARY KEY, data jsonb NOT NULL);"]
    
    for table in tables:
        pgCur.execute("CREATE TABLE IF NOT EXISTS " + table)

    connection.commit()

    
