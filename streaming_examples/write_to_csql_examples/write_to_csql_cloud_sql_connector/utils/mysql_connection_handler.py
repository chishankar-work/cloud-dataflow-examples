from pymysql.connections import Connection
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
from google.cloud.sql.connector import connector
import logging


class SqlEngine():
    def __init__(self, db_config):
        self.db_config = db_config
        pass

    def init_connection_engine(self) -> Engine:
        engine = create_engine(
            "mysql+pymysql://",
            creator=self.getconn(),
        )
        return engine

    def getconn(self) -> Connection:
        conn: Connection = connector.connect(
            self.db_config['mysql_connection_name'],
            self.db_config['myql_database_driver'],
            self.db_config['mysql_username'],
            self.db_config['mysql_password'],
            self.db_config['mysql_databasename']
        )
        return conn

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)