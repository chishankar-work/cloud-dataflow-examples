import logging
from apache_beam.transforms import DoFn
from apache_beam import pvalue
from streaming_examples.write_to_csql_examples.write_to_csql_cloud_sql_connector.exceptions.cloud_sql_exceptions import CloudSqlException
from streaming_examples.write_to_csql_examples.write_to_csql_cloud_sql_connector.utils.mysql_connection_handler import SqlEngine

class WriteToCloudSqlDoFn(DoFn):
    """Write to CloudSQL

    Raises:
        CloudSqlException: A custom Cloud SQL Exception for catching insert errors

    Yields:
        PCollection: Collection of pelements headed for Dead Letter Queue
    """
    dlq_message_tag = "dlq_messages"

    def __init__(self, db_config):
        self.batch_size = 500
        self.db_config = db_config

    def setup(self):
        self._csql_connection_pool = SqlEngine(self.db_config)

    def start_bundle(self):
        self.records = []

    def process(self, element, *args, **kwargs):

        try:
            if len(self.records) >= self.batch_size:
                self.commit_records()

            else:
                self.records.append(element)

        except CloudSqlException as csql_exp:
            # Dead letter the batch within the bundle that threw an error inserting in Cloud SQL
            for r in self.records:
                yield pvalue.TaggedOutput(self.dlq_message_tag, r)

            self.records = []

            logging.warn(
                f"WriteToCloudSql.Process() -> CloudSqlException: {csql_exp}")

        except Exception as exp:

            logging.warn(f"WriteToCloudSql.Process() -> Exception: {exp}")

    def commit_records(self):
        if len(self.records) == 0:
            return

        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        # https://cloud.google.com/sql/docs/mysql/manage-connections#opening_and_closing_connections

        with self._csql_connection_pool.connect() as conn:

            try:
                # Prepared Statement
                sql_insert_query = "INSERT INTO (%s) Values (%s);"

                for r in self.records:
                    sql_query_params = (self.db_config['mysql_databasename'], r)
                    conn.execute(sql_insert_query, sql_query_params)

                conn.execute("COMMIT;")
                logging.info(f"Committed {len(self.records)} records.")
                self.records = []

            except Exception as exp:
                conn.execute("ROLLBACK;")
                logging.warn(
                    f"WriteToCloudSql.commit_records() -> Exception: {exp}")
                raise CloudSqlException

    def finish_bundle(self):
        self.commit_records()

    def teardown(self):
        self._csql_connection_pool.dispose()