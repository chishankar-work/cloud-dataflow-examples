import logging
import apache_beam as beam
from apache_beam import Pipeline, io
from apache_beam.options.pipeline_options import PipelineOptions
from streaming_examples.write_to_csql_examples.utils.custom_options import CustomBeamOptions
from streaming_examples.write_to_csql_examples.write_to_csql_cloud_sql_connector.write_to_csql_transform import WriteToCloudSqlDoFn
from streaming_examples.write_to_csql_examples.utils.common_tags import dlq_message_tag

def run(argv=None):

    pipeline_options = PipelineOptions()

    # Initialize Pipeline
    p = Pipeline(options=pipeline_options)

    # Get custom options
    pipeline_config = PipelineOptions().view_as(CustomBeamOptions)

    # Create Cloud SQL Database Connection Config    
    db_config = {"mysql_connection_name": pipeline_config.mysql_connection_name,
                 "mysql_database_driver": pipeline_config.mysql_database_driver,
                 "mysql_username": pipeline_config.mysql_user,
                 "mysql_password": pipeline_config.mysql_pass,
                 "mysql_databasename": pipeline_config.mysql_db}

    # Read from Pub/Sub
    pb_messages = p | io.ReadFromPubSub(
        subscription=pipeline_config.input_subscription)

    # Write to CloudSQL
    write_to_cloud_sql = pb_messages | beam.ParDo(
        WriteToCloudSqlDoFn(db_config)).with_outputs(dlq_message_tag)

    # Get dead letter collection of records
    dlq_records = write_to_cloud_sql[WriteToCloudSqlDoFn.dlq_message_tag]

    # Publish dead letter records to dead_letter_topic
    dlq_records | io.WriteToPubSub(topic=pipeline_config.dead_letter_topic)

    p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()