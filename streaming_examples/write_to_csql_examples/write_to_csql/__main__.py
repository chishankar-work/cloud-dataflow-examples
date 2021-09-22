import logging
from write_to_csql_examples.write_to_csql import write_to_sql_pipeline

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  write_to_sql_pipeline.run()

# sudo python3 -m write_to_csql_examples.write_to_csql --setup_file write_to_csql_examples/setup.py --region $REGION --input_subscription $INPUT_SUBSCRIPTION --dead_letter_topic $PUBLISH_TOPIC --runner DataflowRunner --project $PROJECT --temp_location $TEMP_LOCATION --mysql_connection_name $MYSQL_CONNECTION_NAME --mysql_user $MYSQL_USER --mysql_pass $MYSQL_PASS --mysql_db $MYSQL_DB --streaming true