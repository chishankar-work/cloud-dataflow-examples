# Pub/Sub Read -> Write CloudSQL:MySQL via CloudSql Connector

```bash
python3 -m streaming_examples.write_to_csql_examples.write_to_csql_cloud_sql_connector \
    --region $REGION \
    --input_subscription $INPUT_SUBSCRIPTION \
    --dead_letter_topic $DLQ_TOPIC \
    --runner $RUNNER \
    --project $PROJECT \
    --temp_location $TEMP_LOCATION \
    --mysql_connection_name $MYSQL_CONNECTION_NAME \
    --mysql_user $MYSQL_USER \
    --mysql_pass $MYSQL_PASS \
    --mysql_db $MYSQL_DB \
    --streaming true
```

