# cloud-dataflow-examples
A repository for Apache Beam/Cloud Dataflow examples.

## Directory

*   streaming_examples
    *   pub_sub_dead_letter_pattern

        ```bash
        python3 -m streaming_examples.pub_sub_dead_letter_pattern \
            --input_subscription $INPUT_SUBSCRIPTION \
            --dead_letter_topic $DEAD_LETTER_TOPIC \
            --region $REGION \
            --runner $RUNNER \
            --project $PROJECT \
            --temp_location $TEMP_LOCATION 
        ```
    *   stateful_processing_examples
        *   stateful_taxi
            ```bash
            python3 -m streaming_examples.stateful_processing_examples.stateful_taxi \
                --input_subscription $INPUT_SUBSCRIPTION \
                --topic $PUBLISH_TOPIC \
                --region $REGION \
                --runner $RUNNER \
                --project $PROJECT \
                --temp_location $TEMP_LOCATION 
            ```
    *   write_to_csql_examples
        * write_to_csql_cloud_sql_connector

            ```bash
            python3 -m streaming_examples.write_to_csql_examples.write_to_csql_cloud_sql_connector \
                --region $REGION 
                --input_subscription $INPUT_SUBSCRIPTION 
                --dead_letter_topic $PUBLISH_TOPIC 
                --runner $RUNNER
                --project $PROJECT 
                --temp_location $TEMP_LOCATION 
                --mysql_connection_name $MYSQL_CONNECTION_NAME 
                --mysql_user $MYSQL_USER 
                --mysql_pass $MYSQL_PASS 
                --mysql_db $MYSQL_DB 
                --streaming true
            ```