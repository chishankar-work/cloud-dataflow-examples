# Pub/Sub Read -> Write CloudSQL[MySQL] via jdbcIO

---
**NOTE**

Requires building a custom expansion service to serve the "com.mysql.cj.jdbc.Driver" jar during pipeline submission. I have included the jar in this repository `beam-sdks-java-extensions-schemaio-expansion-service-2.31.0-SNAPSHOT.jar`. Run the expansion service with the following command *before* you submit the job. Beam will request the jar from the expansion service listening on `$EXPANSION_SERVICE_PORT`.

```bash
java -jar beam-sdks-java-extensions-schemaio-expansion-service-2.31.0-SNAPSHOT.jar $EXPANSION_SERVICE_PORT
```
---

```bash
python3 -m streaming_examples.write_to_csql_examples.write_to_csql_jdbcio \
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
    --mysql_table $MYSQL_TABLE \
    --mysql_expansion_service $EXPANSION_SERVICE_PORT \
    --mysql_ip $MYSQL_IP \
    --mysql_port $MYSQL_PORT \
    --mysql_instance_name $MYSQL_INSTANCE \
    --job_name $UNIQUE_JOB_NAME \
    --subnetwork $SUBNET \
    --experiment="use_runner_v2" \
    --experiment="use_portable_job_submission" \
    --experiment="beam_fn_api" \
    --setup_file "./setup.py" \
    --streaming true \
    --save_main_session true \
    --performance_runtime_type_check true # Read documentation on when to use.
```                

## Helpful Links

* [Multi-Lanugage Pipelines - Beam Documentation](http://beam.apache.org/documentation/programming-guide/#multi-language-pipelines)
*  [apache_beam.io.jdbc module - Beam Documentation](https://beam.apache.org/releases/pydoc/2.29.0/apache_beam.io.jdbc.html)   
* [Multi-language Dataflow pipelines enabled by new, faster architecture - Google Cloud Blog](https://cloud.google.com/blog/products/data-analytics/multi-language-sdks-for-building-cloud-pipelines)
