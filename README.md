# cloud_dataflow_examples
A repository for Apache Beam/Cloud Dataflow examples.

## Getting Started

1. `cd cloud_dataflow_examples`
2. `sudo pip3 install -r requirements.txt`
3. Run the pipelines with the parameters required. Example commands are available on the pipeline README's.

## Directory

*   streaming_examples
    *   pub_sub_dead_letter_pattern
        *   [Pub/Sub Dead-Letter Workaround](streaming_examples/pub_sub_dead_letter_pattern/README.md)

    *   stateful_processing_examples
        *   [Stateful Taxi Status](streaming_examples/stateful_processing_examples/README.md)

    *   write_to_csql_examples
        *   [Pub/Sub Read -> Write CloudSQL:MySQL via CloudSql Connector](streaming_examples/write_to_csql_examples/write_to_csql_cloud_sql_connector/README.md)
            
        *   [Pub/Sub Read -> Write CloudSQL:MySQL via jdbcIO](streaming_examples/write_to_csql_examples/write_to_csql_jdbcio/README.md)

If you have any questions, feel free to open an issue. 

## Articles

*   [Fine-tuning Pub/Sub performance with batch and flow control settings](https://medium.com/google-cloud/pub-sub-flow-control-batching-9ba9a75bce3b)
*   [Stateful Processing In Apache Beam/Cloud Dataflow](https://medium.com/google-cloud/stateful-processing-in-apache-beam-cloud-dataflow-109d1880f76a)