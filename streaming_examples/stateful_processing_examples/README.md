# Stateful Processing Example

This example is explained more in the article [Stateful Processing In Apache Beam/Cloud Dataflow](https://medium.com/google-cloud/stateful-processing-in-apache-beam-cloud-dataflow-109d1880f76a).

```bash
python3 -m streaming_examples.stateful_processing_examples.stateful_taxi \
    --input_subscription $INPUT_SUBSCRIPTION \
    --topic $PUBLISH_TOPIC \
    --region $REGION \
    --runner $RUNNER \
    --project $PROJECT \
    --temp_location $TEMP_LOCATION 
```