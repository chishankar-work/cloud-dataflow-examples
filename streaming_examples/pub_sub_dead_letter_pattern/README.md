# Pub/Sub Dead-Lettering Pattern

Currently Dataflow does not have native integration with [Cloud Pub/Sub dead-lettering](https://cloud.google.com/pubsub/docs/handling-failures) due to the current method of propogating the exception upwards. This example is a workaround for being able to effectively do the same thing, except that this particular workaround does not take into account # of retries. To do that, I would recommend looking into Redis as a way to maintain state of retry attempts on message_id's. 

```bash
python3 -m streaming_examples.pub_sub_dead_letter_pattern \
    --input_subscription $INPUT_SUBSCRIPTION \
    --dead_letter_topic $DLQ_TOPIC \
    --region $REGION \
    --runner $RUNNER \
    --project $PROJECT \
    --temp_location $TEMP_LOCATION 
```

