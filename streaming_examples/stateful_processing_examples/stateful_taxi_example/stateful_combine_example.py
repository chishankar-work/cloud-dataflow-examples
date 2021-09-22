import argparse
import logging

import apache_beam as beam
from apache_beam import window
from apache_beam import trigger
from apache_beam.options.pipeline_options import PipelineOptions
from stateful_processing_examples.stateful_taxi_example.utils.dofns import SetTaxiKeyFn, StatefulParDoFn, FormatMetricsData
from stateful_processing_examples.stateful_taxi_example.utils.combinefns import GetCurrentJobMetrics

def run(input_subscription, topic, pipeline_args=None):
    # Pipeline options
    pipeline_options = PipelineOptions(
        pipeline_args,
        streaming=True,
        save_main_session=True,
        job_name="stateful-dofn-taxi-test-1",
        num_workers=10,
        max_num_workers=10)
    
    # Trigger after processing time (seconds)
    global_window_after_processing_time = 60
    global_window_after_count = 5000

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # [1] Get messages from PubSub
        pubsub_messages = (pipeline | "ReadPubSubMessages" >> beam.io.ReadFromPubSub(
            subscription=input_subscription))

        # [2] Set the JobId as the key
        rekeyed_messages = (pubsub_messages | "SetJobKey" >> beam.ParDo(SetTaxiKeyFn()))

        # [3] Filter and store the most recent taxi_state for taxi_id
        taxi_tuple_status = (rekeyed_messages | "BagStatefulDoFn" >> beam.ParDo(StatefulParDoFn()))

        # [4] Move to global window to maintain a global accumulation
        taxi_tuple_status_global_window = (taxi_tuple_status | "GlobalWindow" >> beam.WindowInto(window.GlobalWindows(),
         trigger=beam.trigger.Repeatedly(
             trigger.AfterAny(trigger.AfterProcessingTime(global_window_after_processing_time),
             trigger.AfterCount(global_window_after_count))),
         accumulation_mode=beam.trigger.AccumulationMode.ACCUMULATING))

        # [5] Accumulate and maintain running talley of taxis in each state
        taxi_status_count = (taxi_tuple_status_global_window | "CombineGlobally" >> beam.CombineGlobally(GetCurrentJobMetrics()).without_defaults())

        # [6] Format metrics to JSON and timestamp
        formatted_taxi_data = (taxi_status_count | "FormatMetrics" >> beam.ParDo(FormatMetricsData()))

        (formatted_taxi_data | "LogOutput" >> beam.Map(lambda i: logging.getLogger().info(f"Result: {i}")))
        
        # [7] Publishes messages to Cloud Pub/Sub
        (formatted_taxi_data | "Publish to Pub/Sub" >> beam.io.WriteToPubSub(topic=topic))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    # args
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help="The Cloud Pub/Sub topic to read from.\n"
             '"projects/<PROJECT_NAME>/subscriptions/<SUBSCRIPTION_NAME>".',
    )

    parser.add_argument(
        "--topic",
        help="The Cloud Pub/Sub topic to publish dead letter messages.\n"
             '"projects/<PROJECT_NAME>/subscriptions/<SUBSCRIPTION_NAME>".',
    )

    known_args, pipeline_args = parser.parse_known_args()

    run(
        known_args.input_subscription,
        known_args.topic,
        pipeline_args
    )

'''
python3 stateful-combine-example.py  --region $REGION  --input_subscription $INPUT_SUBSCRIPTION —topic $PUBLISH_TOPIC --runner DataflowRunner --project $PROJECT  --temp_location $TEMP_LOCATION
'''