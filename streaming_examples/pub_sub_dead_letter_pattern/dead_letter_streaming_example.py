import argparse
import datetime
import json
import logging

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions

# Processes the messages
class FilterMessagesFn(beam.DoFn):
    BAD_MESSAGE_TAG = 'bad_message'
    GOOD_MESSAGE_TAG = 'good_message'

    def process(self, element, window=beam.DoFn.WindowParam):
        try:
            data = element.decode()
            # Tag the elements accordingly
            if 'bad' in data:
                yield pvalue.TaggedOutput(self.BAD_MESSAGE_TAG, element)
            else:
                yield pvalue.TaggedOutput(self.GOOD_MESSAGE_TAG, element)

        # Handle any exceptions in the processing
        except Exception as exp:
            logging.getLogger.warning(exp)
            yield pvalue.TaggedOutput(self.BAD_MESSAGE_TAG, element)

def run(input_subscription,dead_letter_topic, pipeline_args=None):
    # Pipeline options
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Get messages from pubsub
            messages = pipeline | "Read PubSub Messages" >> beam.io.ReadFromPubSub(subscription=input_subscription)
        # Process the messages and tag them accordingly
            message_results = (messages | beam.ParDo(FilterMessagesFn()).with_outputs(
                FilterMessagesFn.BAD_MESSAGE_TAG,
                main=FilterMessagesFn.GOOD_MESSAGE_TAG))
        # Retrieve the messages you want and then handle them a different way
            bad_messages = message_results[FilterMessagesFn.BAD_MESSAGE_TAG]
        # Print bad messages
            (bad_messages | beam.Map(print))
        # HandleBadMessagesFn moves these to another topic
            (bad_messages | beam.io.WriteToPubSub(topic=dead_letter_topic))

if __name__ == "__main__":  # noqa
    logging.getLogger().setLevel(logging.INFO)
    # args
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help="The Cloud Pub/Sub topic to read from.\n"
        '"projects/<PROJECT_NAME>/subscriptions/<SUBSCRIPTION_NAME>".',
    )

    parser.add_argument(
        "--dead_letter_topic",
        help="The Cloud Pub/Sub topic to publish dead letter messages.\n"
        '"projects/<PROJECT_NAME>/subscriptions/<SUBSCRIPTION_NAME>".',
    )

    known_args, pipeline_args = parser.parse_known_args()

    run(
        known_args.input_subscription,
        known_args.dead_letter_topic,
        pipeline_args,
    )