import argparse
import logging
import json
import datetime

import apache_beam as beam
from apache_beam import window
from apache_beam import trigger
from apache_beam.coders.coders import TupleCoder, StrUtf8Coder
from apache_beam.transforms.userstate import BagStateSpec
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions
import os

TIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

# taxi_data = {'taxi_id': taxi_id,
#                     'taxi_status': taxi_state,
#                     'status_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}


# Conversion to Unix Time=
def to_unix_time(time_str, format=TIME_FORMAT):
    epoch = datetime.utcfromtimestamp(0)
    dt = datetime.strptime(time_str, format)
    return (dt - epoch).total_seconds() * 1000.0


# Sets the taxi_id as the key
class SetTaxiKeyFn(beam.DoFn):

    def process(self, element):
        # Convert messages to JSON element
        element = json.loads(element)

        # Sets the taxi_id as key
        yield element['taxi_id'], element

# Stateful ParDo for storing most recent status of taxi_id
class StatefulParDoFn(beam.DoFn):
    TAXI_STATE = BagStateSpec('taxi', TupleCoder((StrUtf8Coder(), StrUtf8Coder())))

    def __init__(self):
        self.BUSY = "busy"
        self.AVAILABLE = "available"
        self.IDLE = "idle"

    def process(self, element, prev_taxi_state=beam.DoFn.StateParam(TAXI_STATE)):

        curr_taxi_status = element[1]['taxi_status']
        curr_taxi_timestamp = element[1]['status_time']

        prev_taxi_info = [x for x in prev_taxi_state.read()]
        update_curr_taxi_flag = False
        new_tup = [0, 0, 0]

        # If there was a previous state listed for this key (job_id)
        if prev_taxi_info:
            prev_taxi_timestamp = prev_taxi_info[0][1]
            prev_taxi_status = prev_taxi_info[0][0]
            # logging.getLogger().info(f"Previous status found for {curr_job_id} -> {prev_job_status}")

            # Compare timestamps if new element is newer than previous job info
            if to_unix_time(curr_taxi_timestamp) - to_unix_time(prev_taxi_timestamp) > 0:

                # Compare previous state to current, update the timestamp
                if prev_taxi_status == curr_taxi_status:
                    # prev_taxi_state.clear()
                    # prev_taxi_state.add((curr_taxi_status, curr_taxi_timestamp))

                    # Don't need to modify anything if the status is the same
                    yield tuple([0, 0, 0])

                # If the state is different and current element is newer
                else:
                    prev_taxi_state.clear()
                    # logging.getLogger().info(f"Previous status for {curr_job_id} is {prev_job_status}, updating to {curr_job_status}")
                    # Update prev_job_state with newer curr_job_info
                    prev_taxi_state.add((curr_taxi_status, curr_taxi_timestamp))
                    update_curr_taxi_flag = True

                    # If previous state is older than current job info, then subtract from respective state
                if prev_taxi_status == self.IDLE:
                    new_tup[2] = -1
                elif prev_taxi_status == self.AVAILABLE:
                    new_tup[1] = -1
                elif prev_taxi_status == self.BUSY:
                    new_tup[0] = -1

            # if current element is not newer then don't update anything return empty values
            else:
                yield tuple([0, 0, 0])

        # If prev_taxi_info is null, then initializes the state and timestamp of new job
        else:
            # logging.getLogger().info(f"Adding new state for job {curr_job_id}")
            prev_taxi_state.add((curr_taxi_status, curr_taxi_timestamp))
            update_curr_taxi_flag = True

        if update_curr_taxi_flag:
            # add value to respective state
            if curr_taxi_status == self.IDLE:
                new_tup[2] = 1
            elif curr_taxi_status == self.AVAILABLE:
                new_tup[1] = 1
            elif curr_taxi_status == self.BUSY:
                new_tup[0] = 1

            # logging.getLogger().info(f"Tuple Value for {curr_job_id}: {new_tup}")
            yield tuple(new_tup)


# Sets the formats data
class FormatMetricsData(beam.DoFn):

    def setup(self):
        self.time_format = TIME_FORMAT

    def process(self, element):
        current_metrics = json.dumps(
            {'busy': element[0],
             'available': element[1],
             'idle': element[2],
             'timestamp': datetime.now().strftime(self.time_format)}).encode('utf-8')

        yield current_metrics


# CombineFn for handling global accumulator
class GetCurrentJobMetrics(beam.CombineFn):

    # Defining the accmulator init state
    def create_accumulator(self):
        return 0, 0, 0

    # Merges accumulators across all keys together since you can't guarantee all keys are
    # processed on the same machine.
    def merge_accumulators(self, accumulators):

        busy = sum([accum[0] for accum in accumulators])
        available = sum([accum[1] for accum in accumulators])
        idle = sum([accum[2] for accum in accumulators])

        return busy, available, idle

    def add_input(self, accumulator, input):

        return accumulator[0] + input[0], accumulator[1] + input[1], accumulator[2] + input[2]

    def extract_output(self, accumulator):
        return accumulator


def run(input_subscription, topic, pipeline_args=None):
    # Pipeline options
    pipeline_options = PipelineOptions(
        pipeline_args,
        streaming=True,
        save_main_session=True,
        job_name="stateful-dofn-taxi-test-after-count-5k",
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
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/chishankar/Documents/testCode/credentials3.json"

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
        pipeline_args,
    )
