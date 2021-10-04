import argparse
from datetime import datetime, timedelta
import json
import pprint
import random
import time
import math
import os

from google.cloud import pubsub_v1

# This is a utility script to publish Taxi data to a topic for consumption by the example pipelines in this project.


def getPublisherClient():
  batch_settings = pubsub_v1.types.BatchSettings(max_messages=1000)

  return pubsub_v1.PublisherClient(batch_settings)


def calculateNumberOfMessages(n):
  try:
    return math.ceil(10**9 / int(n))
  except Exception as exp:
    print(exp)
    return 1


def publishMessages(num_messages, num_taxis, batch_wait_time):
  topic_name = os.environ['PUBLISH_TOPIC']
  time_format = "%Y-%m-%d %H:%M:%S.%f"

  publisher = getPublisherClient()

  batch_taxi_ids = []
  taxi_status = {}

  for i in range(1, num_messages + 1):

    # get taxi_id
    taxi_id = i % num_taxis

    # wait batch_wait_time so
    if taxi_id == 0:
      batch_taxi_ids = []
      taxi_id = num_taxis
      time.sleep(batch_wait_time)

    if i % 10000 == 0:
      print(f"Number of messages published: {i}")

    # checks so that each batch of messages doesn't contain a
    # message for the same jobId
    while taxi_id in batch_taxi_ids:
      taxi_id = random.randrange(1, num_taxis + 1)

    # create random state of jobId
    taxi_state = random.choice(["busy", "available", "idle"])

    # create job data
    taxi_data = {
        "taxi_id": taxi_id,
        "taxi_status": taxi_state,
        "status_time": datetime.now().strftime(time_format)
    }

    # print(json.dumps(taxi_data))
    # Data must be a bytestring
    encoded_taxi_data = json.dumps(taxi_data).encode("utf-8")

    # print(encoded_taxi_data)
    # Collect confirmation and process
    publisher.publish(topic_name, encoded_taxi_data)

    # Keep track of the state
    taxi_status[taxi_id] = taxi_state
    batch_taxi_ids.append(taxi_id)

  print("==== End Results ==== \n")
  # print(f"Number of unique JobIds: {len(set(unique_id))}")
  busy = sum(x == "busy" for x in taxi_status.values())
  available = sum(x == "available" for x in taxi_status.values())
  idle = sum(x == "idle" for x in taxi_status.values())
  print(f"busy - {busy} | available - {available} | idle - {idle} \n")
  pprint.pprint(taxi_status)


if __name__ == "__main__":
  # args
  parser = argparse.ArgumentParser()

  os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.environ[
      'GOOGLE_APPLICATION_CREDENTIALS']

  parser.add_argument("--num_messages",
                      help="Number of messages to publish to topic",
                      default=11560693)

  parser.add_argument("--num_taxis",
                      help="Number of unique job_ids",
                      default=200)

  parser.add_argument(
      "--batch_wait_time",
      help=
      "Length of time to wait between each publish request for throughput test",
      default=0.0)

  parser.add_argument("--load_test_size",
                      help="Gigabytes worth of taxis",
                      required=False)

  known_args, pipeline_args = parser.parse_known_args()

  num_messages = known_args.num_messages

  if known_args.load_test_size is not None:
    num_messages = calculateNumberOfMessages(known_args.load_test_size)

  print(f"Will publish {num_messages} taxi messages.")

  publishMessages(int(num_messages), int(known_args.num_taxis),
                  float(known_args.batch_wait_time))
'''
'{"taxi_id": 125, "taxi_status": "available", "status_time": "2021-09-28 20:03:29.864520"}' = 89 Bytes
'{"taxi_id": 120, "taxi_status": "idle", "status_time": "2021-09-28 20:03:29.864106"}' = 84 Bytes
(10^9) / 86.5 Bytes => 11560693 messages = 1 Gigabyte
'''