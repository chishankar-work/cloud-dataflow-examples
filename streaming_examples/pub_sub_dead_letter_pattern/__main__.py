import logging
from pub_sub_dead_letter_pattern import dead_letter_streaming_example
import argparse

if __name__ == '__main__':

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

    dead_letter_streaming_example.run(known_args.input_subscription,
        known_args.dead_letter_topic,
        pipeline_args)