from streaming_examples.stateful_processing_examples.stateful_taxi import stateful_combine_example
import argparse
import logging

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
        "--topic",
        help="The Cloud Pub/Sub topic to publish dead letter messages.\n"
                '"projects/<PROJECT_NAME>/subscriptions/<SUBSCRIPTION_NAME>".',
    )

    known_args, pipeline_args = parser.parse_known_args()

    stateful_combine_example.run(known_args.input_subscription,known_args.topic,pipeline_args)