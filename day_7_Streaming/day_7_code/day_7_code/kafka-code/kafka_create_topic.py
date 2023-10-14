import argparse
import datetime
import json
import socket
import time

from confluent_kafka.admin import AdminClient, NewTopic

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        """
            Simple Kafka Topic Creation Script
        """
    )

    parser.add_argument(
        "--bootstrap-servers",
        dest="bootstrap_servers",
        help="Kafka Brokers",
        required=True,
    )
    args = parser.parse_args()

    admin_client = AdminClient({"bootstrap.servers": args.bootstrap_servers})

    topic_list = [
        NewTopic(
            topic="order-fixed-window-topic", num_partitions=3, replication_factor=2
        ),
        NewTopic(
            topic="order-rolling-window-topic", num_partitions=3, replication_factor=2
        ),
        NewTopic(
            topic="order-session-window-topic", num_partitions=3, replication_factor=2
        ),
    ]
    futures = admin_client.create_topics(topic_list)

    for topic_name, future in futures.items():
        future.result()
        print(f"Topic {topic_name} Created")
