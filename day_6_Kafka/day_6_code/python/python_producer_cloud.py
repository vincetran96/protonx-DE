import argparse
import json
import socket

from confluent_kafka import Producer


def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                conf[parameter] = value.strip()
    return conf


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


def send_message(conf, topic):
    producer = Producer(conf)
    producer.produce("my-topic", key="key", value="value")

    producer = Producer(conf)

    message_key = b"hello"

    for i in range(10):
        message_value = "some_message_bytes {i}".format(i=i).encode("utf-8")
        producer.produce(topic, key=message_key, value=message_value, callback=acked)
        producer.poll(1)

    producer.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        """
            Simple Kafka Producer
        """
    )

    parser.add_argument(
        "--config-file",
        dest="config_file",
        help="Configuration File",
        required=True,
    )
    parser.add_argument(
        "--topic",
        dest="topic", 
        help="Topic",
        required=True
    )

    args = parser.parse_args()
    send_message(read_ccloud_config(args.config_file), args.topic)
