import argparse
import json
import socket

from confluent_kafka import Producer


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


def send_message(bootstrap_server, topic):
    conf = {"bootstrap.servers": bootstrap_server, "client.id": socket.gethostname()}

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
        "--bootstrap-servers",
        dest="bootstrap_servers",
        help="Kafka Brokers",
        required=True,
    )

    parser.add_argument("--topic", dest="topic", help="Topic", required=True)

    args = parser.parse_args()

    send_message(args.bootstrap_servers, args.topic)
