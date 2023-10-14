import argparse
import datetime
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

    while True:
        try:
            line = input("Press Enter to send message...:")
            message = json.loads(line)
            timestamp = message.get("timestamp")
            timestamp = (
                timestamp
                if isinstance(timestamp, int)
                else int(
                    datetime.datetime.strptime(
                        timestamp, "%Y-%m-%d %H:%M:%S"
                    ).timestamp()
                    * 1000
                )
            )
            producer.produce(
                topic,
                key=message.get("key"),
                value=message,
                timestamp=timestamp,
                callback=acked,
            )
            producer.poll(1)
        except json.decoder.JSONDecodeError:
            print("Invalid JSON format")
        except KeyboardInterrupt:
            print("Existing")
            break

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
