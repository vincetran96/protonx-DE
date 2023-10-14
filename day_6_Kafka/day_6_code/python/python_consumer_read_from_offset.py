import argparse
import binascii
import json

from confluent_kafka import (
    Consumer,
    DeserializingConsumer,
    KafkaError,
    KafkaException,
    TopicPartition,
)


def get_message_with_offset(bootstrap_server, topic, consumer_group, partition, offset):
    conf = {
        "bootstrap.servers": bootstrap_server,
        "group.id": consumer_group,
        "auto.offset.reset": "smallest",
    }

    consumer = Consumer(conf)

    partition = TopicPartition(topic, partition, offset)

    consumer.assign([partition])
    consumer.seek(partition)
    # Read the message
    consume_loop(consumer, [topic])


def consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(
                        "%% %s [%d] reached end at offset %d\n"
                        % (msg.topic(), msg.partition(), msg.offset())
                    )
                elif msg.error():
                    raise KafkaException(msg.error())

            else:
                process_message(msg)
    except KeyboardInterrupt:
        print("Key Board Interrupt")
    finally:
        consumer.close()


def process_message(msg):
    print(
        f"topic: {msg.topic()}, partition: {msg.partition()}, offset: {msg.offset()},  header {msg.headers()}, key: {msg.key() }, value :{msg.value()} , timestamp : {msg.timestamp()}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        """
            Simple Kafka Consumer
        """
    )

    parser.add_argument(
        "--bootstrap-servers",
        dest="bootstrap_servers",
        help="Kafka Brokers",
        required=True,
    )

    parser.add_argument("--topic", dest="topic", help="Topic", required=True)

    parser.add_argument(
        "--consumer-group", dest="consumer_group", help="Consumer Group", required=True
    )
    parser.add_argument(
        "--partition", dest="partition", help="Partition", required=True, type=int
    )
    parser.add_argument(
        "--offset", dest="offset", help="Offset", required=True, type=int
    )

    args = parser.parse_args()

    try:
        get_message_with_offset(
            args.bootstrap_servers,
            args.topic,
            args.consumer_group,
            args.partition,
            args.offset,
        )
    except KeyboardInterrupt:
        print("Exiting...")
        exit(0)
