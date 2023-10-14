import argparse

from confluent_kafka import Consumer, KafkaError, KafkaException


def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                conf[parameter] = value.strip()
    return conf


# offsets_for_times
def get_message(conf, topic, consumer_group):
    conf["group.id"] = consumer_group
    conf["auto.offset.reset"] = "earliest"

    consumer = Consumer(conf)
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
        "--config-file",
        dest="config_file",
        help="Configuration File Path",
        required=True,
    )

    parser.add_argument(
        "--topic",
        dest="topic",
        help="Topic",
        required=True
    )

    parser.add_argument(
        "--consumer-group",
        dest="consumer_group",
        help="Consumer Group",
        required=True
    )

    args = parser.parse_args()

    try:
        get_message(
            read_ccloud_config(args.config_file), args.topic, args.consumer_group
        )
    except KeyboardInterrupt:
        print("Exiting...")
        exit(0)
