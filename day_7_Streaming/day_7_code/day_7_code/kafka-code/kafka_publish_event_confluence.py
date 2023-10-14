import argparse
import datetime
import json
import time

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
        print("Failed to deliver message")
    else:
        print("Message produced:")


def send_message(conf, topic, event_file):
    
    producer = Producer(conf)
    producer.produce("topic", key="key", value="value")


    with open(event_file) as f:
        for line in f.readlines():
            message = json.loads(line)
            timestamp = message.get("timestamp")
            timestamp = (
                # timestamp
                # # if isinstance(timestamp, int)
                # # else int(
                # #     datetime.datetime.strptime(
                # #         timestamp, "%Y-%m-%dT%H:%M:%SZ"
                # #     ).timestamp()
                # #     * 1000
                # # )
                int(datetime.datetime.utcnow().timestamp() * 1000)
            )
            producer.produce(
                topic,
                key=message.get("key"),
                value=json.dumps(message).encode("utf-8"),
                timestamp=timestamp,
                callback=acked,
            )
            producer.poll(1)
            time.sleep(5)

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

    parser.add_argument("--topic", dest="topic", help="Topic", required=True)

    parser.add_argument(
        "--event-file", dest="event_file", help="Event File", required=True
    )


    args = parser.parse_args()
    send_message(read_ccloud_config(args.config_file), args.topic,args.event_file)
