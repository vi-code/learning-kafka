import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab_kafka.client import consumer, decode_message, topic_name


def main() -> None:
    topic = topic_name(os.getenv("TOPIC_SUFFIX", "orders"))
    group_id = os.getenv("GROUP_ID", "replay-demo-v1")
    kafka_consumer = consumer(group_id)
    kafka_consumer.subscribe([topic])

    print(f'Reading {topic} as group "{group_id}". Press Ctrl+C to stop.')

    try:
        while True:
            message = kafka_consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(message.error())
                continue

            event = decode_message(message)
            print(
                {
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "key": event["key"],
                    "value": event["value"],
                }
            )
            kafka_consumer.commit(message=message, asynchronous=False)
    except KeyboardInterrupt:
        print("Stopping consumer.")
    finally:
        kafka_consumer.close()


if __name__ == "__main__":
    main()
