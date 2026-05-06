import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab_kafka.client import commit_message, consumer, decode_message, topic_name


def main() -> None:
    topic = topic_name("audit")
    group_id = os.getenv("GROUP_ID", "audit-readers")
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
                    "tenantId": event["key"],
                    "sequence": event["value"]["sequence"],
                }
            )
            commit_message(kafka_consumer, message)
    except KeyboardInterrupt:
        print("Stopping consumer.")
    finally:
        kafka_consumer.close()


if __name__ == "__main__":
    main()

