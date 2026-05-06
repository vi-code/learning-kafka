import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab_kafka.client import commit_message, consumer, decode_message, topic_name


def normalize_customer_event(event: dict) -> dict:
    return {
        "customerId": event["customerId"],
        "fullName": event["fullName"],
        "email": event.get("email"),
        "marketingOptIn": event.get("marketingOptIn", False),
        "schemaVersion": event["type"],
    }


def main() -> None:
    topic = topic_name("customer-events")
    group_id = os.getenv("GROUP_ID", "customer-readers")
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

            decoded = decode_message(message)
            normalized = normalize_customer_event(decoded["value"])
            print(
                {
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "normalizedEvent": normalized,
                }
            )
            commit_message(kafka_consumer, message)
    except KeyboardInterrupt:
        print("Stopping consumer.")
    finally:
        kafka_consumer.close()


if __name__ == "__main__":
    main()

