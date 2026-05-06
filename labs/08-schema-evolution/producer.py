import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab_kafka.client import encode_event, producer, topic_name


def main() -> None:
    topic = topic_name("customer-events")
    kafka_producer = producer()

    v1_event = {
        "type": "CustomerCreatedV1",
        "customerId": "customer-100",
        "fullName": "Ada Lovelace",
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    v2_event = {
        "type": "CustomerCreatedV2",
        "customerId": "customer-101",
        "fullName": "Grace Hopper",
        "email": "grace@example.com",
        "marketingOptIn": True,
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }

    for event in [v1_event, v2_event]:
        kafka_producer.produce(
            topic,
            key=event["customerId"],
            value=encode_event(event),
        )

    kafka_producer.flush()
    print(f"Published 2 schema versions into {topic}.")


if __name__ == "__main__":
    main()

