import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab_kafka.client import encode_event, producer, topic_name


EVENTS = [
    {"jobId": "job-001", "status": "ready"},
    {"jobId": "job-002", "status": "transient-failure"},
    {"jobId": "job-003", "status": "ready"},
    {"jobId": "job-004", "status": "permanent-failure"},
]


def main() -> None:
    topic = topic_name("fulfillment")
    kafka_producer = producer()

    for event in EVENTS:
        payload = {
            "type": "FulfillmentRequested",
            "attempt": 0,
            **event,
            "createdAt": datetime.now(timezone.utc).isoformat(),
        }
        kafka_producer.produce(
            topic,
            key=payload["jobId"],
            value=encode_event(payload),
        )

    kafka_producer.flush()
    print(f"Seeded {len(EVENTS)} event(s) into {topic}.")


if __name__ == "__main__":
    main()

