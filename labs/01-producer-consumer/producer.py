import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab_kafka.client import delivery_report, encode_event, producer, topic_name


def main() -> None:
    topic = topic_name("hello")
    text = " ".join(sys.argv[1:]) or "Hello from Kafka"
    kafka_producer = producer()

    event = {
        "type": "GreetingCreated",
        "text": text,
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }

    kafka_producer.produce(
        topic,
        key="greeting",
        value=encode_event(event),
        callback=delivery_report,
    )
    kafka_producer.flush()

    print(f"Sent event to {topic}:")
    print(event)


if __name__ == "__main__":
    main()
