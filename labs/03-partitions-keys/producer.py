from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab_kafka.client import delivery_report, encode_event, producer, topic_name


EVENTS = [
    {"customerId": "customer-a", "action": "cart-created"},
    {"customerId": "customer-b", "action": "cart-created"},
    {"customerId": "customer-a", "action": "item-added"},
    {"customerId": "customer-c", "action": "cart-created"},
    {"customerId": "customer-a", "action": "checkout-started"},
    {"customerId": "customer-b", "action": "item-added"},
    {"customerId": "customer-a", "action": "order-placed"},
    {"customerId": "customer-f", "action": "order-placed"},
    {"customerId": "customer-f", "action": "order-placed"},
    {"customerId": "jimbo", "action": "order-placed"},
    {"customerId": "jimbo", "action": "checkout-started"},
]


def main() -> None:
    topic = topic_name("orders")
    kafka_producer = producer()

    for index, event in enumerate(EVENTS, start=1):
        kafka_producer.produce(
            topic,
            key=event["customerId"],
            value=encode_event(
                {
                    "type": "CustomerActivityRecorded",
                    "sequence": index,
                    **event,
                    "createdAt": datetime.now(timezone.utc).isoformat(),
                }
            ),
            callback=delivery_report,
        )

    kafka_producer.flush()
    print(f"Sent {len(EVENTS)} keyed event(s) to {topic}.")


if __name__ == "__main__":
    main()
