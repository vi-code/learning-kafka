import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab_kafka.client import delivery_report, encode_event, producer, topic_name


def build_order(index: int) -> dict:
    customer_id = f"customer-{(index % 4) + 1}"

    return {
        "type": "OrderPlaced",
        "orderId": f"order-{int(time.time())}-{index}",
        "customerId": customer_id,
        "totalCents": 1500 + index * 125,
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    topic = topic_name("orders")
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 12
    kafka_producer = producer()

    for index in range(1, count + 1):
        order = build_order(index)
        kafka_producer.produce(
            topic,
            key=order["customerId"],
            value=encode_event(order),
            callback=delivery_report,
        )

    kafka_producer.flush()
    print(f"Sent {count} order event(s) to {topic}.")


if __name__ == "__main__":
    main()
