from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab_kafka.client import delivery_report, encode_event, producer, topic_name


PAYMENTS = [
    {
        "type": "PaymentAuthorized",
        "paymentId": "payment-001",
        "orderId": "order-001",
        "amountCents": 2599,
        "currency": "USD",
    },
    {
        "type": "PaymentAuthorized",
        "paymentId": "payment-002",
        "orderId": "order-002",
        "amountCents": -100,
        "currency": "USD",
    },
    {
        "type": "PaymentAuthorized",
        "paymentId": "payment-003",
        "orderId": "",
        "amountCents": 4200,
        "currency": "USD",
    },
    {
        "type": "PaymentAuthorized",
        "paymentId": "payment-004",
        "orderId": "order-004",
        "amountCents": 1800,
        "currency": "USD",
    },
]


def main() -> None:
    topic = topic_name("payments")
    kafka_producer = producer()

    for payment in PAYMENTS:
        event = {
            **payment,
            "createdAt": datetime.now(timezone.utc).isoformat(),
        }
        kafka_producer.produce(
            topic,
            key=payment["orderId"] or payment["paymentId"],
            value=encode_event(event),
            callback=delivery_report,
        )

    kafka_producer.flush()
    print(f"Sent {len(PAYMENTS)} payment event(s) to {topic}.")


if __name__ == "__main__":
    main()
