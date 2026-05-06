import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab_kafka.client import consumer, decode_message, encode_event, producer, topic_name


def validate_payment(event: dict) -> str | None:
    if event.get("type") != "PaymentAuthorized":
        return "Unsupported event type"

    if not event.get("paymentId"):
        return "paymentId is required"

    if not event.get("orderId"):
        return "orderId is required"

    if not isinstance(event.get("amountCents"), int) or event["amountCents"] <= 0:
        return "amountCents must be a positive integer"

    if event.get("currency") != "USD":
        return "Only USD payments are supported in this lab"

    return None


def main() -> None:
    source_topic = topic_name("payments")
    dead_letter_topic = topic_name("payments.dead-letter")
    group_id = os.getenv("GROUP_ID", "payment-workers")
    kafka_consumer = consumer(group_id)
    kafka_producer = producer()
    kafka_consumer.subscribe([source_topic])

    print(f'Processing {source_topic} as group "{group_id}".')

    try:
        while True:
            message = kafka_consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(message.error())
                continue

            decoded = decode_message(message)
            event = decoded["value"]
            reason = validate_payment(event)

            if reason:
                dead_letter_event = {
                    "type": "PaymentRejectedToDeadLetter",
                    "reason": reason,
                    "sourceTopic": source_topic,
                    "sourcePartition": message.partition(),
                    "sourceOffset": message.offset(),
                    "originalEvent": event,
                    "rejectedAt": datetime.now(timezone.utc).isoformat(),
                }

                kafka_producer.produce(
                    dead_letter_topic,
                    key=decoded["key"] or event.get("paymentId") or "unknown",
                    value=encode_event(dead_letter_event),
                )
                kafka_producer.flush()

                print(
                    {
                        "status": "sent-to-dead-letter",
                        "reason": reason,
                        "paymentId": event.get("paymentId"),
                        "deadLetterTopic": dead_letter_topic,
                    }
                )
                kafka_consumer.commit(message=message, asynchronous=False)
                continue

            print(
                {
                    "status": "processed",
                    "paymentId": event["paymentId"],
                    "orderId": event["orderId"],
                    "amountCents": event["amountCents"],
                }
            )
            kafka_consumer.commit(message=message, asynchronous=False)
    except KeyboardInterrupt:
        print("Stopping consumer.")
    finally:
        kafka_producer.flush()
        kafka_consumer.close()


if __name__ == "__main__":
    main()
