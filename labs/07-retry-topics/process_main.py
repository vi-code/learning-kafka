import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab_kafka.client import commit_message, consumer, decode_message, encode_event, producer, topic_name


def main() -> None:
    source_topic = topic_name("fulfillment")
    retry_topic = topic_name("fulfillment.retry")
    parking_topic = topic_name("fulfillment.parking")
    group_id = os.getenv("GROUP_ID", "fulfillment-workers")

    kafka_consumer = consumer(group_id)
    kafka_producer = producer()
    kafka_consumer.subscribe([source_topic])

    print(f'Processing {source_topic} as group "{group_id}". Press Ctrl+C to stop.')

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
            attempt = event.get("attempt", 0)

            if event["status"] == "ready":
                print(
                    {
                        "status": "processed",
                        "jobId": event["jobId"],
                        "attempt": attempt,
                    }
                )
                commit_message(kafka_consumer, message)
                continue

            if event["status"] == "transient-failure" and attempt < 2:
                retry_event = {
                    **event,
                    "attempt": attempt + 1,
                    "lastFailureAt": datetime.now(timezone.utc).isoformat(),
                }
                kafka_producer.produce(
                    retry_topic,
                    key=event["jobId"],
                    value=encode_event(retry_event),
                )
                kafka_producer.flush()
                print(
                    {
                        "status": "sent-to-retry",
                        "jobId": event["jobId"],
                        "nextAttempt": attempt + 1,
                    }
                )
                commit_message(kafka_consumer, message)
                continue

            parking_event = {
                **event,
                "parkedAt": datetime.now(timezone.utc).isoformat(),
                "reason": "permanent failure or retry limit reached",
            }
            kafka_producer.produce(
                parking_topic,
                key=event["jobId"],
                value=encode_event(parking_event),
            )
            kafka_producer.flush()
            print(
                {
                    "status": "sent-to-parking",
                    "jobId": event["jobId"],
                    "attempt": attempt,
                }
            )
            commit_message(kafka_consumer, message)
    except KeyboardInterrupt:
        print("Stopping processor.")
    finally:
        kafka_consumer.close()


if __name__ == "__main__":
    main()

