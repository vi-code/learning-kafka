import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab_kafka.client import commit_message, consumer, decode_message, encode_event, producer, topic_name


def main() -> None:
    retry_topic = topic_name("fulfillment.retry")
    source_topic = topic_name("fulfillment")
    kafka_consumer = consumer("fulfillment-retry-replayer")
    kafka_producer = producer()
    kafka_consumer.subscribe([retry_topic])

    print(f"Replaying messages from {retry_topic} back into {source_topic}.")

    try:
        while True:
            message = kafka_consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(message.error())
                continue

            decoded = decode_message(message)
            kafka_producer.produce(
                source_topic,
                key=decoded["key"],
                value=encode_event(decoded["value"]),
            )
            kafka_producer.flush()
            print(
                {
                    "status": "replayed",
                    "jobId": decoded["value"]["jobId"],
                    "attempt": decoded["value"]["attempt"],
                }
            )
            commit_message(kafka_consumer, message)
    except KeyboardInterrupt:
        print("Stopping retry replayer.")
    finally:
        kafka_consumer.close()


if __name__ == "__main__":
    main()

