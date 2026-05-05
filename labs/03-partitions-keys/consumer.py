import os

from lab_kafka.client import consumer, decode_message, topic_name


def main() -> None:
    topic = topic_name("orders")
    group_id = os.getenv("GROUP_ID", "partition-watchers")
    kafka_consumer = consumer(group_id)
    kafka_consumer.subscribe([topic])

    print(f"Watching partitions for {topic}. Press Ctrl+C to stop.")

    try:
        while True:
            message = kafka_consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(message.error())
                continue

            event = decode_message(message)
            value = event["value"]

            print(
                {
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "key": event["key"],
                    "sequence": value["sequence"],
                    "customerId": value["customerId"],
                    "action": value["action"],
                }
            )
            kafka_consumer.commit(message=message, asynchronous=False)
    except KeyboardInterrupt:
        print("Stopping consumer.")
    finally:
        kafka_consumer.close()


if __name__ == "__main__":
    main()
