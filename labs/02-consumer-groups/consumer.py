import os
import time

from lab_kafka.client import consumer, decode_message, topic_name


def main() -> None:
    topic = topic_name("orders")
    group_id = os.getenv("GROUP_ID", "order-workers")
    worker_name = os.getenv("WORKER_NAME", f"worker-{os.getpid()}")
    kafka_consumer = consumer(group_id)
    kafka_consumer.subscribe([topic])

    print(f'{worker_name} consuming {topic} as group "{group_id}".')

    try:
        while True:
            message = kafka_consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(message.error())
                continue

            event = decode_message(message)
            time.sleep(0.5)

            print(
                {
                    "workerName": worker_name,
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "orderId": event["value"]["orderId"],
                    "customerId": event["value"]["customerId"],
                }
            )
            kafka_consumer.commit(message=message, asynchronous=False)
    except KeyboardInterrupt:
        print("Stopping consumer.")
    finally:
        kafka_consumer.close()


if __name__ == "__main__":
    main()
