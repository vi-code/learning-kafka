from confluent_kafka import KafkaException

from lab_kafka.client import admin_client
from lab_kafka.topics import TOPICS


def main() -> None:
    admin = admin_client()
    existing_topics = set(admin.list_topics(timeout=10).topics)
    missing_topics = [topic for topic in TOPICS if topic.topic not in existing_topics]

    if not missing_topics:
        print("All lab topics already exist.")
        return

    futures = admin.create_topics(missing_topics)

    print("Creating topics:")
    for topic_name, future in futures.items():
        try:
            future.result()
            print(f"- {topic_name}")
        except KafkaException as error:
            print(f"- {topic_name}: {error}")


if __name__ == "__main__":
    main()

