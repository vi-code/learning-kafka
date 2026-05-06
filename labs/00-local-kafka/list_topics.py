import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab_kafka.client import admin_client, topic_prefix


def main() -> None:
    admin = admin_client()
    metadata = admin.list_topics(timeout=10)
    prefix = f"{topic_prefix()}."

    topics = sorted(
        (topic for topic in metadata.topics.values() if topic.topic.startswith(prefix)),
        key=lambda topic: topic.topic,
    )

    if not topics:
        print(f'No topics found with prefix "{prefix}"')
        return

    print("Lab topics:")
    for topic in topics:
        print(f"- {topic.topic} ({len(topic.partitions)} partition(s))")


if __name__ == "__main__":
    main()
