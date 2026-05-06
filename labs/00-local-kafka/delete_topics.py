import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab_kafka.client import admin_client, topic_prefix


def main() -> None:
    admin = admin_client()
    prefix = f"{topic_prefix()}."
    topics = sorted(
        topic for topic in admin.list_topics(timeout=10).topics if topic.startswith(prefix)
    )

    if not topics:
        print(f'No topics found with prefix "{prefix}"')
        return

    futures = admin.delete_topics(topics)

    print("Deleted lab topics:")
    for topic_name, future in futures.items():
        future.result()
        print(f"- {topic_name}")


if __name__ == "__main__":
    main()
