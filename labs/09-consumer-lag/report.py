import sys
from pathlib import Path

from confluent_kafka import TopicPartition

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab_kafka.client import admin_client, consumer, topic_name


def main() -> None:
    topic = topic_name("audit")
    group_id = "audit-readers"
    metadata = admin_client().list_topics(topic=topic, timeout=10)
    topic_metadata = metadata.topics[topic]
    partitions = sorted(topic_metadata.partitions)
    topic_partitions = [TopicPartition(topic, partition) for partition in partitions]

    kafka_consumer = consumer(group_id)
    committed = kafka_consumer.committed(topic_partitions, timeout=10)

    print({"topic": topic, "groupId": group_id})

    total_lag = 0
    for partition_state in committed:
        low_offset, high_offset = kafka_consumer.get_watermark_offsets(
            TopicPartition(topic, partition_state.partition),
            timeout=10,
            cached=False,
        )
        committed_offset = partition_state.offset
        if committed_offset < 0:
            committed_offset = low_offset
        lag = max(high_offset - committed_offset, 0)
        total_lag += lag

        print(
            {
                "partition": partition_state.partition,
                "earliestOffset": low_offset,
                "latestOffset": high_offset,
                "committedOffset": committed_offset,
                "lag": lag,
            }
        )

    print({"totalLag": total_lag})
    kafka_consumer.close()


if __name__ == "__main__":
    main()

