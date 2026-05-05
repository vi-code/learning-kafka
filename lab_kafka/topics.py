from confluent_kafka.admin import NewTopic

from lab_kafka.client import topic_name


TOPICS = [
    NewTopic(topic_name("hello"), num_partitions=1, replication_factor=1),
    NewTopic(topic_name("orders"), num_partitions=3, replication_factor=1),
    NewTopic(topic_name("payments"), num_partitions=3, replication_factor=1),
    NewTopic(topic_name("payments.dead-letter"), num_partitions=1, replication_factor=1),
]

