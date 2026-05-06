from confluent_kafka.admin import NewTopic

from lab_kafka.client import topic_name


TOPICS = [
    NewTopic(topic_name("hello"), num_partitions=1, replication_factor=1),
    NewTopic(topic_name("orders"), num_partitions=4, replication_factor=1),
    NewTopic(topic_name("payments"), num_partitions=3, replication_factor=1),
    NewTopic(topic_name("payments.dead-letter"), num_partitions=1, replication_factor=1),
    NewTopic(topic_name("fulfillment"), num_partitions=3, replication_factor=1),
    NewTopic(topic_name("fulfillment.retry"), num_partitions=3, replication_factor=1),
    NewTopic(topic_name("fulfillment.parking"), num_partitions=1, replication_factor=1),
    NewTopic(topic_name("customer-events"), num_partitions=3, replication_factor=1),
    NewTopic(topic_name("audit"), num_partitions=3, replication_factor=1),
]
