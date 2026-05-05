import { topicName } from "./kafka.js";

export const topics = [
  {
    topic: topicName("hello"),
    numPartitions: 1,
    replicationFactor: 1
  },
  {
    topic: topicName("orders"),
    numPartitions: 3,
    replicationFactor: 1
  },
  {
    topic: topicName("payments"),
    numPartitions: 3,
    replicationFactor: 1
  },
  {
    topic: topicName("payments.dead-letter"),
    numPartitions: 1,
    replicationFactor: 1
  }
];

