import { Kafka, Partitioners, logLevel } from "kafkajs";

const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9092")
  .split(",")
  .map((broker) => broker.trim())
  .filter(Boolean);

export const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID ?? "learning-kafka-lab",
  brokers,
  logLevel: logLevel.ERROR,
  retry: {
    initialRetryTime: 300,
    retries: 8
  }
});

export const topicPrefix = process.env.TOPIC_PREFIX ?? "lab";

export function topicName(slug) {
  return `${topicPrefix}.${slug}`;
}

export function createProducer() {
  return kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner
  });
}

export function decodeMessage(message) {
  return {
    key: message.key?.toString() ?? null,
    value: message.value ? JSON.parse(message.value.toString()) : null,
    headers: Object.fromEntries(
      Object.entries(message.headers ?? {}).map(([key, value]) => [
        key,
        value?.toString()
      ])
    )
  };
}

export function encodeEvent(event) {
  return JSON.stringify(event);
}
