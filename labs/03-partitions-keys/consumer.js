import { decodeMessage, kafka, topicName } from "../../lib/kafka.js";

const topic = topicName("orders");
const groupId = process.env.GROUP_ID ?? "partition-watchers";
const consumer = kafka.consumer({ groupId });

process.on("SIGINT", async () => {
  await consumer.disconnect();
  process.exit(0);
});

await consumer.connect();
await consumer.subscribe({ topic, fromBeginning: true });

console.log(`Watching partitions for ${topic}. Press Ctrl+C to stop.`);

await consumer.run({
  eachMessage: async ({ partition, message }) => {
    const event = decodeMessage(message);

    console.log({
      partition,
      offset: message.offset,
      key: event.key,
      sequence: event.value.sequence,
      customerId: event.value.customerId,
      action: event.value.action
    });
  }
});

