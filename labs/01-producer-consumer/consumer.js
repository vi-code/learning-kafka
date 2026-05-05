import { decodeMessage, kafka, topicName } from "../../lib/kafka.js";

const topic = topicName("hello");
const groupId = process.env.GROUP_ID ?? "hello-readers";
const consumer = kafka.consumer({ groupId });

process.on("SIGINT", async () => {
  await consumer.disconnect();
  process.exit(0);
});

await consumer.connect();
await consumer.subscribe({ topic, fromBeginning: true });

console.log(`Consuming ${topic} as group "${groupId}". Press Ctrl+C to stop.`);

await consumer.run({
  eachMessage: async ({ partition, message }) => {
    const event = decodeMessage(message);
    console.log({
      partition,
      offset: message.offset,
      key: event.key,
      value: event.value
    });
  }
});

