import { decodeMessage, kafka, topicName } from "../../lib/kafka.js";

const topic = topicName("orders");
const groupId = process.env.GROUP_ID ?? "order-workers";
const workerName = process.env.WORKER_NAME ?? `worker-${process.pid}`;
const consumer = kafka.consumer({ groupId });

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

process.on("SIGINT", async () => {
  await consumer.disconnect();
  process.exit(0);
});

await consumer.connect();
await consumer.subscribe({ topic, fromBeginning: true });

console.log(`${workerName} consuming ${topic} as group "${groupId}".`);

await consumer.run({
  eachMessage: async ({ partition, message }) => {
    const event = decodeMessage(message);
    await sleep(500);

    console.log({
      workerName,
      partition,
      offset: message.offset,
      orderId: event.value.orderId,
      customerId: event.value.customerId
    });
  }
});

