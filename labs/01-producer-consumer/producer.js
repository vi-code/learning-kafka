import { createProducer, encodeEvent, topicName } from "../../lib/kafka.js";

const topic = topicName("hello");
const producer = createProducer();
const text = process.argv.slice(2).join(" ") || "Hello from Kafka";

try {
  await producer.connect();

  const event = {
    type: "GreetingCreated",
    text,
    createdAt: new Date().toISOString()
  };

  await producer.send({
    topic,
    messages: [
      {
        key: "greeting",
        value: encodeEvent(event)
      }
    ]
  });

  console.log(`Sent event to ${topic}:`);
  console.log(event);
} finally {
  await producer.disconnect();
}
