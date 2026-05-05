import { createProducer, encodeEvent, topicName } from "../../lib/kafka.js";

const topic = topicName("orders");
const producer = createProducer();

const events = [
  { customerId: "customer-a", action: "cart-created" },
  { customerId: "customer-b", action: "cart-created" },
  { customerId: "customer-a", action: "item-added" },
  { customerId: "customer-c", action: "cart-created" },
  { customerId: "customer-a", action: "checkout-started" },
  { customerId: "customer-b", action: "item-added" },
  { customerId: "customer-a", action: "order-placed" }
];

try {
  await producer.connect();

  await producer.send({
    topic,
    messages: events.map((event, index) => ({
      key: event.customerId,
      value: encodeEvent({
        type: "CustomerActivityRecorded",
        sequence: index + 1,
        ...event,
        createdAt: new Date().toISOString()
      })
    }))
  });

  console.log(`Sent ${events.length} keyed event(s) to ${topic}.`);
} finally {
  await producer.disconnect();
}
