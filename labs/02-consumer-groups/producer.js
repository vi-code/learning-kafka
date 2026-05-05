import { createProducer, encodeEvent, topicName } from "../../lib/kafka.js";

const topic = topicName("orders");
const producer = createProducer();
const count = Number.parseInt(process.argv[2] ?? "12", 10);

function buildOrder(index) {
  const customerId = `customer-${(index % 4) + 1}`;

  return {
    type: "OrderPlaced",
    orderId: `order-${Date.now()}-${index}`,
    customerId,
    totalCents: 1500 + index * 125,
    createdAt: new Date().toISOString()
  };
}

try {
  await producer.connect();

  const messages = Array.from({ length: count }, (_, index) => {
    const order = buildOrder(index + 1);

    return {
      key: order.customerId,
      value: encodeEvent(order)
    };
  });

  await producer.send({ topic, messages });

  console.log(`Sent ${messages.length} order event(s) to ${topic}.`);
} finally {
  await producer.disconnect();
}
