import { createProducer, encodeEvent, topicName } from "../../lib/kafka.js";

const topic = topicName("payments");
const producer = createProducer();

const payments = [
  {
    type: "PaymentAuthorized",
    paymentId: "payment-001",
    orderId: "order-001",
    amountCents: 2599,
    currency: "USD"
  },
  {
    type: "PaymentAuthorized",
    paymentId: "payment-002",
    orderId: "order-002",
    amountCents: -100,
    currency: "USD"
  },
  {
    type: "PaymentAuthorized",
    paymentId: "payment-003",
    orderId: "",
    amountCents: 4200,
    currency: "USD"
  },
  {
    type: "PaymentAuthorized",
    paymentId: "payment-004",
    orderId: "order-004",
    amountCents: 1800,
    currency: "USD"
  }
];

try {
  await producer.connect();

  await producer.send({
    topic,
    messages: payments.map((payment) => ({
      key: payment.orderId || payment.paymentId,
      value: encodeEvent({
        ...payment,
        createdAt: new Date().toISOString()
      })
    }))
  });

  console.log(`Sent ${payments.length} payment event(s) to ${topic}.`);
} finally {
  await producer.disconnect();
}
