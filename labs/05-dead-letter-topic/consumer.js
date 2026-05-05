import {
  createProducer,
  decodeMessage,
  encodeEvent,
  kafka,
  topicName
} from "../../lib/kafka.js";

const sourceTopic = topicName("payments");
const deadLetterTopic = topicName("payments.dead-letter");
const groupId = process.env.GROUP_ID ?? "payment-workers";
const consumer = kafka.consumer({ groupId });
const producer = createProducer();

function validatePayment(event) {
  if (event.type !== "PaymentAuthorized") {
    return "Unsupported event type";
  }

  if (!event.paymentId) {
    return "paymentId is required";
  }

  if (!event.orderId) {
    return "orderId is required";
  }

  if (!Number.isInteger(event.amountCents) || event.amountCents <= 0) {
    return "amountCents must be a positive integer";
  }

  if (event.currency !== "USD") {
    return "Only USD payments are supported in this lab";
  }

  return null;
}

process.on("SIGINT", async () => {
  await consumer.disconnect();
  await producer.disconnect();
  process.exit(0);
});

await producer.connect();
await consumer.connect();
await consumer.subscribe({ topic: sourceTopic, fromBeginning: true });

console.log(`Processing ${sourceTopic} as group "${groupId}".`);

await consumer.run({
  eachMessage: async ({ partition, message }) => {
    const decoded = decodeMessage(message);
    const reason = validatePayment(decoded.value);

    if (reason) {
      await producer.send({
        topic: deadLetterTopic,
        messages: [
          {
            key: decoded.key ?? decoded.value?.paymentId ?? "unknown",
            value: encodeEvent({
              type: "PaymentRejectedToDeadLetter",
              reason,
              sourceTopic,
              sourcePartition: partition,
              sourceOffset: message.offset,
              originalEvent: decoded.value,
              rejectedAt: new Date().toISOString()
            })
          }
        ]
      });

      console.log({
        status: "sent-to-dead-letter",
        reason,
        paymentId: decoded.value?.paymentId,
        deadLetterTopic
      });
      return;
    }

    console.log({
      status: "processed",
      paymentId: decoded.value.paymentId,
      orderId: decoded.value.orderId,
      amountCents: decoded.value.amountCents
    });
  }
});
