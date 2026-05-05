# Milestone 3: Partitions, Keys, and Ordering

Kafka stores messages inside partitions.

When you send a message with a key, KafkaJS hashes that key and picks a partition. Messages with the same key go to the same partition, which preserves order for that key.

This matters for entities like:

- One customer.
- One bank account.
- One order.
- One device.

## Run It

Terminal 1:

```bash
npm run lab:03:consumer
```

Terminal 2:

```bash
npm run lab:03:producer
```

Watch the consumer output. The same `customerId` should consistently appear on the same partition.

## Try This

Run the producer several times:

```bash
npm run lab:03:producer
```

Then open `producer.js` and change the customer ids. Observe how partition assignment changes.

## Reflection

Answer in your own words:

- Why should events for the same customer usually use the same key?
- What could go wrong if related events landed on random partitions?

