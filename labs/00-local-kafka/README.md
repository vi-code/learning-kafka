# Milestone 0: Local Kafka

Kafka is a server that stores streams of events. Your code connects to Kafka as a client.

In this milestone you will:

- Start Kafka locally.
- Create topics.
- List topics.
- Learn the basic vocabulary.

## Vocabulary

Broker: a Kafka server.

Topic: a named stream of events, like `lab.orders`.

Partition: a topic shard. Kafka stores messages inside partitions.

Producer: code that writes events to a topic.

Consumer: code that reads events from a topic.

Consumer group: one or more consumers that share work.

Offset: the position of a message inside a partition.

## Run It

From the repo root:

```bash
npm install
npm run kafka:up
npm run topics:create
npm run topics:list
```

Expected topic names:

- `lab.hello`
- `lab.orders`
- `lab.payments`
- `lab.payments.dead-letter`

## Try This

Open `lib/topics.js`, change the `lab.orders` partition count from `3` to `4`, then run:

```bash
npm run topics:delete
npm run topics:create
npm run topics:list
```

That is your first infrastructure change.

## Reflection

Write a short note in your own words:

- What is a topic?
- Why might a topic need more than one partition?

