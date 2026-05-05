# Milestone 1: Producer and Consumer

This milestone teaches the basic Kafka loop:

1. A producer sends events to a topic.
2. Kafka stores those events.
3. A consumer reads those events.

## Run It

Terminal 1:

```bash
npm run lab:01:consumer
```

Terminal 2:

```bash
npm run lab:01:producer
```

You should see the consumer print the messages sent by the producer.

## Look At The Code

Read these files:

- `labs/01-producer-consumer/producer.js`
- `labs/01-producer-consumer/consumer.js`

Notice:

- The producer sends to `lab.hello`.
- The consumer subscribes to `lab.hello`.
- The consumer keeps running because Kafka consumers are long-lived workers.

## Try This

Pass your own message:

```bash
node labs/01-producer-consumer/producer.js "Kafka is starting to make sense"
```

## Reflection

Answer in your own words:

- What does the producer know about the consumer?
- What does the consumer know about the producer?
- Why is it useful that they do not call each other directly?

