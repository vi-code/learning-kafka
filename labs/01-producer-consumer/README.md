# Milestone 1: Producer and Consumer

This milestone teaches the basic Kafka loop:

1. A producer sends events to a topic.
2. Kafka stores those events.
3. A consumer reads those events.

## Run It

Terminal 1:

```bash
python labs/01-producer-consumer/consumer.py
```

Terminal 2:

```bash
python labs/01-producer-consumer/producer.py
```

You should see the consumer print the messages sent by the producer.

## Look At The Code

Read these files:

- `labs/01-producer-consumer/producer.py`
- `labs/01-producer-consumer/consumer.py`

Notice:

- The producer sends to `lab.hello`.
- The consumer subscribes to `lab.hello`.
- The consumer keeps running because Kafka consumers are long-lived workers.

## Try This

Pass your own message:

```bash
python labs/01-producer-consumer/producer.py "Kafka is starting to make sense"
```

## Reflection

Answer in your own words:

- What does the producer know about the consumer?
- What does the consumer know about the producer?
- Why is it useful that they do not call each other directly?
