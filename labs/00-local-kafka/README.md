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
python -m pip install -r requirements.txt
docker compose up -d
python labs/00-local-kafka/create_topics.py
python labs/00-local-kafka/list_topics.py
```

Expected topic names:

- `lab.hello`
- `lab.orders`
- `lab.payments`
- `lab.payments.dead-letter`

## Try This

Open `lab_kafka/topics.py`, change the `lab.orders` partition count from `3` to `4`, then run:

```bash
python labs/00-local-kafka/delete_topics.py
python labs/00-local-kafka/create_topics.py
python labs/00-local-kafka/list_topics.py
```

That is your first infrastructure change.

## Reflection

Write a short note in your own words:

- What is a topic?
- Why might a topic need more than one partition?
