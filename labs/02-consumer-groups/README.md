# Milestone 2: Consumer Groups

A consumer group lets multiple consumers share work.

Kafka assigns partitions to consumers in the same group. One partition is consumed by only one consumer in the group at a time.

## Run It

Terminal 1:

```bash
python labs/02-consumer-groups/consumer.py
```

Terminal 2:

```bash
python labs/02-consumer-groups/consumer.py
```

Terminal 3:

```bash
python labs/02-consumer-groups/producer.py
```

Watch how the two consumers split messages.

## Try This

Start a third consumer with the same group:

```bash
python labs/02-consumer-groups/consumer.py
```

Because `lab.orders` has three partitions, up to three consumers can actively share work.

Now start a fourth consumer. It will join the group, but one consumer will likely sit idle because there are only three partitions.

## Reflection

Answer in your own words:

- Why does partition count limit parallelism?
- What happens if a consumer crashes?
- Why might you use the same group id in production?
