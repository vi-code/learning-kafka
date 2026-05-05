# Milestone 2: Consumer Groups

A consumer group lets multiple consumers share work.

Kafka assigns partitions to consumers in the same group. One partition is consumed by only one consumer in the group at a time.

## Run It

Terminal 1:

```bash
npm run lab:02:consumer
```

Terminal 2:

```bash
npm run lab:02:consumer
```

Terminal 3:

```bash
npm run lab:02:producer
```

Watch how the two consumers split messages.

## Try This

Start a third consumer with the same group:

```bash
npm run lab:02:consumer
```

Because `lab.orders` has three partitions, up to three consumers can actively share work.

Now start a fourth consumer. It will join the group, but one consumer will likely sit idle because there are only three partitions.

## Reflection

Answer in your own words:

- Why does partition count limit parallelism?
- What happens if a consumer crashes?
- Why might you use the same group id in production?

