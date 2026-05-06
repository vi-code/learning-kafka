# Milestone 6: Producer Guarantees

This milestone introduces settings that matter in real production systems:

- `acks=all`
- idempotent production
- batching
- compression

These settings help a producer be safer and more efficient when traffic grows.

## What You Are Learning

A beginner producer just sends messages.

An enterprise producer usually needs to answer:

- What happens if the broker is slow?
- Can retries create duplicates?
- How do we reduce network overhead?
- How do we trade latency for throughput?

## Run It

Terminal 1:

```bash
python labs/06-producer-guarantees/consumer.py
```

Terminal 2:

```bash
python labs/06-producer-guarantees/producer.py
```

## What To Notice

- The producer sends many events in one burst.
- The topic is partitioned.
- The consumer can still read the events in order per key.
- The producer code now looks more like a production producer.

## Reflection

Answer in your own words:

- Why is `acks=all` safer than weaker acknowledgement settings?
- Why does idempotence matter during retries?
- Why do teams batch messages instead of sending every event immediately?

