# Milestone 7: Retry Topics And Parking Lots

Enterprise Kafka systems usually treat failures in layers:

- success
- transient failure that should be retried
- permanent failure that should be parked for investigation

This milestone shows a common pattern:

- main topic
- retry topic
- parking lot topic

## Run It

Step 1:

```bash
python labs/07-retry-topics/seed_events.py
```

Step 2:

```bash
python labs/07-retry-topics/process_main.py
```

Step 3:

```bash
python labs/07-retry-topics/replay_retry.py
```

Step 4:

```bash
python labs/07-retry-topics/process_main.py
```

## What You Are Learning

- Kafka does not give you delayed retries for free.
- Teams often model retries explicitly with topics.
- Some failures should be retried.
- Some failures should stop retrying and be parked.

## Reflection

- What kinds of failures are retryable?
- Why is a parking lot topic safer than infinite retries?
- What business data would you keep with a failed event?

