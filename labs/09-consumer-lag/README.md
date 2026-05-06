# Milestone 9: Consumer Lag

Enterprise Kafka operations care about one question all the time:

"Are consumers keeping up?"

Consumer lag is the gap between:

- the latest offset in Kafka
- the offset a consumer group has committed

## Run It

Step 1:

```bash
python labs/06-producer-guarantees/producer.py 40
```

Step 2:

```bash
python labs/09-consumer-lag/report.py
```

Step 3:

```bash
python labs/06-producer-guarantees/consumer.py
```

Step 4:

```bash
python labs/09-consumer-lag/report.py
```

## What You Are Learning

- Lag is an operational signal.
- A healthy group is usually moving its committed offsets forward.
- Lag by partition helps teams debug uneven load and slow consumers.

## Reflection

- What would make lag grow?
- Why does partition-level lag matter more than one total number?
- Why is lag monitoring important before incidents become outages?

