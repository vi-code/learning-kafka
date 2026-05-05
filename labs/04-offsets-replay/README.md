# Milestone 4: Offsets and Replay

Kafka does not delete a message just because one consumer read it.

Instead, each consumer group tracks offsets. An offset is a position inside a partition.

That means:

- The same group id continues where it left off.
- A new group id can read old events from the beginning.
- Kafka can be used for replaying history.

## Run It

First, produce some order events:

```bash
python labs/02-consumer-groups/producer.py
```

Now consume them with the default replay group:

```bash
python labs/04-offsets-replay/consumer.py
```

Stop the consumer with Ctrl+C, then run it again. It should not print the same old messages, because the group already committed offsets.

Now change the group id:

```bash
GROUP_ID=replay-demo-v2 python labs/04-offsets-replay/consumer.py
```

On Windows PowerShell:

```powershell
$env:GROUP_ID="replay-demo-v2"; python labs/04-offsets-replay/consumer.py
```

The new group can read from the beginning.

## Reflection

Answer in your own words:

- What does Kafka store?
- What does the consumer group store?
- Why is replay useful for debugging or rebuilding a service?
