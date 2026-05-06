# Milestone 8: Schema Evolution

At enterprise scale, events live longer than code versions.

That means producers and consumers evolve at different speeds, and the event contract has to survive that mismatch.

This milestone uses two event versions:

- `CustomerCreatedV1`
- `CustomerCreatedV2`

The consumer handles both.

## Run It

Terminal 1:

```bash
python labs/08-schema-evolution/consumer.py
```

Terminal 2:

```bash
python labs/08-schema-evolution/producer.py
```

## What You Are Learning

- Old and new event versions may coexist.
- Consumers should usually be tolerant readers.
- Schema evolution is a discipline, not just a code change.

## Reflection

- Why is adding a field usually safer than renaming one?
- Why should consumers avoid assuming every event looks exactly the same forever?
- What problems would schema registries help solve in a bigger system?

