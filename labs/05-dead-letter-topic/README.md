# Milestone 5: Bad Messages and Dead-Letter Topics

Real event streams contain bad messages.

A common pattern is to move messages that cannot be processed to a dead-letter topic. That lets the consumer keep working while preserving failure details for later debugging.

In this milestone:

- The producer sends valid and invalid payment events.
- The consumer validates each event.
- Valid events are processed.
- Invalid events are sent to `lab.payments.dead-letter`.

## Run It

Terminal 1:

```bash
npm run lab:05:consumer
```

Terminal 2:

```bash
npm run lab:05:producer
```

Watch the consumer output. Some messages should be processed, and some should be forwarded to the dead-letter topic.

## Inspect The Dead-Letter Topic

Use the same consumer with a different topic by running:

```bash
TOPIC_SUFFIX=payments.dead-letter GROUP_ID=dead-letter-reader npm run lab:04:consumer
```

On Windows PowerShell:

```powershell
$env:TOPIC_SUFFIX="payments.dead-letter"; $env:GROUP_ID="dead-letter-reader"; npm run lab:04:consumer
```

## Reflection

Answer in your own words:

- Why not crash forever on one bad message?
- What information should a dead-letter event include?
- When should a team alert on dead-letter messages?

