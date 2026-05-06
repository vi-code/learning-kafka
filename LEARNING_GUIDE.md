# Kafka Learning Guide

This guide is the bridge between "I see Python files" and "I understand what Kafka is doing."

## Start Here

Do not begin by reading all the code.

Begin in this order:

1. Read this file.
2. Read `README.md`.
3. Run one milestone at a time.
4. After each milestone, write down what you think happened before you move on.

The goal is not to memorize Kafka vocabulary. The goal is to build a mental model.

## The Big Picture

Before Kafka, understand message queues in plain English:

- One part of a system wants to say that something happened.
- Another part of the system wants to react to that event.
- A queue or event log sits between them.
- That separation makes systems less tightly coupled.

Simple example:

- An order service says: "Order placed."
- A payment service reacts.
- An email service reacts.
- An analytics service reacts.

Without a queue, every service calls every other service directly.

With a queue or event broker:

- the producer publishes an event
- the broker stores it
- consumers read it

That is the core idea you are learning in this repo.

## Message Queues Vs Kafka

Think of a basic queue like this:

- a message is put into a line
- one worker takes it
- after it is consumed, it is usually gone

Think of Kafka like this:

- events are written to a log
- consumers track where they are in that log
- multiple consumers can read the same history
- replay is normal

That replay ability is one of Kafka's superpowers.

## What To Learn First

Use this exact progression.

### Phase 1: Why Messaging Exists

Goal:
Understand why systems use queues or event brokers at all.

Questions to answer:

- Why not just make one service call another directly?
- What happens if the downstream service is slow or down?
- Why is asynchronous communication useful?

You do not need Kafka yet for this phase. Just understand the problem.

### Phase 2: The Smallest Kafka Loop

Use:

- `labs/01-producer-consumer`

Goal:
Understand producer -> broker -> consumer.

What to notice:

- The producer does not know who reads the event.
- The consumer does not know who wrote it.
- Kafka is the middle layer.

If you only learn one thing today, learn this.

### Phase 3: Consumer Groups

Use:

- `labs/02-consumer-groups`

Goal:
Understand how Kafka scales consumers.

What to notice:

- Multiple consumers can share work.
- Kafka assigns partitions to consumers.
- Parallelism is limited by partition count.

This is the first point where Kafka stops looking like a simple queue and starts looking like a distributed system tool.

### Phase 4: Keys And Partitions

Use:

- `labs/03-partitions-keys`

Goal:
Understand why related events use the same key.

What to notice:

- Same key usually means same partition.
- Same partition gives per-key ordering.
- Ordering in distributed systems is local, not magical.

This is a big concept. Stay here until it clicks.

### Phase 5: Offsets And Replay

Use:

- `labs/04-offsets-replay`

Goal:
Understand that Kafka stores event history and consumers store reading position.

What to notice:

- Kafka keeps messages.
- Consumer groups keep track of progress.
- A new group can replay old events.

This is the concept that most clearly separates Kafka from a normal work queue.

### Phase 6: Bad Messages

Use:

- `labs/05-dead-letter-topic`

Goal:
Understand failure handling in event-driven systems.

What to notice:

- Some messages are invalid.
- Consumers need a strategy.
- Dead-letter topics preserve bad events without stopping all progress.

## How To Use Each Lab

For every lab, follow this ritual:

1. Read the milestone README only.
2. Predict what will happen.
3. Run the consumer first.
4. Run the producer second.
5. Watch the output.
6. Open the Python code and connect it to what you saw.
7. Change one thing and rerun it.
8. Write a 3-line note in your own words.

Do not skip step 6 or step 8. That is where learning actually happens.

## What To Write Down

Use this template after every milestone:

```md
# Milestone X Notes

## What problem is this solving?

## What happened when I ran it?

## What confused me?

## What is the most important Kafka concept here?

## One thing I changed and what I observed
```

## Beginner Mental Model

Keep these mappings in your head:

- Producer: code that writes an event
- Broker: Kafka server
- Topic: named event stream
- Partition: a shard of a topic
- Consumer: code that reads events
- Consumer group: a team of consumers sharing work
- Offset: a position in a partition
- Key: value Kafka uses to choose partitioning

If a term feels abstract, ask: "what job does this thing do?"

## A Good 5-Day Plan

### Day 1

- Read `LEARNING_GUIDE.md`
- Read `README.md`
- Run Milestone 1
- Write notes

### Day 2

- Run Milestone 2
- Draw a picture of one topic with three partitions and two consumers
- Write notes

### Day 3

- Run Milestone 3
- Change keys and observe partition behavior
- Write notes

### Day 4

- Run Milestone 4
- Change group ids and replay history
- Write notes

### Day 5

- Run Milestone 5
- Look at the dead-letter topic
- Write notes
- Summarize what makes Kafka different from a basic queue

## The Main Thing To Internalize

Kafka is not just "a place where messages wait."

Kafka is a durable event log where:

- producers publish facts
- consumers read at their own pace
- groups coordinate work
- offsets make replay possible

That sentence is the center of the whole lab.

## If You Feel Lost

That is normal.

When you get stuck, narrow the question:

- What event was produced?
- Which topic received it?
- Which consumer group read it?
- Which partition handled it?
- What offset was committed?

Most Kafka confusion becomes manageable when you ask those five questions.

## Enterprise Track

Once the first five milestones feel comfortable, you are ready for the next layer.

These are the concepts teams care about in production:

- producer guarantees
- retries and parking lots
- schema evolution
- lag and operational visibility

### Milestone 6: Producer Guarantees

Use:

- `labs/06-producer-guarantees`

Learn:

- why `acks=all` exists
- why idempotence matters
- why batching and compression are normal in high-volume systems

Enterprise question:
"How do we publish safely when traffic and retry pressure go up?"

### Milestone 7: Retry Topics And Parking Lots

Use:

- `labs/07-retry-topics`

Learn:

- retries are a workflow, not just an exception handler
- transient and permanent failures need different handling
- infinite retries can become their own outage

Enterprise question:
"What should happen when downstream dependencies fail for 30 seconds, 30 minutes, or forever?"

### Milestone 8: Schema Evolution

Use:

- `labs/08-schema-evolution`

Learn:

- producers and consumers upgrade on different schedules
- event contracts should evolve carefully
- tolerant readers help reduce breakage

Enterprise question:
"How do we change event shape without breaking everybody downstream?"

### Milestone 9: Consumer Lag

Use:

- `labs/09-consumer-lag`

Learn:

- lag is one of the clearest production health signals
- partition-level lag tells a better story than one total number
- Kafka operations are as important as Kafka coding

Enterprise question:
"How do we know whether consumers are keeping up before users notice trouble?"

## A Good 10-Day Plan

### Days 1-5

- Complete milestones 1 through 5.
- Focus on understanding the basic Kafka mental model.

### Day 6

- Run milestone 6.
- Compare a basic producer mindset with a production producer mindset.

### Day 7

- Run milestone 7.
- Draw the flow: main topic -> retry topic -> parking lot.

### Day 8

- Run milestone 8.
- Write down what makes a schema change safe or unsafe.

### Day 9

- Run milestone 9.
- Observe how lag changes before and after the consumer catches up.

### Day 10

- Summarize the difference between beginner Kafka usage and enterprise Kafka usage.

Try to explain:

- reliability
- operability
- compatibility
- failure handling

Those four themes are where enterprise Kafka thinking really begins.
