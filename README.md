# Learning Kafka From Scratch

A hands-on Kafka lab for absolute beginners. The goal is to learn Kafka by running small examples, changing code, and observing what happens.

This repo is intentionally simple:

- Kafka runs locally in Docker.
- Examples are plain JavaScript using Node.js and KafkaJS.
- Each milestone focuses on one Kafka concept.
- The code is small enough to read before you run it.

Future AI chats should read `AI_CONTEXT.md` first. It captures the project purpose, conventions, and next milestone ideas.

## What You Will Learn

By the end of the implemented milestones, you will understand:

- What a Kafka broker is.
- What topics and partitions are.
- How producers publish events.
- How consumers read events.
- How consumer groups share work.
- Why message keys matter.
- How offsets make replay possible.
- How to handle bad messages with a dead-letter topic.

## Prerequisites

Install these first:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Node.js](https://nodejs.org/) 20 or newer
- Git, if you want to commit your progress

## Quick Start

Install dependencies:

```bash
npm install
```

Start Kafka:

```bash
npm run kafka:up
```

Create the lab topics:

```bash
npm run topics:create
```

Run your first consumer in one terminal:

```bash
npm run lab:01:consumer
```

Run your first producer in another terminal:

```bash
npm run lab:01:producer
```

When you are done:

```bash
npm run kafka:down
```

## Milestones

### Milestone 0: Local Kafka

Files:

- `docker-compose.yml`
- `labs/00-local-kafka/README.md`
- `labs/00-local-kafka/create-topics.js`

You will start a single-node Kafka cluster and create topics manually. This teaches you that Kafka is infrastructure first: producers and consumers are clients that connect to a broker.

### Milestone 1: Producer and Consumer

Files:

- `labs/01-producer-consumer/README.md`
- `labs/01-producer-consumer/producer.js`
- `labs/01-producer-consumer/consumer.js`

You will publish events to a topic and read them back. This is the smallest useful Kafka loop.

### Milestone 2: Consumer Groups

Files:

- `labs/02-consumer-groups/README.md`
- `labs/02-consumer-groups/producer.js`
- `labs/02-consumer-groups/consumer.js`

You will run multiple consumers with the same group id and watch Kafka split partitions between them.

### Milestone 3: Partitions, Keys, and Ordering

Files:

- `labs/03-partitions-keys/README.md`
- `labs/03-partitions-keys/producer.js`
- `labs/03-partitions-keys/consumer.js`

You will send keyed messages and observe that Kafka uses keys to choose partitions. This is how you preserve ordering for related events.

### Milestone 4: Offsets and Replay

Files:

- `labs/04-offsets-replay/README.md`
- `labs/04-offsets-replay/consumer.js`

You will learn why Kafka can replay old events, and why changing a consumer group id changes what the consumer sees.

### Milestone 5: Bad Messages and Dead-Letter Topics

Files:

- `labs/05-dead-letter-topic/README.md`
- `labs/05-dead-letter-topic/producer.js`
- `labs/05-dead-letter-topic/consumer.js`

You will process payment events, detect invalid messages, and publish failures to a dead-letter topic for later inspection.

## Suggested GitHub Story

Use commits as learning checkpoints:

```bash
git init
git add .
git commit -m "Create beginner Kafka learning lab"
```

After each milestone, commit what you changed or notes you added:

```bash
git add .
git commit -m "Complete Kafka producer consumer milestone"
```

This makes the repo useful as both a portfolio project and a record of how your understanding grew.

## Useful Commands

```bash
npm run kafka:up          # start Kafka
npm run kafka:down        # stop Kafka
npm run kafka:logs        # follow Kafka logs
npm run topics:create     # create all lab topics
npm run topics:list       # list topics
npm run topics:delete     # delete lab topics
npm run check             # syntax-check lab scripts
```

## Troubleshooting

If a script cannot connect to Kafka, make sure Docker Desktop is running and Kafka is up:

```bash
docker compose ps
```

If topics already exist, that is fine. The topic creation script skips existing topics.

If a consumer seems stuck, it may simply be waiting for new events. Run the matching producer in another terminal.
