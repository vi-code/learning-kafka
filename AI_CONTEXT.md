# AI Context

This file is the handoff note for future AI chats working in this repo. Keep it current when the lab structure, learning goals, or project conventions change.

## Project Purpose

This repo is a hands-on Kafka learning lab for a complete Kafka beginner. It should teach concepts from scratch through small, runnable code exercises that can also serve as a public GitHub portfolio project.

The tone should stay beginner-friendly, practical, and encouraging. Avoid assuming prior Kafka knowledge.

## Current Stack

- Runtime: Node.js 20+ with ES modules.
- Kafka client: KafkaJS.
- Local infrastructure: Docker Compose running Bitnami Kafka in KRaft mode.
- Main local broker URL: `localhost:9092`.

## Repository Layout

- `README.md`: top-level learning path, quick start, milestones, GitHub story, troubleshooting.
- `docker-compose.yml`: single-node local Kafka.
- `package.json`: npm scripts for Kafka, topic management, and each lab.
- `lib/kafka.js`: shared KafkaJS client helpers.
- `lib/topics.js`: lab topic definitions.
- `labs/00-local-kafka`: start Kafka, create/list/delete topics, learn vocabulary.
- `labs/01-producer-consumer`: first producer and consumer.
- `labs/02-consumer-groups`: multiple consumers sharing partitions.
- `labs/03-partitions-keys`: keys, partitions, and ordering.
- `labs/04-offsets-replay`: offsets, consumer groups, and replay.
- `labs/05-dead-letter-topic`: validation failures and dead-letter topic pattern.

## Learning Design

Each milestone should keep this shape:

1. Explain the concept in plain language.
2. Run one or two commands.
3. Read a small amount of code.
4. Change something and observe the result.
5. Add a reflection prompt so the learner can commit notes.

Good future milestones:

- Serialization and schemas.
- Retries and idempotent producers.
- Consumer lag.
- Kafka Connect concepts.
- Stream processing with Kafka Streams or a lightweight Node equivalent.
- Observability and dashboards.

## Commands

Install dependencies:

```bash
npm install
```

Start Kafka:

```bash
npm run kafka:up
```

Create topics:

```bash
npm run topics:create
```

Run checks:

```bash
npm run check
```

Stop Kafka:

```bash
npm run kafka:down
```

## Conventions

- Keep examples small and readable.
- Prefer explicit scripts over clever abstractions.
- Use JSON events with `type` and timestamps.
- Keep topic names under the `lab.` prefix unless there is a strong reason not to.
- Do not auto-create topics in Kafka; topic creation is part of the learning.
- Keep docs and code in sync. If a command changes, update the relevant milestone README.

## Current Validation State

Last validated locally:

- `npm install`
- `npm run check`
- `docker compose config`

Kafka was not left running as part of setup.

## GitHub Notes

The intended first commit is the full lab scaffold plus this context file.

Suggested repo name:

```text
learning-kafka
```

Suggested description:

```text
Hands-on Kafka learning lab for beginners.
```

