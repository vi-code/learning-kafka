# AI Context

This file is the handoff note for future AI chats working in this repo. Keep it current when the lab structure, learning goals, or project conventions change.

## Project Purpose

This repo is a hands-on Kafka learning lab for a complete Kafka beginner. It should teach concepts from scratch through small, runnable code exercises that can also serve as a public GitHub portfolio project.

The tone should stay beginner-friendly, practical, and encouraging. Avoid assuming prior Kafka knowledge.

## Current Stack

- Runtime: Python 3.11+ in a repo-local virtual environment when possible.
- Kafka client: `confluent-kafka`.
- Local infrastructure: Docker Compose running the official Apache Kafka image.
- Main local broker URL: `localhost:9092`.

## Repository Layout

- `README.md`: top-level learning path, quick start, milestones, GitHub story, troubleshooting.
- `docker-compose.yml`: single-node local Kafka.
- `requirements.txt`: Python dependency lock point for learners.
- `lab_kafka/client.py`: shared Kafka client helpers.
- `lab_kafka/topics.py`: lab topic definitions.
- `labs/00-local-kafka`: start Kafka, create/list/delete topics, learn vocabulary.
- `labs/01-producer-consumer`: first producer and consumer.
- `labs/02-consumer-groups`: multiple consumers sharing partitions.
- `labs/03-partitions-keys`: keys, partitions, and ordering.
- `labs/04-offsets-replay`: offsets, consumer groups, and replay.
- `labs/05-dead-letter-topic`: validation failures and dead-letter topic pattern.
- `LEARNING_GUIDE.md`: beginner learning path and study guide.

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
- Stream processing with a lightweight Python stream-processing example.
- Observability and dashboards.

## Commands

Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Start Kafka:

```bash
docker compose up -d
```

Create topics:

```bash
python labs/00-local-kafka/create_topics.py
```

Run checks:

```bash
python -m compileall lab_kafka labs
```

Stop Kafka:

```bash
docker compose down
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

- `python -m pip install -r requirements.txt`
- `python -m compileall lab_kafka labs`
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
