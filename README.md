# Learning Kafka From Scratch

A hands-on Kafka lab for absolute beginners. The goal is to learn Kafka by running small examples, changing code, and observing what happens.

This repo is intentionally simple:

- Kafka runs locally in Docker.
- Examples are plain Python using `confluent-kafka`.
- Each milestone focuses on one Kafka concept.
- The code is small enough to read before you run it.

Future AI chats should read `AI_CONTEXT.md` first. It captures the project purpose, conventions, and next milestone ideas.

If you are learning Kafka from scratch, read `LEARNING_GUIDE.md` before diving into the code. It explains what each lab is teaching and how to study it.

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
- How stronger producer guarantees work.
- How teams model retries and parking lot topics.
- How schema evolution affects long-lived event streams.
- How to reason about consumer lag in production.

## Prerequisites

Install these first:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Python](https://www.python.org/downloads/) 3.11 or newer
- Git, if you want to commit your progress

## Quick Start

Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

If activation fails, try the shell-specific form:

```bash
bash/zsh: source .venv/bin/activate
fish: source .venv/bin/activate.fish
```

You can also skip activation entirely and call the venv interpreter directly:

```bash
.venv/bin/python -m pip install -r requirements.txt
```

Verify the Kafka client import:

```bash
python -c "import confluent_kafka; print(confluent_kafka.__version__)"
```

Without activation:

```bash
.venv/bin/python -c "import confluent_kafka; print(confluent_kafka.__version__)"
```

Start Kafka:

```bash
docker compose up -d
```

Create the lab topics:

```bash
python labs/00-local-kafka/create_topics.py
```

Run your first consumer in one terminal:

```bash
python labs/01-producer-consumer/consumer.py
```

Run your first producer in another terminal:

```bash
python labs/01-producer-consumer/producer.py
```

When you are done:

```bash
docker compose down
```

## Milestones

### Beginner Track

### Milestone 0: Local Kafka

Files:

- `docker-compose.yml`
- `labs/00-local-kafka/README.md`
- `labs/00-local-kafka/create_topics.py`

You will start a single-node Kafka cluster and create topics manually. This teaches you that Kafka is infrastructure first: producers and consumers are clients that connect to a broker.

### Milestone 1: Producer and Consumer

Files:

- `labs/01-producer-consumer/README.md`
- `labs/01-producer-consumer/producer.py`
- `labs/01-producer-consumer/consumer.py`

You will publish events to a topic and read them back. This is the smallest useful Kafka loop.

### Milestone 2: Consumer Groups

Files:

- `labs/02-consumer-groups/README.md`
- `labs/02-consumer-groups/producer.py`
- `labs/02-consumer-groups/consumer.py`

You will run multiple consumers with the same group id and watch Kafka split partitions between them.

### Milestone 3: Partitions, Keys, and Ordering

Files:

- `labs/03-partitions-keys/README.md`
- `labs/03-partitions-keys/producer.py`
- `labs/03-partitions-keys/consumer.py`

You will send keyed messages and observe that Kafka uses keys to choose partitions. This is how you preserve ordering for related events.

### Milestone 4: Offsets and Replay

Files:

- `labs/04-offsets-replay/README.md`
- `labs/04-offsets-replay/consumer.py`

You will learn why Kafka can replay old events, and why changing a consumer group id changes what the consumer sees.

### Milestone 5: Bad Messages and Dead-Letter Topics

Files:

- `labs/05-dead-letter-topic/README.md`
- `labs/05-dead-letter-topic/producer.py`
- `labs/05-dead-letter-topic/consumer.py`

You will process payment events, detect invalid messages, and publish failures to a dead-letter topic for later inspection.

### Enterprise Track

### Milestone 6: Producer Guarantees

Files:

- `labs/06-producer-guarantees/README.md`
- `labs/06-producer-guarantees/producer.py`
- `labs/06-producer-guarantees/consumer.py`

You will learn why enterprise producers care about acknowledgements, batching, compression, and idempotence.

### Milestone 7: Retry Topics And Parking Lots

Files:

- `labs/07-retry-topics/README.md`
- `labs/07-retry-topics/seed_events.py`
- `labs/07-retry-topics/process_main.py`
- `labs/07-retry-topics/replay_retry.py`

You will model transient failures, retries, and parking lots the way many production Kafka systems do.

### Milestone 8: Schema Evolution

Files:

- `labs/08-schema-evolution/README.md`
- `labs/08-schema-evolution/producer.py`
- `labs/08-schema-evolution/consumer.py`

You will learn why event contracts evolve carefully and why consumers should usually be tolerant readers.

### Milestone 9: Consumer Lag

Files:

- `labs/09-consumer-lag/README.md`
- `labs/09-consumer-lag/report.py`

You will learn how teams inspect consumer lag and why it is one of the most important Kafka operational signals.

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

## Recommended Learning Path

Start here:

1. `LEARNING_GUIDE.md`
2. Milestones 1 through 5
3. Milestones 6 through 9

Treat milestones 1 through 5 as your fundamentals track and 6 through 9 as your enterprise track.

## Useful Commands

```bash
docker compose up -d                          # start Kafka
docker compose down                           # stop Kafka
docker compose logs -f kafka                  # follow Kafka logs
python labs/00-local-kafka/create_topics.py   # create all lab topics
python labs/00-local-kafka/list_topics.py     # list topics
python labs/00-local-kafka/delete_topics.py   # delete lab topics
python -m compileall lab_kafka labs           # syntax-check lab scripts
```

## Troubleshooting

If a script cannot connect to Kafka, make sure Docker Desktop is running and Kafka is up:

```bash
docker compose ps
```

### WSL And Docker Desktop

If you run the lab inside WSL, Docker must be available inside that WSL distro, not only from Windows PowerShell.

From your WSL shell, check:

```bash
docker info
docker compose version
docker pull apache/kafka:3.7.2
```

If `docker info` cannot connect to the daemon, open Docker Desktop on Windows and enable WSL integration for your distro:

1. Open Docker Desktop.
2. Go to Settings.
3. Open Resources.
4. Open WSL Integration.
5. Enable integration for the distro where this repo is running.
6. Restart the WSL shell.

Then retry:

```bash
docker compose up -d
```

If topics already exist, that is fine. The topic creation script skips existing topics.

If a consumer seems stuck, it may simply be waiting for new events. Run the matching producer in another terminal.

If you see `ModuleNotFoundError: No module named 'confluent_kafka'` inside WSL, install the dependency in that same WSL shell and interpreter:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -c "import confluent_kafka; print(confluent_kafka.__version__)"
```

If activation keeps failing, use:

```bash
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -c "import confluent_kafka; print(confluent_kafka.__version__)"
```

If you see `error: externally-managed-environment`, your distro is blocking global pip installs. Stay inside `.venv` instead of using `sudo pip` or `--break-system-packages`.
