import json
import os

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient


def brokers() -> str:
    return os.getenv("KAFKA_BROKERS", "localhost:9092")


def topic_prefix() -> str:
    return os.getenv("TOPIC_PREFIX", "lab")


def topic_name(slug: str) -> str:
    return f"{topic_prefix()}.{slug}"


def admin_client() -> AdminClient:
    return AdminClient({"bootstrap.servers": brokers()})


def producer() -> Producer:
    return Producer({"bootstrap.servers": brokers()})


def consumer(group_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": brokers(),
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def encode_event(event: dict) -> str:
    return json.dumps(event, separators=(",", ":"))


def decode_message(message) -> dict:
    key = message.key().decode("utf-8") if message.key() else None
    value = json.loads(message.value().decode("utf-8")) if message.value() else None

    return {
        "key": key,
        "value": value,
    }


def delivery_report(error, message) -> None:
    if error is not None:
        raise RuntimeError(f"Delivery failed: {error}")

    print(
        {
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
        }
    )
