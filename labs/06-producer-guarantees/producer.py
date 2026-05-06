import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab_kafka.client import delivery_report, encode_event, enterprise_producer, topic_name


def main() -> None:
    topic = topic_name("audit")
    kafka_producer = enterprise_producer()
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 20

    started_at = time.perf_counter()

    for index in range(1, count + 1):
        event = {
            "type": "AuditEventRecorded",
            "eventId": f"audit-{int(time.time())}-{index}",
            "tenantId": f"tenant-{(index % 3) + 1}",
            "sequence": index,
            "createdAt": datetime.now(timezone.utc).isoformat(),
        }
        kafka_producer.produce(
            topic,
            key=event["tenantId"],
            value=encode_event(event),
            callback=delivery_report,
        )

    kafka_producer.flush()
    elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)

    print(
        {
            "topic": topic,
            "messagesSent": count,
            "acks": "all",
            "idempotence": True,
            "compression": "snappy",
            "lingerMs": 25,
            "elapsedMs": elapsed_ms,
        }
    )


if __name__ == "__main__":
    main()

