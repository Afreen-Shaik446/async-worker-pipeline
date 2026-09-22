"""Task producer: publishes durable JSON task messages."""
import argparse
import json
import uuid

import pika

from . import config
from .topology import declare_topology


def publish(channel, task_id: str, payload: dict) -> None:
    body = json.dumps({"task_id": task_id, "payload": payload}).encode()
    channel.basic_publish(
        exchange=config.MAIN_EXCHANGE,
        routing_key=config.ROUTING_KEY_TASK,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type="application/json",
            headers={"x-retry-count": 0},
        ),
    )
    print(f"published task {task_id}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=3, help="Number of tasks to publish")
    parser.add_argument("--fail", action="store_true", help="Publish one task that will fail")
    args = parser.parse_args()

    connection = pika.BlockingConnection(pika.URLParameters(config.RABBITMQ_URL))
    channel = connection.channel()
    declare_topology(channel)

    for _ in range(args.count):
        publish(channel, str(uuid.uuid4()), {"kind": "demo"})
    if args.fail:
        publish(channel, str(uuid.uuid4()), {"kind": "demo", "fail": True})

    connection.close()


if __name__ == "__main__":
    main()
