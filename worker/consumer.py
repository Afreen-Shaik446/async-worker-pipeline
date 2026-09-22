"""Task consumer: processes tasks, retries with exponential backoff, DLQs exhausted tasks.

Retry strategy: on failure the consumer acks the message and republishes it to
the dead-letter exchange with routing key "retry" and a per-message TTL equal to
the backoff delay. The retry queue holds it until the TTL expires, then
dead-letters it back to the main exchange. After MAX_RETRIES attempts the task
is published to the DLQ ("dead" routing key) and marked dead_lettered in MongoDB.
"""
import datetime
import json
import logging
import threading

import pika
import pymongo

from . import config
from .health import start_health_server
from .topology import declare_topology

logger = logging.getLogger(__name__)


def compute_backoff_seconds(attempt: int) -> float:
    """Exponential backoff: base * 2^attempt, capped. attempt is 0-based."""
    return min(config.BACKOFF_BASE_SECONDS * (2 ** attempt), config.BACKOFF_CAP_SECONDS)


def process_task(payload: dict) -> None:
    """Demo task handler. Replace with real work.

    Raises on payloads marked {"fail": true} to exercise the retry/DLQ path.
    """
    if payload.get("fail"):
        raise RuntimeError("task requested failure (demo)")
    logger.info("processed task payload: %s", payload)


class TaskConsumer:
    def __init__(self) -> None:
        self.mongo = pymongo.MongoClient(config.MONGO_URL, serverSelectionTimeoutMS=5000)
        self.tasks = self.mongo[config.MONGO_DB]["tasks"]
        self.connection = pika.BlockingConnection(pika.URLParameters(config.RABBITMQ_URL))
        self.channel = self.connection.channel()
        declare_topology(self.channel)
        self.channel.basic_qos(prefetch_count=1)

    def _record(self, task_id: str, update: dict) -> None:
        update = dict(update)
        update["updated_at"] = datetime.datetime.now(datetime.timezone.utc)
        self.tasks.update_one({"task_id": task_id}, {"$set": update}, upsert=True)

    def _schedule_retry(self, body: bytes, headers: dict, attempt: int) -> None:
        delay_ms = int(compute_backoff_seconds(attempt) * 1000)
        self.channel.basic_publish(
            exchange=config.DLX_EXCHANGE,
            routing_key=config.ROUTING_KEY_RETRY,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
                expiration=str(delay_ms),  # per-message TTL
                headers={"x-retry-count": attempt + 1},
            ),
        )
        logger.info("scheduled retry %d in %d ms", attempt + 1, delay_ms)

    def _send_to_dlq(self, body: bytes, headers: dict) -> None:
        self.channel.basic_publish(
            exchange=config.DLX_EXCHANGE,
            routing_key=config.ROUTING_KEY_DEAD,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
                headers=headers,
            ),
        )

    def on_message(self, channel, method, properties, body: bytes) -> None:
        headers = dict(properties.headers or {})
        attempt = int(headers.get("x-retry-count", 0))
        try:
            message = json.loads(body)
        except json.JSONDecodeError:
            logger.exception("dropping unparseable message")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        task_id = str(message.get("task_id", "unknown"))
        payload = message.get("payload", {})
        self._record(task_id, {"status": "processing", "attempt": attempt + 1})

        try:
            process_task(payload)
        except Exception:
            logger.exception("task %s failed on attempt %d", task_id, attempt + 1)
            if attempt < config.MAX_RETRIES:
                self._record(task_id, {"status": "retrying"})
                self._schedule_retry(body, headers, attempt)
            else:
                self._record(task_id, {"status": "dead_lettered"})
                self._send_to_dlq(body, headers)
                logger.warning("task %s exhausted retries; sent to DLQ", task_id)
        else:
            self._record(task_id, {"status": "succeeded"})
            logger.info("task %s succeeded", task_id)
        finally:
            channel.basic_ack(delivery_tag=method.delivery_tag)

    def run(self) -> None:
        self.channel.basic_consume(
            queue=config.MAIN_QUEUE, on_message_callback=self.on_message
        )
        logger.info("consumer started; waiting for messages")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()
        finally:
            self.connection.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    consumer = TaskConsumer()
    health_thread = threading.Thread(
        target=start_health_server,
        args=(config.HEALTH_PORT, consumer),
        daemon=True,
    )
    health_thread.start()
    consumer.run()


if __name__ == "__main__":
    main()
