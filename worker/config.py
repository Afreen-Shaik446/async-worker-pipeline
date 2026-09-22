"""Shared configuration from environment variables."""
import os

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "pipeline")

MAIN_EXCHANGE = "tasks.direct"
DLX_EXCHANGE = "tasks.dlx"
MAIN_QUEUE = "tasks.q"
RETRY_QUEUE = "tasks.retry.q"
DLQ = "tasks.dlq"
ROUTING_KEY_TASK = "task"
ROUTING_KEY_RETRY = "retry"
ROUTING_KEY_DEAD = "dead"

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))
BACKOFF_BASE_SECONDS = float(os.environ.get("BACKOFF_BASE_SECONDS", "2"))
BACKOFF_CAP_SECONDS = float(os.environ.get("BACKOFF_CAP_SECONDS", "300"))

HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8080"))
