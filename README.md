# async-worker-pipeline

A Python async task pipeline built on RabbitMQ (pika) and MongoDB: a producer publishes durable task messages, a consumer processes them with retry (exponential backoff), a dead-letter queue catches exhausted tasks, task state is persisted in MongoDB, and a small HTTP health endpoint reports broker/store connectivity.

This is a demonstration project — it shows messaging patterns, not a production deployment.

## Architecture

```
                        ┌─────────────────────────────┐
                        │   tasks.direct (exchange)   │
                        └──────────────┬──────────────┘
                     publish           │ routing key: task
                 ┌──────────┐          ▼
                 │ producer │   ┌──────────────┐
                 └──────────┘   │   tasks.q    │◀──────────────────┐
                                └──────┬───────┘   dead-lettered    │
                                       │ consume        retry      │
                                ┌──────▼───────┐     (expired)      │
                                │   consumer   │                  │
                                │              │                  │
                                │ success: ack │   ┌──────────────┴──────┐
                                │ fail: schedule│   │ tasks.retry.q       │
                                │ retry w/      │──▶│ (per-msg TTL,       │
                                │ backoff       │   │  DLX → main)        │
                                └──────┬───────┘   └─────────────────────┘
                                       │ max retries exceeded
                                       ▼
                                ┌──────────────┐
                                │  tasks.dlq   │  (inspect / requeue manually)
                                └──────────────┘

  MongoDB "tasks" collection persists every task:
  received → processing → succeeded | dead_lettered
```

Retry uses the dead-letter exchange (`tasks.dlx`) pattern: failed messages are republished with a per-message TTL (`expiration`) to `tasks.retry.q`, which dead-letters them back to the main exchange after the delay. Backoff is `min(base * 2^attempt, cap)`.

## Quickstart

```bash
docker compose up -d        # RabbitMQ (mgmt UI :15672), MongoDB (:27017), worker
docker compose logs -f worker

# Publish a few demo tasks (guest/guest on localhost only)
python -m worker.producer --count 5

# Publish one task that will fail and end up in the DLQ
python -m worker.producer --fail

# Health check
curl localhost:8080/healthz
```

Without Docker, install `requirements.txt`, export `RABBITMQ_URL` and `MONGO_URL`, then run `python -m worker.consumer` and `python -m worker.producer`.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/%2F` | Broker connection URL |
| `MONGO_URL` | `mongodb://localhost:27017` | MongoDB connection URL |
| `MONGO_DB` | `pipeline` | Database name |
| `MAX_RETRIES` | `5` | Attempts before a task goes to the DLQ |
| `BACKOFF_BASE_SECONDS` | `2` | Base delay for exponential backoff |
| `BACKOFF_CAP_SECONDS` | `300` | Maximum retry delay |
| `HEALTH_PORT` | `8080` | Health endpoint port |

## Tests

`tests/test_backoff.py` covers the pure backoff math (no broker needed):

```bash
pip install pytest
pytest -v
```
