"""Declares the exchange/queue topology used by producer and consumer."""
import pika

from . import config


def declare_topology(channel: pika.adapters.blocking_connection.BlockingChannel) -> None:
    # Main + dead-letter exchanges
    channel.exchange_declare(exchange=config.MAIN_EXCHANGE, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=config.DLX_EXCHANGE, exchange_type="direct", durable=True)

    # Main queue: unexpected dead-letters (requeue=False) route to the DLQ.
    # The consumer itself acks and republishes for retries; this is a safety net.
    channel.queue_declare(
        queue=config.MAIN_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": config.DLX_EXCHANGE,
            "x-dead-letter-routing-key": config.ROUTING_KEY_DEAD,
        },
    )
    channel.queue_bind(
        queue=config.MAIN_QUEUE,
        exchange=config.MAIN_EXCHANGE,
        routing_key=config.ROUTING_KEY_TASK,
    )

    # Retry queue: holds messages until their per-message TTL expires,
    # then dead-letters them back to the main exchange.
    channel.queue_declare(
        queue=config.RETRY_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": config.MAIN_EXCHANGE,
            "x-dead-letter-routing-key": config.ROUTING_KEY_TASK,
        },
    )
    channel.queue_bind(
        queue=config.RETRY_QUEUE,
        exchange=config.DLX_EXCHANGE,
        routing_key=config.ROUTING_KEY_RETRY,
    )

    # Dead-letter queue for exhausted tasks
    channel.queue_declare(queue=config.DLQ, durable=True)
    channel.queue_bind(
        queue=config.DLQ,
        exchange=config.DLX_EXCHANGE,
        routing_key=config.ROUTING_KEY_DEAD,
    )
