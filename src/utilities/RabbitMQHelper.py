# -*- coding: utf-8 -*-
"""
RabbitMQ utility with connection reuse and auto-reconnect behavior.
"""

from __future__ import annotations

import atexit
import json
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from threading import Lock
from typing import TYPE_CHECKING, Any

from .ConfigLoader import ConfigLoader
from .Logging import Logging

try:
    import pika
except Exception:  # pragma: no cover
    pika = None

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel


class RabbitMQHelper:
    """Singleton RabbitMQ helper for queue-send and exchange-publish operations."""

    _connection = None
    _channel: BlockingChannel | None = None
    _lock: Lock = Lock()
    _delivery_confirm_enabled = False

    @classmethod
    def initialize_connection(cls) -> None:
        """Initialize singleton connection/channel at application startup."""
        cls._ensure_channel()

    @classmethod
    def _as_bool(cls, value: Any, default: bool) -> bool:
        if value is None:
            return default
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    @classmethod
    def _build_connection_parameters(cls) -> Any:
        pika_module = cls._require_pika()

        host = str(ConfigLoader.get("OFTL_RABITMQ_HOST", "localhost"))
        port = int(ConfigLoader.get("OFTL_RABITMQ_PORT", 5672))
        username = str(ConfigLoader.get("OFTL_RABITMQ_USERNAME", "guest"))
        password = str(ConfigLoader.get("OFTL_RABITMQ_PASSWORD_SECRET", "guest"))
        virtual_host = str(ConfigLoader.get("OFTL_RABITMQ_VHOST", "/"))
        heartbeat = int(ConfigLoader.get("OFTL_RABITMQ_HEARTBEAT", 60))
        blocked_timeout = float(ConfigLoader.get("OFTL_RABITMQ_BLOCKED_CONNECTION_TIMEOUT", 30))
        connection_attempts = int(ConfigLoader.get("OFTL_RABITMQ_CONNECTION_ATTEMPTS", 3))
        retry_delay = float(ConfigLoader.get("OFTL_RABITMQ_RETRY_DELAY", 2))
        socket_timeout = float(ConfigLoader.get("OFTL_RABITMQ_SOCKET_TIMEOUT", 5))

        credentials = pika_module.PlainCredentials(username=username, password=password)
        return pika_module.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtual_host,
            heartbeat=heartbeat,
            blocked_connection_timeout=blocked_timeout,
            connection_attempts=connection_attempts,
            retry_delay=retry_delay,
            socket_timeout=socket_timeout,
            credentials=credentials,
        )

    @classmethod
    def _require_pika(cls) -> Any:
        if pika is None:
            raise RuntimeError("RabbitMQ client is unavailable. Install dependency: pika")
        return pika

    @classmethod
    def _ensure_channel(cls) -> BlockingChannel:
        with cls._lock:
            connection_closed = cls._connection is None or cls._connection.is_closed
            channel_closed = cls._channel is None or cls._channel.is_closed

            if connection_closed or channel_closed:
                if cls._channel is not None:
                    try:
                        cls._channel.close()
                    except Exception:
                        pass
                if cls._connection is not None:
                    try:
                        cls._connection.close()
                    except Exception:
                        pass

                params = cls._build_connection_parameters()
                pika_module = cls._require_pika()
                cls._connection = pika_module.BlockingConnection(params)
                cls._channel = cls._connection.channel()
                cls._delivery_confirm_enabled = False
                Logging.info("RabbitMQ connection established.")

            if cls._channel is None:  # pragma: no cover
                raise RuntimeError("RabbitMQ channel initialization failed.")
            return cls._channel

    @classmethod
    def _ensure_delivery_confirmation(cls, channel: BlockingChannel) -> None:
        if cls._delivery_confirm_enabled:
            return
        channel.confirm_delivery()
        cls._delivery_confirm_enabled = True

    @classmethod
    def _build_payload(cls, message: Any) -> tuple[bytes, str]:
        if isinstance(message, bytes):
            return message, "application/octet-stream"
        if isinstance(message, str):
            return message.encode("utf-8"), "text/plain"
        return (
            json.dumps(
                message,
                separators=(",", ":"),
                ensure_ascii=False,
                default=cls._json_default,
            ).encode("utf-8"),
            "application/json",
        )

    @staticmethod
    def _json_default(value: Any) -> Any:
        if is_dataclass(value) and not isinstance(value, type):
            return asdict(value)
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

    @classmethod
    def send_p2p_message(cls, queue_name: str, message: Any) -> bool:
        """Send a point-to-point message to a queue. Queue is created if missing."""
        durable_queue = cls._as_bool(ConfigLoader.get("OFTL_RABITMQ_QUEUE_DURABLE", "true"), True)
        persistent_message = cls._as_bool(ConfigLoader.get("OFTL_RABITMQ_MESSAGE_PERSISTENT", "true"), True)
        pika_module = cls._require_pika()

        def _send_once() -> bool:
            channel = cls._ensure_channel()
            cls._ensure_delivery_confirmation(channel)
            channel.queue_declare(queue=queue_name, durable=durable_queue)

            payload, content_type = cls._build_payload(message)
            properties = pika_module.BasicProperties(
                content_type=content_type,
                content_encoding="utf-8",
                delivery_mode=2 if persistent_message else 1,
            )

            return bool(
                channel.basic_publish(
                    exchange="",
                    routing_key=queue_name,
                    body=payload,
                    mandatory=cls._as_bool(ConfigLoader.get("OFTL_RABITMQ_PUBLISH_MANDATORY", "false"), False),
                    properties=properties,
                )
            )

        try:
            return _send_once()
        except Exception as exc:
            Logging.warning("RabbitMQ send failed, retrying once: %s", exc)
            cls.close()
            return _send_once()

    @classmethod
    def publish_message(
        cls,
        exchange_name: str,
        routing_key: str,
        message: Any,
        exchange_type: str | None = None,
    ) -> bool:
        """Publish a message to an exchange + routing key."""
        resolved_exchange_type = exchange_type or str(ConfigLoader.get("OFTL_RABITMQ_EXCHANGE_TYPE", "direct"))
        durable_exchange = cls._as_bool(ConfigLoader.get("OFTL_RABITMQ_EXCHANGE_DURABLE", "true"), True)
        persistent_message = cls._as_bool(ConfigLoader.get("OFTL_RABITMQ_MESSAGE_PERSISTENT", "true"), True)
        pika_module = cls._require_pika()

        def _publish_once() -> bool:
            channel = cls._ensure_channel()
            cls._ensure_delivery_confirmation(channel)
            channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=resolved_exchange_type,
                durable=durable_exchange,
            )

            payload, content_type = cls._build_payload(message)
            properties = pika_module.BasicProperties(
                content_type=content_type,
                content_encoding="utf-8",
                delivery_mode=2 if persistent_message else 1,
            )

            return bool(
                channel.basic_publish(
                    exchange=exchange_name,
                    routing_key=routing_key,
                    body=payload,
                    mandatory=cls._as_bool(ConfigLoader.get("OFTL_RABITMQ_PUBLISH_MANDATORY", "false"), False),
                    properties=properties,
                )
            )

        try:
            return _publish_once()
        except Exception as exc:
            Logging.warning("RabbitMQ publish failed, retrying once: %s", exc)
            cls.close()
            return _publish_once()

    @classmethod
    def close(cls) -> None:
        with cls._lock:
            if cls._channel is not None:
                try:
                    cls._channel.close()
                except Exception:
                    pass
            if cls._connection is not None:
                try:
                    cls._connection.close()
                except Exception:
                    pass

            cls._channel = None
            cls._connection = None
            cls._delivery_confirm_enabled = False


atexit.register(RabbitMQHelper.close)
