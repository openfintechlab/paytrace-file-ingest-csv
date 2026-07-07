# -*- coding: utf-8 -*-
"""Centralized logging helpers with light context support."""

from __future__ import annotations

import logging
from typing import Any

from environs import Env

env = Env()


class Logging:
    """Utility class for centralized application logging."""

    _DEFAULT_LEVEL = "INFO"
    _DEFAULT_FORMAT = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
    _SENSITIVE_MARKERS = ("password", "secret", "token", "credential", "authorization")

    _level_name = env.str("OFTL_LOG_LEVEL", _DEFAULT_LEVEL).upper()
    _log_format = env.str("OFTL_LOG_FORMAT", _DEFAULT_FORMAT).strip() or _DEFAULT_FORMAT
    _log_level = getattr(logging, _level_name, logging.INFO)

    logging.basicConfig(level=_log_level, format=_log_format)
    _logger = logging.getLogger("OFTL")

    @classmethod
    def _sanitize_context(cls, context: dict[str, Any]) -> dict[str, Any]:
        sanitized: dict[str, Any] = {}
        for key, value in context.items():
            if any(marker in key.lower() for marker in cls._SENSITIVE_MARKERS):
                sanitized[key] = "***"
                continue
            sanitized[key] = value
        return sanitized

    @classmethod
    def _render_message(cls, message: str, **context: Any) -> str:
        if not context:
            return message
        sanitized = cls._sanitize_context(context)
        rendered_context = " ".join(f"{key}={sanitized[key]!r}" for key in sorted(sanitized))
        return f"{message} | {rendered_context}"

    @classmethod
    def info(cls, message: str, *args: Any, **kwargs: Any) -> None:
        cls._logger.info(message, *args, **kwargs)

    @classmethod
    def warning(cls, message: str, *args: Any, **kwargs: Any) -> None:
        cls._logger.warning(message, *args, **kwargs)

    @classmethod
    def error(cls, message: str, *args: Any, **kwargs: Any) -> None:
        cls._logger.error(message, *args, **kwargs)

    @classmethod
    def info_context(cls, message: str, **context: Any) -> None:
        cls._logger.info(cls._render_message(message, **context))

    @classmethod
    def warning_context(cls, message: str, **context: Any) -> None:
        cls._logger.warning(cls._render_message(message, **context))

    @classmethod
    def error_context(cls, message: str, **context: Any) -> None:
        cls._logger.error(cls._render_message(message, **context))

    @classmethod
    def exception_context(cls, message: str, **context: Any) -> None:
        cls._logger.exception(cls._render_message(message, **context))
