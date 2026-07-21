from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Iterator

from app.config import settings

LOG_CONTEXT_FIELDS = (
    "requestId",
    "taskId",
    "messageId",
    "taskType",
    "workerId",
    "retryCount",
    "queueLatencyMillis",
    "logType",
)
SENSITIVE_FIELD_TOKENS = (
    "authorization",
    "password",
    "paymentkey",
    "payment_key",
    "secret",
    "token",
    "api_key",
)
NON_SENSITIVE_TOKEN_METRIC_FIELDS = {
    "inputtokens",
    "outputtokens",
    "totaltokens",
    "cachedinputtokens",
    "reasoningoutputtokens",
}
STANDARD_LOG_RECORD_FIELDS = set(logging.makeLogRecord({}).__dict__.keys()) | {"message", "asctime"}
DEFAULT_LOG_CONTEXT: dict[str, Any] = {
    "requestId": None,
    "taskId": None,
    "messageId": None,
    "taskType": None,
    "workerId": None,
    "retryCount": None,
    "queueLatencyMillis": None,
    "logType": settings.worker_log_type,
}
_worker_log_context: ContextVar[dict[str, Any]] = ContextVar(
    "worker_log_context",
    default=DEFAULT_LOG_CONTEXT.copy(),
)


def get_log_context() -> dict[str, Any]:
    return {**DEFAULT_LOG_CONTEXT, **_worker_log_context.get()}


@contextmanager
def bind_log_context(**values: Any) -> Iterator[None]:
    context = get_log_context()
    for key, value in values.items():
        if key in LOG_CONTEXT_FIELDS:
            context[key] = value
    token = _worker_log_context.set(context)
    try:
        yield
    finally:
        _worker_log_context.reset(token)


def log_event(logger: logging.Logger, level: int, event: str, message: str | None = None, **fields: Any) -> None:
    logger.log(level, message or event, extra={"event": event, **fields})


def log_info(logger: logging.Logger, event: str, message: str | None = None, **fields: Any) -> None:
    log_event(logger, logging.INFO, event, message, **fields)


def log_warning(logger: logging.Logger, event: str, message: str | None = None, **fields: Any) -> None:
    log_event(logger, logging.WARNING, event, message, **fields)


def log_error(logger: logging.Logger, event: str, message: str | None = None, **fields: Any) -> None:
    log_event(logger, logging.ERROR, event, message, **fields)


def log_exception(logger: logging.Logger, event: str, message: str | None = None, **fields: Any) -> None:
    logger.exception(message or event, extra={"event": event, **fields})


class WorkerContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        context = get_log_context()
        for field_name, default_value in context.items():
            if not hasattr(record, field_name):
                setattr(record, field_name, default_value)
        if not hasattr(record, "event"):
            setattr(record, "event", None)
        if not hasattr(record, "errorCode"):
            setattr(record, "errorCode", None)
        return True


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "logger_name": record.name,
            "event": getattr(record, "event", None) or record.getMessage(),
            "errorCode": getattr(record, "errorCode", None),
            "requestId": getattr(record, "requestId", None),
            "taskId": getattr(record, "taskId", None),
            "messageId": getattr(record, "messageId", None),
            "taskType": getattr(record, "taskType", None),
            "workerId": getattr(record, "workerId", None),
            "retryCount": getattr(record, "retryCount", None),
            "queueLatencyMillis": getattr(record, "queueLatencyMillis", None),
            "logType": getattr(record, "logType", settings.worker_log_type),
        }

        message = record.getMessage()
        if message and message != payload["event"]:
            payload["message"] = message

        for key, value in record.__dict__.items():
            if key in STANDARD_LOG_RECORD_FIELDS or key in payload:
                continue
            payload[key] = _sanitize_value(key, value)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)

        return json.dumps(payload, ensure_ascii=False)


def configure_worker_logging() -> None:
    root_logger = logging.getLogger()
    if getattr(root_logger, "_worker_logging_configured", False):
        return

    root_logger.setLevel(logging.INFO)
    context_filter = WorkerContextFilter()

    if not root_logger.handlers:
        root_logger.addHandler(logging.StreamHandler())

    for handler in root_logger.handlers:
        handler.setFormatter(JsonLogFormatter())
        handler.addFilter(context_filter)

    root_logger.addFilter(context_filter)
    root_logger._worker_logging_configured = True  # type: ignore[attr-defined]


def _sanitize_value(key: str, value: Any) -> Any:
    if _is_sensitive_key(key):
        return "***masked***"
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, dict):
        return {str(item_key): _sanitize_value(str(item_key), item_value) for item_key, item_value in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_sanitize_value(key, item) for item in value]
    if hasattr(value, "model_dump"):
        return _sanitize_value(key, value.model_dump(mode="json"))
    return str(value)


def _is_sensitive_key(key: str) -> bool:
    lowered = key.lower()
    if lowered in NON_SENSITIVE_TOKEN_METRIC_FIELDS:
        return False
    return any(token in lowered for token in SENSITIVE_FIELD_TOKENS)
