from __future__ import annotations

import json
from enum import Enum

from openai import APIConnectionError, APIStatusError, APITimeoutError, BadRequestError, RateLimitError
from pydantic import ValidationError


class FailureReasonCode(str, Enum):
    RATE_LIMIT = "RATE_LIMIT"
    QUEUE_TIMEOUT = "QUEUE_TIMEOUT"
    OPENAI_TIMEOUT = "OPENAI_TIMEOUT"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    INTERNAL_ERROR = "INTERNAL_ERROR"


FAILURE_REASON_METRIC_LABELS: dict[FailureReasonCode, str] = {
    FailureReasonCode.RATE_LIMIT: "rate_limit",
    FailureReasonCode.QUEUE_TIMEOUT: "queue_timeout",
    FailureReasonCode.OPENAI_TIMEOUT: "timeout",
    FailureReasonCode.VALIDATION_ERROR: "validation_error",
    FailureReasonCode.INTERNAL_ERROR: "internal_error",
}


def classify_openai_failure(exc: Exception) -> FailureReasonCode:
    if isinstance(exc, RateLimitError):
        return FailureReasonCode.RATE_LIMIT
    if isinstance(exc, (APITimeoutError, APIConnectionError)):
        return FailureReasonCode.OPENAI_TIMEOUT
    if isinstance(exc, BadRequestError):
        return FailureReasonCode.VALIDATION_ERROR
    if isinstance(exc, APIStatusError):
        status_code = getattr(exc, "status_code", None)
        if status_code == 429:
            return FailureReasonCode.RATE_LIMIT
        if status_code is not None and status_code >= 500:
            return FailureReasonCode.INTERNAL_ERROR
        return FailureReasonCode.VALIDATION_ERROR
    if isinstance(exc, (ValidationError, json.JSONDecodeError, TypeError, ValueError)):
        return FailureReasonCode.VALIDATION_ERROR
    return FailureReasonCode.INTERNAL_ERROR
