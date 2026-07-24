from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

DURATION_BUCKETS = (
    0.01,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    3.0,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
    600.0,
    900.0,
    1800.0,
)

_TASK_TYPE_LABELS = {
    "ANALYSIS": "analysis",
    "JOB_POSTING_INGEST": "jobposting",
}

_REASON_LABELS = {
    "RATE_LIMIT": "rate_limit",
    "QUEUE_TIMEOUT": "queue_timeout",
    "OPENAI_TIMEOUT": "timeout",
    "VALIDATION_ERROR": "validation_error",
    "INTERNAL_ERROR": "internal_error",
}

llm_request_duration_seconds = Histogram(
    "llm_request_duration_seconds",
    "Latency of OpenAI requests executed by the worker.",
    labelnames=("task_type", "operation", "outcome"),
    buckets=DURATION_BUCKETS,
)

worker_llm_request_errors_total = Counter(
    "worker_llm_request_errors_total",
    "Count of OpenAI request failures observed by the worker.",
    labelnames=("task_type", "operation", "error_type"),
)

worker_internal_api_duration_seconds = Histogram(
    "worker_internal_api_duration_seconds",
    "Latency of Spring internal API calls made by the worker.",
    labelnames=("task_type", "endpoint", "method", "outcome"),
    buckets=DURATION_BUCKETS,
)

worker_context_fetch_duration_seconds = Histogram(
    "worker_context_fetch_duration_seconds",
    "Latency of worker context fetch API calls.",
    labelnames=("task_type", "endpoint", "outcome"),
    buckets=DURATION_BUCKETS,
)

worker_callback_duration_seconds = Histogram(
    "worker_callback_duration_seconds",
    "Latency of worker callback/finalization API calls.",
    labelnames=("task_type", "endpoint", "outcome"),
    buckets=DURATION_BUCKETS,
)

worker_task_processing_duration_seconds = Histogram(
    "worker_task_processing_duration_seconds",
    "End-to-end worker task processing time from processing start to terminal outcome.",
    labelnames=("task_type", "outcome"),
    buckets=DURATION_BUCKETS,
)

worker_task_queue_wait_duration_seconds = Histogram(
    "worker_task_queue_wait_duration_seconds",
    "Time a task spent waiting in the queue before worker processing started.",
    labelnames=("task_type",),
    buckets=DURATION_BUCKETS,
)

worker_task_retry_count_total = Counter(
    "worker_task_retry_count_total",
    "Count of worker retry transitions.",
    labelnames=("task_type", "reason"),
)

worker_task_inflight = Gauge(
    "worker_task_inflight",
    "Current number of worker tasks being processed.",
    labelnames=("task_type",),
)

worker_task_concurrency_limit = Gauge(
    "worker_task_concurrency_limit",
    "Configured bounded concurrency limit per worker task type.",
    labelnames=("task_type",),
)

_CONTEXT_ENDPOINTS = {
    "analysis_context",
    "job_posting_context",
}

_CALLBACK_ENDPOINTS = {
    "analysis_complete",
    "analysis_failed",
    "analysis_retry",
    "job_posting_complete",
    "job_posting_failed",
    "job_posting_finalize",
    "job_posting_retry",
}


def task_type_label(task_type: str | None) -> str:
    if task_type in {"analysis", "jobposting", "unknown"}:
        return task_type
    return _TASK_TYPE_LABELS.get((task_type or "").upper(), "unknown")


def reason_label(reason: str | None) -> str:
    return _REASON_LABELS.get((reason or "").upper(), "unknown")


def error_type_label(error_type: str | None) -> str:
    return _REASON_LABELS.get((error_type or "").upper(), "unknown")


def observe_task_queue_wait(task_type: str | None, seconds: float) -> None:
    worker_task_queue_wait_duration_seconds.labels(
        task_type=task_type_label(task_type)
    ).observe(max(seconds, 0.0))


def observe_task_processing(task_type: str | None, outcome: str, seconds: float) -> None:
    worker_task_processing_duration_seconds.labels(
        task_type=task_type_label(task_type),
        outcome=outcome,
    ).observe(max(seconds, 0.0))


def increment_task_retry(task_type: str | None, reason: str | None) -> None:
    worker_task_retry_count_total.labels(
        task_type=task_type_label(task_type),
        reason=reason_label(reason),
    ).inc()


def increment_task_inflight(task_type: str | None) -> None:
    worker_task_inflight.labels(task_type=task_type_label(task_type)).inc()


def decrement_task_inflight(task_type: str | None) -> None:
    worker_task_inflight.labels(task_type=task_type_label(task_type)).dec()


def set_task_concurrency_limit(task_type: str | None, limit: int) -> None:
    worker_task_concurrency_limit.labels(task_type=task_type_label(task_type)).set(max(float(limit), 0.0))


def observe_llm_request(task_type: str | None, operation: str, outcome: str, seconds: float) -> None:
    llm_request_duration_seconds.labels(
        task_type=task_type_label(task_type),
        operation=operation,
        outcome=outcome,
    ).observe(max(seconds, 0.0))


def increment_llm_request_error(task_type: str | None, operation: str, error_type: str | None) -> None:
    worker_llm_request_errors_total.labels(
        task_type=task_type_label(task_type),
        operation=operation,
        error_type=error_type_label(error_type),
    ).inc()


def observe_internal_api(task_type: str | None, endpoint: str, method: str, outcome: str, seconds: float) -> None:
    worker_internal_api_duration_seconds.labels(
        task_type=task_type_label(task_type),
        endpoint=endpoint,
        method=method.upper(),
        outcome=outcome,
    ).observe(max(seconds, 0.0))
    if endpoint in _CONTEXT_ENDPOINTS:
        worker_context_fetch_duration_seconds.labels(
            task_type=task_type_label(task_type),
            endpoint=endpoint,
            outcome=outcome,
        ).observe(max(seconds, 0.0))
    if endpoint in _CALLBACK_ENDPOINTS:
        worker_callback_duration_seconds.labels(
            task_type=task_type_label(task_type),
            endpoint=endpoint,
            outcome=outcome,
        ).observe(max(seconds, 0.0))
