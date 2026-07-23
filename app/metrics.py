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
)

_TASK_TYPE_LABELS = {
    "ANALYSIS": "analysis",
    "JOB_POSTING_INGEST": "jobposting",
}

_REASON_LABELS = {
    "RATE_LIMIT": "rate_limit",
    "QUEUE_TIMEOUT": "queue_timeout",
    "OPENAI_TIMEOUT": "openai_timeout",
    "VALIDATION_ERROR": "validation_error",
    "INTERNAL_ERROR": "internal_error",
}

worker_queue_wait_duration = Histogram(
    "worker_queue_wait_duration",
    "Time from message enqueue to worker processing start in seconds.",
    labelnames=("task_type",),
    buckets=DURATION_BUCKETS,
)

worker_processing_duration = Histogram(
    "worker_processing_duration",
    "Time from worker processing start to succeeded/failed/retry outcome in seconds.",
    labelnames=("task_type", "outcome"),
    buckets=DURATION_BUCKETS,
)

worker_context_fetch_duration = Histogram(
    "worker_context_fetch_duration",
    "Time spent fetching worker context from the Spring internal API in seconds.",
    labelnames=("task_type", "outcome"),
    buckets=DURATION_BUCKETS,
)

worker_callback_duration = Histogram(
    "worker_callback_duration",
    "Time spent reflecting worker completion/failure/retry callbacks to the Spring internal API in seconds.",
    labelnames=("task_type", "outcome"),
    buckets=DURATION_BUCKETS,
)

llm_request_duration = Histogram(
    "llm_request_duration",
    "Latency per OpenAI request in seconds.",
    labelnames=("operation", "outcome"),
    buckets=DURATION_BUCKETS,
)

worker_retry_count = Counter(
    "worker_retry_count",
    "Number of worker retry transitions.",
    labelnames=("task_type", "reason"),
)

worker_inflight_tasks = Gauge(
    "worker_inflight_tasks",
    "Current number of inflight worker tasks.",
    labelnames=("task_type",),
)

retrieval_duration = Histogram(
    "retrieval_duration",
    "Time spent fetching downstream retrieval data needed by a worker task in seconds.",
    labelnames=("task_type", "outcome"),
    buckets=DURATION_BUCKETS,
)

result_store_duration = Histogram(
    "result_store_duration",
    "Time spent storing worker results to the Spring internal API in seconds.",
    labelnames=("task_type", "outcome"),
    buckets=DURATION_BUCKETS,
)


def task_type_label(task_type: str | None) -> str:
    if task_type in {"analysis", "jobposting", "unknown"}:
        return task_type
    return _TASK_TYPE_LABELS.get((task_type or "").upper(), "unknown")


def reason_label(reason: str | None) -> str:
    return _REASON_LABELS.get((reason or "").upper(), "unknown")


def observe_queue_wait(task_type: str | None, seconds: float) -> None:
    worker_queue_wait_duration.labels(task_type=task_type_label(task_type)).observe(max(seconds, 0.0))


def observe_processing(task_type: str | None, outcome: str, seconds: float) -> None:
    worker_processing_duration.labels(
        task_type=task_type_label(task_type),
        outcome=outcome,
    ).observe(max(seconds, 0.0))


def observe_context_fetch(task_type: str | None, outcome: str, seconds: float) -> None:
    worker_context_fetch_duration.labels(
        task_type=task_type_label(task_type),
        outcome=outcome,
    ).observe(max(seconds, 0.0))


def observe_callback(task_type: str | None, outcome: str, seconds: float) -> None:
    worker_callback_duration.labels(
        task_type=task_type_label(task_type),
        outcome=outcome,
    ).observe(max(seconds, 0.0))


def observe_llm_request(operation: str, outcome: str, seconds: float) -> None:
    llm_request_duration.labels(
        operation=operation,
        outcome=outcome,
    ).observe(max(seconds, 0.0))


def increment_retry(task_type: str | None, reason: str | None) -> None:
    worker_retry_count.labels(
        task_type=task_type_label(task_type),
        reason=reason_label(reason),
    ).inc()


def increment_inflight(task_type: str | None) -> None:
    worker_inflight_tasks.labels(task_type=task_type_label(task_type)).inc()


def decrement_inflight(task_type: str | None) -> None:
    worker_inflight_tasks.labels(task_type=task_type_label(task_type)).dec()


def observe_retrieval(task_type: str | None, outcome: str, seconds: float) -> None:
    retrieval_duration.labels(
        task_type=task_type_label(task_type),
        outcome=outcome,
    ).observe(max(seconds, 0.0))


def observe_result_store(task_type: str | None, outcome: str, seconds: float) -> None:
    result_store_duration.labels(
        task_type=task_type_label(task_type),
        outcome=outcome,
    ).observe(max(seconds, 0.0))
