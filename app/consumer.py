from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import socket
import threading
from datetime import datetime, timedelta, timezone
from time import monotonic
from typing import Any, Callable
from uuid import uuid4

import pika

from app.api_client import SpringWorkerApiClient
from app.async_runtime import AsyncConsumerRuntime
from app.concurrency import TaskTypeConcurrencyLimiter, TaskTypeConcurrencyConfig
from app.config import settings
from app.delivery import WorkerDeliveryService
from app.logging_utils import bind_log_context, log_exception, log_info, log_warning
from app.metrics import (
    decrement_task_inflight,
    increment_task_inflight,
    increment_task_retry,
    observe_task_processing,
    observe_task_queue_wait,
    set_task_concurrency_limit,
)
from app.openai_client import AnalysisOpenAiWorker, JobPostingOpenAiWorker
from app.processors import AnalysisTaskProcessor, JobPostingTaskProcessor
from app.recovery import PendingDeliveryStore, TerminalMessageStore
from app.schemas import (
    AnalysisTaskStatusResponse,
    AnalysisLlmResponse,
    AnalysisTaskMessage,
    AnalysisWorkerCompleteRequest,
    AnalysisWorkerContextRequest,
    AnalysisWorkerFailureRequest,
    AnalysisWorkerResultStoreRequest,
    AnalysisWorkerRetryRequest,
    AnalysisWorkerRunningRequest,
    JobPostingClassificationCandidateResponse,
    JobPostingClassificationResultResponse,
    JobPostingExtractResponse,
    JobPostingGenerateResponse,
    JobPostingIngestResponse,
    JobPostingIngestTaskMessage,
    JobPostingWorkerFailureRequest,
    JobPostingWorkerFinalizeRequest,
    JobPostingWorkerResultStoreRequest,
    JobPostingWorkerRetryRequest,
    JobPostingWorkerRunningRequest,
    JobPostingTaskStatusResponse,
    NonRetryableWorkerError,
    PendingDeliveryEntry,
    RetryableWorkerError,
)

logger = logging.getLogger(__name__)
TERMINAL_TASK_STATUSES = {"FAILED", "SUCCEEDED", "SUCCESS", "COMPLETED", "COMPLETE", "CANCELLED"}


class RabbitMqConsumer:
    TERMINAL_TASK_STATUSES = TERMINAL_TASK_STATUSES

    def __init__(
        self,
        *,
        api_client: SpringWorkerApiClient | None = None,
        openai_worker: JobPostingOpenAiWorker | None = None,
        analysis_openai_worker: AnalysisOpenAiWorker | None = None,
        recovery_store: PendingDeliveryStore | None = None,
        terminal_message_store: TerminalMessageStore | None = None,
        sleep_fn: Callable[[float], None] | None = None,
        concurrency_limiter: TaskTypeConcurrencyLimiter | None = None,
    ) -> None:
        self._thread: threading.Thread | None = None
        self._recovery_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._recovery_lock = threading.Lock()
        self._connection: pika.BlockingConnection | None = None
        self._channel: pika.adapters.blocking_connection.BlockingChannel | None = None
        self._api_client = api_client or SpringWorkerApiClient()
        self._openai_worker = openai_worker or JobPostingOpenAiWorker()
        self._analysis_openai_worker = analysis_openai_worker or AnalysisOpenAiWorker()
        self._recovery_store = recovery_store or PendingDeliveryStore(settings.worker_recovery_spool_dir)
        self._terminal_message_store = terminal_message_store or TerminalMessageStore(settings.worker_terminal_message_dir)
        self._sleep_fn = sleep_fn or (lambda seconds: self._stop_event.wait(seconds))
        self._worker_id = f"{socket.gethostname()}:{os.getpid()}"
        self._inflight_task_types: dict[str, str] = {}
        self._inflight_lock = threading.Lock()
        self._delivery_service = WorkerDeliveryService(
            api_client=self._api_client,
            recovery_store=self._recovery_store,
            run_api_call_with_retry=self._run_api_call_with_retry,
            run_api_call_with_retry_async=self._run_api_call_with_retry_async,
            generate_request_id=self._generate_request_id,
            utcnow=self._utcnow,
            entry_log_context_factory=self._entry_log_context,
        )
        self._job_posting_processor = JobPostingTaskProcessor(
            api_client=self._api_client,
            openai_worker=self._openai_worker,
            delivery_service=self._delivery_service,
            worker_id=self._worker_id,
        )
        self._analysis_processor = AnalysisTaskProcessor(
            api_client=self._api_client,
            openai_worker=self._analysis_openai_worker,
            delivery_service=self._delivery_service,
            worker_id=self._worker_id,
        )
        self._concurrency_limiter = concurrency_limiter or TaskTypeConcurrencyLimiter(
            TaskTypeConcurrencyConfig.from_settings(settings)
        )
        self._async_runtime = AsyncConsumerRuntime(self)
        set_task_concurrency_limit("ANALYSIS", self._concurrency_limiter.limit_for("ANALYSIS"))
        set_task_concurrency_limit("JOB_POSTING_INGEST", self._concurrency_limiter.limit_for("JOB_POSTING_INGEST"))

    def start(self) -> None:
        self._async_runtime.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._async_runtime.stop()

    def _run(self) -> None:
        credentials = pika.PlainCredentials(settings.rabbitmq_username, settings.rabbitmq_password)
        parameters = pika.ConnectionParameters(
            host=settings.rabbitmq_host,
            port=settings.rabbitmq_port,
            virtual_host=settings.rabbitmq_vhost,
            credentials=credentials,
            heartbeat=30,
        )

        while not self._stop_event.is_set():
            with bind_log_context(workerId=self._worker_id):
                try:
                    self._connection = pika.BlockingConnection(parameters)
                    self._channel = self._connection.channel()
                    self._channel.confirm_delivery()
                    self._channel.basic_qos(prefetch_count=settings.rabbitmq_prefetch_count)
                    self._recover_pending_deliveries()
                    self._channel.basic_consume(
                        queue=settings.rabbitmq_queue,
                        on_message_callback=self._on_message,
                    )
                    self._channel.basic_consume(
                        queue=settings.analysis_rabbitmq_queue,
                        on_message_callback=self._on_message,
                    )
                    log_info(
                        logger,
                        "worker.consumer.started",
                        "RabbitMQ consumer를 시작합니다.",
                        queues=[settings.rabbitmq_queue, settings.analysis_rabbitmq_queue],
                        prefetchCount=settings.rabbitmq_prefetch_count,
                    )
                    self._channel.start_consuming()
                except Exception:
                    log_exception(
                        logger,
                        "worker.consumer.failed",
                        "RabbitMQ consumer 연결/소비 중 오류가 발생했습니다.",
                    )
                    if self._stop_event.wait(5):
                        break

    def _recovery_loop(self) -> None:
        self._recover_pending_deliveries()
        while not self._stop_event.wait(settings.worker_recovery_poll_interval_seconds):
            self._recover_pending_deliveries()

    def _on_message(self, channel, method, properties, body: bytes) -> None:  # type: ignore[no-untyped-def]
        incoming_context = self._extract_message_context(properties)
        processing_started_at: float | None = None
        slot_lease = None
        try:
            payload = json.loads(body.decode("utf-8"))
            incoming_context = self._extract_message_context(properties, payload)
            message = self._deserialize_message(payload, properties)
        except Exception:
            with bind_log_context(**incoming_context):
                log_exception(
                    logger,
                    "queue.consume.failed",
                    "메시지 역직렬화에 실패했습니다.",
                    deliveryTag=getattr(method, "delivery_tag", None),
                    taskProcessingLatencyMs=0,
                    failureReason="INVALID_PAYLOAD",
                    errorCode="INVALID_PAYLOAD",
                    bodySize=len(body),
                )
            self._ack_message(channel, method.delivery_tag, reason="invalid-payload")
            return

        if not self._register_inflight(message.taskId, message.taskType):
            with bind_log_context(**self._message_log_context(message)):
                log_warning(
                    logger,
                    "queue.consume.failed",
                    "동일 taskId가 이미 처리 중이어서 메시지를 재큐잉합니다.",
                    deliveryTag=getattr(method, "delivery_tag", None),
                    requeue=True,
                    failureReason="TASK_ALREADY_INFLIGHT",
                    errorCode="TASK_ALREADY_INFLIGHT",
                )
                self._nack_message(
                    channel,
                    method.delivery_tag,
                    requeue=True,
                    reason="task-already-inflight",
                    message=message,
                )
            return

        slot_lease = self._concurrency_limiter.try_acquire(message.taskType)
        if slot_lease is None:
            with bind_log_context(**self._message_log_context(message)):
                log_warning(
                    logger,
                    "queue.consume.failed",
                    "task type 동시 처리 상한에 도달해 메시지를 재큐잉합니다.",
                    deliveryTag=getattr(method, "delivery_tag", None),
                    requeue=True,
                    failureReason="TASK_TYPE_LIMIT_REACHED",
                    errorCode="TASK_TYPE_LIMIT_REACHED",
                    concurrencyLimit=self._concurrency_limiter.limit_for(message.taskType),
                )
                self._nack_message(
                    channel,
                    method.delivery_tag,
                    requeue=True,
                    reason="task-type-limit-reached",
                    message=message,
                )
            self._release_inflight(message.taskId)
            return

        try:
            with bind_log_context(**self._message_log_context(message)):
                processing_started_at = monotonic()
                log_info(
                    logger,
                    "queue.consume.started",
                    "RabbitMQ 메시지 소비를 시작합니다.",
                    deliveryTag=getattr(method, "delivery_tag", None),
                    redelivered=getattr(method, "redelivered", False),
                )
                if isinstance(message, AnalysisTaskMessage):
                    self._process_analysis_task(message)
                else:
                    self._process_job_posting_task(message)
                processing_latency_ms = self._elapsed_millis(processing_started_at) or 0
                observe_task_processing(message.taskType, "succeeded", processing_latency_ms / 1000)
                log_info(
                    logger,
                    "queue.consume.completed",
                    "RabbitMQ 메시지 소비가 완료되었습니다.",
                    deliveryTag=getattr(method, "delivery_tag", None),
                    taskProcessingLatencyMs=processing_latency_ms,
                )
                self._ack_message(channel, method.delivery_tag, reason="processed-successfully", message=message)
        except NonRetryableWorkerError as exc:
            with bind_log_context(**self._message_log_context(message, queue_latency_millis=exc.queue_latency_millis)):
                processing_latency_ms = self._elapsed_millis(processing_started_at) or 0
                log_warning(
                    logger,
                    "queue.consume.failed",
                    "비재시도 에러로 작업을 실패 처리합니다.",
                    deliveryTag=getattr(method, "delivery_tag", None),
                    failureReason=exc.failure_reason,
                    errorCode=exc.failure_reason,
                    error=str(exc),
                    taskProcessingLatencyMs=processing_latency_ms,
                )
                outcome = self._handle_non_retryable(channel, method.delivery_tag, message, body, properties, exc)
                observe_task_processing(message.taskType, outcome, processing_latency_ms / 1000)
        except RetryableWorkerError as exc:
            with bind_log_context(**self._message_log_context(message, queue_latency_millis=exc.queue_latency_millis)):
                processing_latency_ms = self._elapsed_millis(processing_started_at) or 0
                log_warning(
                    logger,
                    "queue.consume.failed",
                    "재시도 가능한 에러가 발생했습니다.",
                    deliveryTag=getattr(method, "delivery_tag", None),
                    failureReason=exc.failure_reason,
                    errorCode=exc.failure_reason,
                    error=str(exc),
                    taskProcessingLatencyMs=processing_latency_ms,
                )
                outcome = self._retry_or_fail(channel, method.delivery_tag, properties, message, body, exc)
                observe_task_processing(message.taskType, outcome, processing_latency_ms / 1000)
        except Exception as exc:
            with bind_log_context(**self._message_log_context(message)):
                processing_latency_ms = self._elapsed_millis(processing_started_at) or 0
                log_exception(
                    logger,
                    "queue.consume.failed",
                    "예상치 못한 worker 에러가 발생했습니다.",
                    deliveryTag=getattr(method, "delivery_tag", None),
                    failureReason="INTERNAL_ERROR",
                    errorCode="INTERNAL_ERROR",
                    taskProcessingLatencyMs=processing_latency_ms,
                )
                retryable_exc = RetryableWorkerError(str(exc), failure_reason="INTERNAL_ERROR")
                outcome = self._retry_or_fail(channel, method.delivery_tag, properties, message, body, retryable_exc)
                observe_task_processing(message.taskType, outcome, processing_latency_ms / 1000)
        finally:
            if slot_lease is not None:
                slot_lease.release()
            self._release_inflight(message.taskId)

    def _deserialize_message(
        self,
        payload: dict[str, Any],
        properties: Any | None = None,
    ) -> JobPostingIngestTaskMessage | AnalysisTaskMessage:
        enriched_payload = dict(payload)
        header_context = self._extract_message_context(properties, payload)
        if header_context["requestId"] is not None:
            enriched_payload["requestId"] = header_context["requestId"]
        if header_context["messageId"] is not None:
            enriched_payload["messageId"] = header_context["messageId"]
        if header_context["taskId"] is not None:
            enriched_payload["taskId"] = header_context["taskId"]
        if header_context["taskType"] is not None:
            enriched_payload["taskType"] = header_context["taskType"]
        if header_context["retryCount"] is not None:
            enriched_payload["retryCount"] = header_context["retryCount"]

        task_type = enriched_payload.get("taskType")
        if task_type == "ANALYSIS":
            return AnalysisTaskMessage.model_validate(enriched_payload)
        return JobPostingIngestTaskMessage.model_validate(enriched_payload)

    def _process_job_posting_task(self, message: JobPostingIngestTaskMessage) -> None:
        self._job_posting_processor.process(message)

    def _process_analysis_task(self, message: AnalysisTaskMessage) -> None:
        self._analysis_processor.process(message)

    def _build_job_posting_finalize_request(
        self,
        message: JobPostingIngestTaskMessage,
        *,
        extracted: JobPostingExtractResponse,
        candidates: list[JobPostingClassificationCandidateResponse],
        classification: JobPostingClassificationResultResponse,
        generated: JobPostingGenerateResponse,
    ) -> JobPostingWorkerFinalizeRequest:
        return JobPostingWorkerFinalizeRequest(
            taskId=message.taskId,
            userId=message.userId,
            extracted=extracted,
            candidates=candidates,
            classification=classification,
            generated=generated,
        )

    def _build_analysis_complete_request(
        self,
        message: AnalysisTaskMessage,
        llm_response: AnalysisLlmResponse,
        queue_latency_millis: int | None,
    ) -> AnalysisWorkerCompleteRequest:
        return AnalysisWorkerCompleteRequest(
            userId=message.userId,
            mockApplyId=message.mockApplyId,
            workerId=self._worker_id,
            queueLatencyMillis=queue_latency_millis,
            llmResponse=llm_response,
        )

    def _store_job_posting_result(
        self,
        message: JobPostingIngestTaskMessage,
        finalize_request: JobPostingWorkerFinalizeRequest,
    ) -> None:
        self._delivery_service.store_job_posting_result(message, finalize_request)

    def _store_analysis_result(self, message: AnalysisTaskMessage, llm_response: AnalysisLlmResponse) -> None:
        self._delivery_service.store_analysis_result(message, llm_response)

    def _complete_low_confidence_job_posting(
        self,
        message: JobPostingIngestTaskMessage,
        result: JobPostingIngestResponse,
    ) -> None:
        self._delivery_service.complete_low_confidence_job_posting(message, result)

    def _enqueue_pending_delivery(
        self,
        *,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage,
        delivery_kind: str,
        delivery_path: str,
        payload: dict[str, Any],
        retry_count: int,
    ) -> PendingDeliveryEntry:
        return self._delivery_service.enqueue_pending_delivery(
            message=message,
            delivery_kind=delivery_kind,
            delivery_path=delivery_path,
            payload=payload,
            retry_count=retry_count,
        )

    def _deliver_pending_entry(
        self,
        entry: PendingDeliveryEntry,
        *,
        retry_count: int,
        replayed: bool,
    ) -> bool:
        return self._delivery_service.deliver_pending_entry(
            entry,
            retry_count=retry_count,
            replayed=replayed,
        )

    def _recover_pending_deliveries(self) -> None:
        if not self._recovery_lock.acquire(blocking=False):
            return

        try:
            entries = self._recovery_store.list_entries()
            if not entries:
                return

            with bind_log_context(workerId=self._worker_id):
                log_info(
                    logger,
                    "worker.recovery.scan.started",
                    "recovery spool 재전송을 시작합니다.",
                    pendingCount=len(entries),
                )
            for entry in entries:
                if self._stop_event.is_set():
                    return
                if not self._register_inflight(entry.taskId, entry.taskType):
                    continue
                try:
                    delivered = self._deliver_pending_entry(
                        entry,
                        retry_count=entry.retryCount,
                        replayed=True,
                    )
                    if not delivered:
                        with bind_log_context(**self._entry_log_context(entry)):
                            log_warning(
                                logger,
                                "worker.recovery.replay_pending",
                                "recovery spool 재전송이 아직 완료되지 않았습니다.",
                                nextAttemptAt=entry.nextAttemptAt,
                                lastError=entry.lastError,
                                errorCode="RECOVERY_REPLAY_PENDING",
                            )
                except NonRetryableWorkerError:
                    with bind_log_context(**self._entry_log_context(entry)):
                        log_exception(
                            logger,
                            "worker.recovery.replay_failed",
                            "recovery spool 항목 재전송이 비재시도 오류로 종료되었습니다.",
                            deliveryKind=entry.deliveryKind,
                            errorCode="RECOVERY_REPLAY_FAILED",
                        )
                finally:
                    self._release_inflight(entry.taskId)
        finally:
            self._recovery_lock.release()

    def _run_api_call_with_retry(
        self,
        *,
        operation_name: str,
        task_id: str,
        retry_count: int,
        action: Callable[[], Any],
        on_retryable_error: Callable[[int, str, str | None], None] | None = None,
        replayed: bool = False,
    ) -> Any:
        max_attempts = max(settings.worker_api_retry_max_attempts, 1)
        attempt = 0

        while True:
            attempt += 1
            try:
                return action()
            except RetryableWorkerError as exc:
                next_attempt_at = None
                delay_seconds = 0.0
                if attempt < max_attempts:
                    delay_seconds = self._compute_backoff_seconds(attempt)
                    next_attempt_at = (self._utcnow() + timedelta(seconds=delay_seconds)).isoformat()
                if on_retryable_error is not None:
                    on_retryable_error(attempt, str(exc), next_attempt_at)
                if attempt >= max_attempts:
                    raise
                self._sleep_fn(delay_seconds)
            except NonRetryableWorkerError:
                raise

    async def _run_api_call_with_retry_async(
        self,
        *,
        operation_name: str,
        task_id: str,
        retry_count: int,
        action: Callable[[], Any],
        on_retryable_error: Callable[[int, str, str | None], None] | None = None,
        replayed: bool = False,
    ) -> Any:
        max_attempts = max(settings.worker_api_retry_max_attempts, 1)
        attempt = 0

        while True:
            attempt += 1
            try:
                return await action()
            except RetryableWorkerError as exc:
                next_attempt_at = None
                delay_seconds = 0.0
                if attempt < max_attempts:
                    delay_seconds = self._compute_backoff_seconds(attempt)
                    next_attempt_at = (self._utcnow() + timedelta(seconds=delay_seconds)).isoformat()
                if on_retryable_error is not None:
                    on_retryable_error(attempt, str(exc), next_attempt_at)
                if attempt >= max_attempts:
                    raise
                await asyncio.sleep(delay_seconds)
            except NonRetryableWorkerError:
                raise

    def _compute_backoff_seconds(self, attempt: int) -> float:
        base_delay_seconds = max(settings.worker_api_retry_base_delay_millis, 0) / 1000
        max_delay_seconds = max(settings.worker_api_retry_max_delay_millis, settings.worker_api_retry_base_delay_millis) / 1000
        exponential_delay = min(base_delay_seconds * (2 ** max(attempt - 1, 0)), max_delay_seconds)
        jitter = random.uniform(0, exponential_delay * 0.2 if exponential_delay > 0 else 0.1)
        return min(exponential_delay + jitter, max_delay_seconds)

    def _normalize_classification(
        self,
        classification: JobPostingClassificationResultResponse,
        candidates: list[JobPostingClassificationCandidateResponse],
    ) -> JobPostingClassificationResultResponse:
        return self._job_posting_processor._normalize_classification(classification, candidates)

    def _retry_or_fail(
        self,
        channel,
        delivery_tag: int,
        properties,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage,
        body: bytes,
        error: RetryableWorkerError,
    ) -> str:
        next_retry_count = message.retryCount + 1
        max_retry_count = self._resolve_max_retry_count(message)
        queue_latency_millis = error.queue_latency_millis or self._safe_compute_queue_latency(message.submittedAt)

        with bind_log_context(**self._message_log_context(message, retry_count=next_retry_count, queue_latency_millis=queue_latency_millis)):
            log_warning(
                logger,
                "queue.consume.retry",
                "작업을 retry 경로로 전환합니다.",
                failureReason=error.failure_reason,
                errorCode=error.failure_reason,
                maxRetryCount=max_retry_count,
            )
            if isinstance(message, AnalysisTaskMessage):
                log_warning(
                    logger,
                    "worker.analysis.failed",
                    "analysis 작업이 재시도 경로로 전환되었습니다.",
                    errorCode=error.failure_reason,
                    error=str(error),
                    openaiRequestId=error.openai_request_id,
                )

            if isinstance(message, AnalysisTaskMessage):
                if next_retry_count > max_retry_count:
                    return self._finalize_failed_message(
                        channel,
                        delivery_tag,
                        properties,
                        message,
                        body,
                        error_message=str(error),
                        failure_reason=error.failure_reason,
                        retry_count=next_retry_count,
                        queue_latency_millis=queue_latency_millis,
                        openai_request_id=error.openai_request_id,
                        outcome_reason="retry-exhausted",
                    )

                self._safe_retry_analysis_task(
                    message,
                    str(error),
                    error.failure_reason,
                    next_retry_count,
                    queue_latency_millis,
                    error.openai_request_id,
                )
            else:
                if next_retry_count > max_retry_count:
                    return self._finalize_failed_message(
                        channel,
                        delivery_tag,
                        properties,
                        message,
                        body,
                        error_message=str(error),
                        failure_reason=error.failure_reason,
                        retry_count=next_retry_count,
                        queue_latency_millis=queue_latency_millis,
                        openai_request_id=None,
                        outcome_reason="retry-exhausted",
                    )
                self._safe_retry_job_posting_task(
                    message,
                    str(error),
                    error.failure_reason,
                    next_retry_count,
                    queue_latency_millis,
                )

            republished = message.model_copy(
                update={
                    "retryCount": next_retry_count,
                    "maxRetryCount": max_retry_count,
                }
            ).model_dump(mode="json", exclude_none=True)
            exchange, routing_key = self._resolve_publish_target(message)
            published = self._publish_with_confirm(
                channel,
                exchange=exchange,
                routing_key=routing_key,
                body=json.dumps(republished, ensure_ascii=True),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=2,
                    message_id=message.messageId,
                    headers=self._build_message_headers(message, retry_count=next_retry_count),
                ),
            )
            if published:
                self._ack_message(channel, delivery_tag, reason="republished-for-retry", message=message)
                increment_task_retry(message.taskType, error.failure_reason)
                return "retry"
            self._nack_message(channel, delivery_tag, requeue=True, reason="retry-republish-failed", message=message)
            increment_task_retry(message.taskType, error.failure_reason)
            return "retry"

    def _handle_non_retryable(
        self,
        channel,
        delivery_tag: int,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage,
        body: bytes,
        properties: Any,
        error: NonRetryableWorkerError,
    ) -> str:
        if isinstance(message, AnalysisTaskMessage):
            queue_latency_millis = error.queue_latency_millis or self._safe_compute_queue_latency(message.submittedAt)
            log_warning(
                logger,
                "worker.analysis.failed",
                "analysis 작업이 실패했습니다.",
                errorCode=error.failure_reason,
                error=str(error),
                openaiRequestId=error.openai_request_id,
            )
            return self._finalize_failed_message(
                channel,
                delivery_tag,
                properties,
                message,
                body,
                error_message=str(error),
                failure_reason=error.failure_reason,
                retry_count=message.retryCount,
                queue_latency_millis=queue_latency_millis,
                openai_request_id=error.openai_request_id,
                outcome_reason="non-retryable-error",
            )

        queue_latency_millis = error.queue_latency_millis or self._safe_compute_queue_latency(message.submittedAt)
        return self._finalize_failed_message(
            channel,
            delivery_tag,
            properties,
            message,
            body,
            error_message=str(error),
            failure_reason=error.failure_reason,
            retry_count=message.retryCount,
            queue_latency_millis=queue_latency_millis,
            openai_request_id=None,
            outcome_reason="non-retryable-error",
        )

    def _resolve_publish_target(self, message: JobPostingIngestTaskMessage | AnalysisTaskMessage) -> tuple[str, str]:
        if isinstance(message, AnalysisTaskMessage):
            return settings.analysis_rabbitmq_exchange, settings.analysis_rabbitmq_routing_key
        return settings.rabbitmq_exchange, settings.rabbitmq_routing_key

    def _resolve_max_retry_count(self, message: JobPostingIngestTaskMessage | AnalysisTaskMessage) -> int:
        if isinstance(message, AnalysisTaskMessage):
            return message.maxRetryCount or settings.analysis_max_retry_count
        return message.maxRetryCount or settings.worker_max_retry_count

    def _publish_dlq(
        self,
        channel,
        body: bytes,
        properties: Any,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage,
    ) -> bool:
        routing_key = settings.analysis_rabbitmq_dlq if isinstance(message, AnalysisTaskMessage) else settings.rabbitmq_dlq
        return self._publish_with_confirm(
            channel,
            exchange="",
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                content_type=getattr(properties, "content_type", "application/json"),
                delivery_mode=2,
                message_id=message.messageId,
                headers=self._merge_publish_headers(properties, message),
            ),
        )

    def _finalize_failed_message(
        self,
        channel,
        delivery_tag: int,
        properties: Any,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage,
        body: bytes,
        *,
        error_message: str,
        failure_reason: str,
        retry_count: int,
        queue_latency_millis: int | None,
        openai_request_id: str | None,
        outcome_reason: str,
    ) -> str:
        if self._terminal_message_store.contains(message.taskId, message.messageId):
            with bind_log_context(**self._message_log_context(message, retry_count=retry_count, queue_latency_millis=queue_latency_millis)):
                log_warning(
                    logger,
                    "worker.task.failed",
                    "이미 terminal 처리된 메시지여서 추가 실패 처리와 DLQ 적재를 건너뜁니다.",
                    failureReason=failure_reason,
                    errorCode=failure_reason,
                    outcome=outcome_reason,
                )
            self._ack_message(channel, delivery_tag, reason="already-terminal-message", message=message)
            return "failed"

        if self._is_task_already_terminal(message):
            with bind_log_context(**self._message_log_context(message, retry_count=retry_count, queue_latency_millis=queue_latency_millis)):
                log_warning(
                    logger,
                    "worker.task.failed",
                    "이미 terminal 상태인 task여서 추가 실패 처리와 DLQ 적재를 건너뜁니다.",
                    failureReason=failure_reason,
                    errorCode=failure_reason,
                    outcome=outcome_reason,
                )
            self._ack_message(channel, delivery_tag, reason="already-terminal-task", message=message)
            return "failed"

        with bind_log_context(**self._message_log_context(message, retry_count=retry_count, queue_latency_millis=queue_latency_millis)):
            if isinstance(message, AnalysisTaskMessage):
                self._safe_fail_analysis_task(
                    message,
                    error_message,
                    failure_reason,
                    retry_count,
                    queue_latency_millis,
                    openai_request_id,
                )
            else:
                self._safe_fail_job_posting_task(
                    message,
                    error_message,
                    failure_reason,
                    retry_count,
                    queue_latency_millis,
                )

            published = self._publish_dlq_once(channel, body, properties, message, failure_reason=failure_reason)
            if published:
                self._ack_message(channel, delivery_tag, reason=f"{outcome_reason}-dlq-published", message=message)
                return "failed"
            log_warning(
                logger,
                "worker.task.failed",
                "DLQ publish가 실패했지만 task는 이미 terminal 상태로 반영되어 재큐잉하지 않습니다.",
                failureReason=failure_reason,
                errorCode=failure_reason,
                outcome=outcome_reason,
            )
            self._ack_message(
                channel,
                delivery_tag,
                reason=f"{outcome_reason}-dlq-publish-failed-no-requeue",
                message=message,
            )
            return "failed"

    def _publish_dlq_once(
        self,
        channel,
        body: bytes,
        properties: Any,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage,
        *,
        failure_reason: str,
    ) -> bool:
        if self._terminal_message_store.contains(message.taskId, message.messageId):
            log_warning(
                logger,
                "worker.dlq.skipped",
                "이미 DLQ 적재된 메시지여서 중복 publish를 건너뜁니다.",
                failureReason=failure_reason,
                errorCode=failure_reason,
            )
            return True

        log_info(
            logger,
            "worker.dlq.publish.started",
            "DLQ publish를 시도합니다.",
            failureReason=failure_reason,
            errorCode=failure_reason,
        )
        published = self._publish_dlq(channel, body, properties, message)
        log_info(
            logger,
            "worker.dlq.publish.completed",
            "DLQ publish 결과입니다.",
            published=published,
            failureReason=failure_reason,
            errorCode=failure_reason,
        )
        if not published:
            return False

        try:
            self._terminal_message_store.record(
                task_id=message.taskId,
                request_id=message.requestId,
                message_id=message.messageId,
                task_type=message.taskType,
                retry_count=message.retryCount,
                failure_reason=failure_reason,
            )
        except Exception:
            log_exception(logger, "worker.dlq.ledger_failed", "terminal message ledger 기록에 실패했습니다.")
            return True
        return True

    def _is_task_already_terminal(self, message: JobPostingIngestTaskMessage | AnalysisTaskMessage) -> bool:
        try:
            task_status = self._get_task_status(message)
        except Exception:
            log_exception(
                logger,
                "worker.task.terminal_check_failed",
                "task terminal 상태 확인에 실패했습니다. 보수적으로 실패 처리 경로를 계속 진행합니다.",
            )
            return False

        status = (task_status.status or "").upper()
        if status not in TERMINAL_TASK_STATUSES:
            return False

        log_info(
            logger,
            "worker.task.terminal_confirmed",
            "task terminal 상태를 확인했습니다.",
            status=task_status.status,
            failureReason=task_status.failureReason,
            errorCode=task_status.failureReason,
        )
        return True

    def _get_task_status(
        self,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage,
    ) -> JobPostingTaskStatusResponse | AnalysisTaskStatusResponse:
        if isinstance(message, AnalysisTaskMessage):
            return self._api_client.get_analysis_task(message.taskId)
        return self._api_client.get_job_posting_task(message.taskId)

    def _safe_retry_job_posting_task(
        self,
        message: JobPostingIngestTaskMessage,
        error_message: str,
        failure_reason: str,
        retry_count: int,
        queue_latency_millis: int | None,
    ) -> None:
        with bind_log_context(**self._message_log_context(message, retry_count=retry_count, queue_latency_millis=queue_latency_millis)):
            try:
                log_warning(
                    logger,
                    "worker.task.retry",
                    "job posting 작업을 retry 상태로 반영합니다.",
                    failureReason=failure_reason,
                    errorCode=failure_reason,
                )
                self._api_client.retry_job_posting_task(
                    message.taskId,
                    JobPostingWorkerRetryRequest(
                        errorMessage=error_message,
                        failureReason=failure_reason,
                        retryCount=retry_count,
                        workerId=self._worker_id,
                        queueLatencyMillis=queue_latency_millis,
                    ),
                )
            except Exception:
                log_exception(
                    logger,
                    "worker.task.retry",
                    "Spring API에 job posting retry 상태를 반영하지 못했습니다.",
                    failureReason=failure_reason,
                    errorCode=failure_reason,
                )

    def _safe_fail_job_posting_task(
        self,
        message: JobPostingIngestTaskMessage,
        error_message: str,
        failure_reason: str,
        retry_count: int,
        queue_latency_millis: int | None,
    ) -> None:
        with bind_log_context(**self._message_log_context(message, retry_count=retry_count, queue_latency_millis=queue_latency_millis)):
            try:
                log_warning(
                    logger,
                    "worker.task.failed",
                    "job posting 작업을 failed 상태로 반영합니다.",
                    failureReason=failure_reason,
                    errorCode=failure_reason,
                )
                self._api_client.fail_job_posting_task(
                    message.taskId,
                    JobPostingWorkerFailureRequest(
                        errorMessage=error_message,
                        failureReason=failure_reason,
                        retryCount=retry_count,
                        workerId=self._worker_id,
                        queueLatencyMillis=queue_latency_millis,
                    ),
                )
            except Exception:
                log_exception(
                    logger,
                    "worker.task.failed",
                    "Spring API에 job posting 실패 상태를 반영하지 못했습니다.",
                    failureReason=failure_reason,
                    errorCode=failure_reason,
                )

    def _safe_retry_analysis_task(
        self,
        message: AnalysisTaskMessage,
        error_message: str,
        failure_reason: str,
        retry_count: int,
        queue_latency_millis: int | None,
        openai_request_id: str | None,
    ) -> None:
        with bind_log_context(**self._message_log_context(message, retry_count=retry_count, queue_latency_millis=queue_latency_millis)):
            try:
                log_warning(
                    logger,
                    "worker.task.retry",
                    "analysis 작업을 retry 상태로 반영합니다.",
                    failureReason=failure_reason,
                    errorCode=failure_reason,
                    openaiRequestId=openai_request_id,
                )
                self._api_client.retry_analysis_task(
                    message.taskId,
                    AnalysisWorkerRetryRequest(
                        errorMessage=error_message,
                        failureReason=failure_reason,
                        retryCount=retry_count,
                        workerId=self._worker_id,
                        queueLatencyMillis=queue_latency_millis,
                    ),
                )
            except Exception:
                log_exception(
                    logger,
                    "worker.task.retry",
                    "Spring API에 analysis retry 상태를 반영하지 못했습니다.",
                    failureReason=failure_reason,
                    errorCode=failure_reason,
                    openaiRequestId=openai_request_id,
                )

    def _safe_fail_analysis_task(
        self,
        message: AnalysisTaskMessage,
        error_message: str,
        failure_reason: str,
        retry_count: int,
        queue_latency_millis: int | None,
        openai_request_id: str | None,
    ) -> None:
        with bind_log_context(**self._message_log_context(message, retry_count=retry_count, queue_latency_millis=queue_latency_millis)):
            try:
                log_warning(
                    logger,
                    "worker.task.failed",
                    "analysis 작업을 failed 상태로 반영합니다.",
                    failureReason=failure_reason,
                    errorCode=failure_reason,
                    openaiRequestId=openai_request_id,
                )
                self._api_client.fail_analysis_task(
                    message.taskId,
                    AnalysisWorkerFailureRequest(
                        errorMessage=error_message,
                        failureReason=failure_reason,
                        retryCount=retry_count,
                        workerId=self._worker_id,
                        queueLatencyMillis=queue_latency_millis,
                    ),
                )
            except Exception:
                log_exception(
                    logger,
                    "worker.task.failed",
                    "Spring API에 analysis 실패 상태를 반영하지 못했습니다.",
                    failureReason=failure_reason,
                    errorCode=failure_reason,
                    openaiRequestId=openai_request_id,
                )

    def _compute_queue_latency_millis(self, submitted_at: datetime) -> int:
        submitted_at_datetime = submitted_at
        if submitted_at_datetime.tzinfo is None:
            submitted_at_datetime = submitted_at_datetime.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        latency = (now - submitted_at_datetime.astimezone(timezone.utc)).total_seconds() * 1000
        return max(int(latency), 0)

    def _safe_compute_queue_latency(self, submitted_at: datetime) -> int | None:
        try:
            return self._compute_queue_latency_millis(submitted_at)
        except Exception:
            log_exception(
                logger,
                "worker.queue_latency.failed",
                "queue latency 계산에 실패했습니다.",
                submittedAt=submitted_at,
            )
            return None

    def _ensure_analysis_not_timed_out(self, queue_latency_millis: int) -> None:
        if queue_latency_millis > settings.analysis_queue_timeout_millis:
            raise NonRetryableWorkerError(
                (
                    f"analysis 작업이 queue timeout을 초과했습니다. "
                    f"latency={queue_latency_millis}ms threshold={settings.analysis_queue_timeout_millis}ms"
                ),
                failure_reason="QUEUE_TIMEOUT",
                queue_latency_millis=queue_latency_millis,
            )

    def _publish_with_confirm(
        self,
        channel,
        *,
        exchange: str,
        routing_key: str,
        body: str | bytes,
        properties: pika.BasicProperties,
    ) -> bool:
        try:
            return bool(
                channel.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=body,
                    properties=properties,
                    mandatory=True,
                )
            )
        except Exception:
            with bind_log_context(workerId=self._worker_id):
                log_exception(
                    logger,
                    "worker.queue.publish_failed",
                    "RabbitMQ publish 확인에 실패했습니다.",
                    exchange=exchange,
                    routingKey=routing_key,
                )
            return False

    def _ack_message(
        self,
        channel,
        delivery_tag: int,
        *,
        reason: str,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage | None = None,
    ) -> None:
        channel.basic_ack(delivery_tag=delivery_tag)

    def _nack_message(
        self,
        channel,
        delivery_tag: int,
        *,
        requeue: bool,
        reason: str,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage | None = None,
    ) -> None:
        channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)

    def _register_inflight(self, task_id: str, task_type: str | None) -> bool:
        with self._inflight_lock:
            if task_id in self._inflight_task_types:
                return False
            self._inflight_task_types[task_id] = task_type or "unknown"
            increment_task_inflight(task_type)
            return True

    def _release_inflight(self, task_id: str) -> None:
        with self._inflight_lock:
            task_type = self._inflight_task_types.pop(task_id, None)
        if task_type is not None:
            decrement_task_inflight(task_type)

    def _message_log_context(
        self,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage | None,
        *,
        retry_count: int | None = None,
        queue_latency_millis: int | None = None,
    ) -> dict[str, str | int | None]:
        if message is None:
            return {
                "requestId": None,
                "taskId": None,
                "messageId": None,
                "taskType": None,
                "userId": None,
                "workerId": self._worker_id,
                "retryCount": retry_count,
                "queueLatencyMillis": queue_latency_millis,
            }
        return {
            "requestId": message.requestId,
            "taskId": message.taskId,
            "messageId": message.messageId,
            "taskType": message.taskType,
            "userId": message.userId,
            "workerId": self._worker_id,
            "retryCount": retry_count if retry_count is not None else message.retryCount,
            "queueLatencyMillis": queue_latency_millis,
        }

    def _entry_log_context(self, entry: PendingDeliveryEntry) -> dict[str, str | int | None]:
        return {
            "requestId": entry.requestId,
            "taskId": entry.taskId,
            "messageId": entry.messageId,
            "taskType": entry.taskType,
            "userId": None,
            "workerId": self._worker_id,
            "retryCount": entry.retryCount,
            "queueLatencyMillis": None,
        }

    def _extract_message_context(
        self,
        properties: Any | None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, str | int | None]:
        headers = getattr(properties, "headers", None) or {}
        return {
            "requestId": (
                self._coerce_string(headers.get("x-request-id"))
                or self._coerce_string((payload or {}).get("requestId"))
                or self._generate_request_id()
            ),
            "taskId": self._coerce_string(headers.get("x-task-id")) or self._coerce_string((payload or {}).get("taskId")),
            "messageId": (
                self._coerce_string(headers.get("x-message-id"))
                or self._coerce_string(getattr(properties, "message_id", None))
                or self._coerce_string((payload or {}).get("messageId"))
            ),
            "taskType": self._coerce_string(headers.get("x-task-type")) or self._coerce_string((payload or {}).get("taskType")),
            "userId": self._coerce_int((payload or {}).get("userId")),
            "workerId": self._worker_id,
            "retryCount": self._coerce_int(headers.get("x-retry-count"), (payload or {}).get("retryCount")),
            "queueLatencyMillis": None,
        }

    def _build_message_headers(
        self,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage,
        *,
        retry_count: int | None = None,
    ) -> dict[str, Any]:
        request_id = message.requestId or self._generate_request_id()
        message.requestId = request_id
        headers = {
            "x-request-id": request_id,
            "x-task-id": message.taskId,
            "x-task-type": message.taskType,
            "x-retry-count": retry_count if retry_count is not None else message.retryCount,
            "x-message-id": message.messageId,
        }
        return headers

    def _merge_publish_headers(
        self,
        properties: Any,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage,
    ) -> dict[str, Any]:
        headers = dict(getattr(properties, "headers", None) or {})
        headers.update(self._build_message_headers(message))
        return headers

    def _coerce_string(self, value: Any) -> str | None:
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="replace")
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        if value is None:
            return None
        return str(value)

    def _coerce_int(self, value: Any, fallback: Any = None) -> int | None:
        for candidate in (value, fallback):
            if candidate is None:
                continue
            if isinstance(candidate, bytes):
                candidate = candidate.decode("utf-8", errors="replace")
            try:
                return int(candidate)
            except (TypeError, ValueError):
                continue
        return None

    def _generate_request_id(self) -> str:
        return f"worker-{uuid4()}"

    def _elapsed_millis(self, started_at: float | None) -> int | None:
        if started_at is None:
            return None
        return max(int((monotonic() - started_at) * 1000), 0)

    def _observe_processing_metric(self, task_type: str | None, outcome: str, processing_latency_ms: int) -> None:
        observe_task_processing(task_type, outcome, processing_latency_ms / 1000)

    def _now_monotonic(self) -> float:
        return monotonic()

    def _utcnow(self) -> datetime:
        return datetime.now(timezone.utc)
