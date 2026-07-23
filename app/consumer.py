from __future__ import annotations

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
from app.config import settings
from app.logging_utils import bind_log_context, log_exception, log_info, log_warning
from app.metrics import (
    decrement_inflight,
    increment_inflight,
    increment_retry,
    observe_callback,
    observe_context_fetch,
    observe_processing,
    observe_queue_wait,
    observe_result_store,
    observe_retrieval,
)
from app.openai_client import AnalysisOpenAiWorker, JobPostingOpenAiWorker
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
    def __init__(
        self,
        *,
        api_client: SpringWorkerApiClient | None = None,
        openai_worker: JobPostingOpenAiWorker | None = None,
        analysis_openai_worker: AnalysisOpenAiWorker | None = None,
        recovery_store: PendingDeliveryStore | None = None,
        terminal_message_store: TerminalMessageStore | None = None,
        sleep_fn: Callable[[float], None] | None = None,
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

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="rabbitmq-consumer", daemon=True)
        self._thread.start()
        self._recovery_thread = threading.Thread(target=self._recovery_loop, name="delivery-recovery", daemon=True)
        self._recovery_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._channel and self._channel.is_open:
            with bind_log_context(workerId=self._worker_id):
                try:
                    self._channel.stop_consuming()
                except Exception:
                    log_exception(
                        logger,
                        "worker.consumer.stop_failed",
                        "RabbitMQ consuming stop 중 오류가 발생했지만 종료를 계속합니다.",
                    )
        if self._connection and self._connection.is_open:
            with bind_log_context(workerId=self._worker_id):
                try:
                    self._connection.close()
                except Exception:
                    log_exception(
                        logger,
                        "worker.consumer.connection_close_failed",
                        "RabbitMQ connection close 중 오류가 발생했지만 종료를 계속합니다.",
                    )

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
                observe_processing(message.taskType, "succeeded", processing_latency_ms / 1000)
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
                observe_processing(message.taskType, outcome, processing_latency_ms / 1000)
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
                observe_processing(message.taskType, outcome, processing_latency_ms / 1000)
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
                observe_processing(message.taskType, outcome, processing_latency_ms / 1000)
        finally:
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
        if message.taskType != "JOB_POSTING_INGEST":
            raise NonRetryableWorkerError(f"지원하지 않는 taskType입니다. taskType={message.taskType}")

        task_started_at = monotonic()
        queue_latency_millis = self._safe_compute_queue_latency(message.submittedAt)
        if queue_latency_millis is not None:
            observe_queue_wait(message.taskType, queue_latency_millis / 1000)
        with bind_log_context(queueLatencyMillis=queue_latency_millis):
            log_info(
                logger,
                "worker.task.started",
                "job posting 작업을 시작합니다.",
            )
            self._api_client.mark_job_posting_running(
                message.taskId,
                JobPostingWorkerRunningRequest(
                    workerId=self._worker_id,
                    retryCount=message.retryCount,
                    submittedAt=message.submittedAt,
                ),
            )

            context_started_at = monotonic()
            try:
                context = self._api_client.get_context(message.userId, message.imageObjectKey)
            except Exception:
                observe_context_fetch(message.taskType, "failed", self._elapsed_seconds(context_started_at))
                raise
            context_fetch_latency_ms = self._elapsed_millis(context_started_at)
            observe_context_fetch(message.taskType, "succeeded", self._elapsed_seconds(context_started_at))
            log_info(
                logger,
                "worker.context.fetch.completed",
                "job posting context 조회가 완료되었습니다.",
                latencyMs=context_fetch_latency_ms,
                contextFetchLatencyMs=context_fetch_latency_ms,
            )

            extracted = self._openai_worker.extract(message.rawText, context.imageUrl)
            candidates_started_at = monotonic()
            try:
                candidates = self._api_client.get_candidates(extracted)
            except Exception:
                observe_retrieval(message.taskType, "failed", self._elapsed_seconds(candidates_started_at))
                raise
            candidate_fetch_latency_ms = self._elapsed_millis(candidates_started_at)
            observe_retrieval(message.taskType, "succeeded", self._elapsed_seconds(candidates_started_at))
            log_info(
                logger,
                "worker.candidates.fetch.completed",
                "job posting candidate 조회가 완료되었습니다.",
                latencyMs=candidate_fetch_latency_ms,
                candidateFetchLatencyMs=candidate_fetch_latency_ms,
                candidateCount=len(candidates),
            )
            if not candidates:
                raise NonRetryableWorkerError(
                    "소분류 후보를 찾을 수 없습니다.",
                    failure_reason="VALIDATION_ERROR",
                    queue_latency_millis=queue_latency_millis,
                )

            classification = self._normalize_classification(
                self._openai_worker.classify(extracted, candidates),
                candidates,
            )
            if classification.confidence < settings.job_posting_confidence_threshold:
                result = JobPostingIngestResponse(
                    savedToDatabase=False,
                    message="소분류 분류 confidence가 낮아 저장을 보류했습니다.",
                    extracted=extracted,
                    candidates=candidates,
                    classification=classification,
                    generated=None,
                    saved=None,
                )
                delivery_started_at = monotonic()
                log_info(
                    logger,
                    "worker.delivery.started",
                    "저신뢰도 분기 complete 전달을 시작합니다.",
                    deliveryKind="JOB_POSTING_COMPLETE",
                )
                self._complete_low_confidence_job_posting(message, result)
                complete_delivery_latency_ms = self._elapsed_millis(delivery_started_at)
                log_info(
                    logger,
                    "worker.delivery.completed",
                    "저신뢰도 분기 complete 전달이 완료되었습니다.",
                    deliveryKind="JOB_POSTING_COMPLETE",
                    latencyMs=complete_delivery_latency_ms,
                    completeDeliveryLatencyMs=complete_delivery_latency_ms,
                )
                log_info(
                    logger,
                    "worker.task.completed",
                    "job posting 작업이 완료되었습니다.",
                    confidence=classification.confidence,
                    taskProcessingLatencyMs=self._elapsed_millis(task_started_at),
                    contextFetchLatencyMs=context_fetch_latency_ms,
                    candidateFetchLatencyMs=candidate_fetch_latency_ms,
                    completeDeliveryLatencyMs=complete_delivery_latency_ms,
                )
                return

            generated = self._openai_worker.generate(extracted, classification)
            finalize_request = self._build_job_posting_finalize_request(
                message,
                extracted=extracted,
                candidates=candidates,
                classification=classification,
                generated=generated,
            )
            result_store_started_at = monotonic()
            try:
                self._store_job_posting_result(message, finalize_request)
            except Exception:
                observe_result_store(message.taskType, "failed", self._elapsed_seconds(result_store_started_at))
                raise
            result_store_latency_ms = self._elapsed_millis(result_store_started_at)
            observe_result_store(message.taskType, "succeeded", self._elapsed_seconds(result_store_started_at))
            log_info(
                logger,
                "worker.result.store.completed",
                "job posting result 저장이 완료되었습니다.",
                latencyMs=result_store_latency_ms,
                resultStoreLatencyMs=result_store_latency_ms,
            )
            pending_entry = self._enqueue_pending_delivery(
                message=message,
                delivery_kind="JOB_POSTING_FINALIZE",
                delivery_path="/api/internal/worker/job-postings/ingest/finalize",
                payload=finalize_request.model_dump(mode="json"),
                retry_count=message.retryCount,
            )
            delivery_started_at = monotonic()
            delivered = self._deliver_pending_entry(
                pending_entry,
                retry_count=message.retryCount,
                replayed=False,
            )
            finalize_delivery_latency_ms = self._elapsed_millis(delivery_started_at)
            if not delivered:
                log_warning(
                    logger,
                    "worker.delivery.deferred",
                    "job posting finalize 전달이 보류되어 recovery spool에 남겼습니다.",
                    deliveryKind=pending_entry.deliveryKind,
                    taskProcessingLatencyMs=self._elapsed_millis(task_started_at),
                    finalizeDeliveryLatencyMs=finalize_delivery_latency_ms,
                )
                return
            log_info(
                logger,
                "worker.task.completed",
                "job posting 작업이 완료되었습니다.",
                taskProcessingLatencyMs=self._elapsed_millis(task_started_at),
                contextFetchLatencyMs=context_fetch_latency_ms,
                candidateFetchLatencyMs=candidate_fetch_latency_ms,
                resultStoreLatencyMs=result_store_latency_ms,
                finalizeDeliveryLatencyMs=finalize_delivery_latency_ms,
            )

    def _process_analysis_task(self, message: AnalysisTaskMessage) -> None:
        task_started_at = monotonic()
        queue_latency_millis = self._compute_queue_latency_millis(message.submittedAt)
        observe_queue_wait(message.taskType, queue_latency_millis / 1000)
        self._ensure_analysis_not_timed_out(queue_latency_millis)

        with bind_log_context(queueLatencyMillis=queue_latency_millis):
            log_info(
                logger,
                "worker.task.started",
                "analysis 작업을 시작합니다.",
            )
            self._api_client.mark_analysis_running(
                message.taskId,
                AnalysisWorkerRunningRequest(
                    workerId=self._worker_id,
                    retryCount=message.retryCount,
                    submittedAt=message.submittedAt,
                ),
            )
            context_started_at = monotonic()
            try:
                context = self._api_client.get_analysis_context(
                    AnalysisWorkerContextRequest(
                        taskId=message.taskId,
                        userId=message.userId,
                        mockApplyId=message.mockApplyId,
                    )
                )
            except Exception:
                observe_context_fetch(message.taskType, "failed", self._elapsed_seconds(context_started_at))
                raise
            context_fetch_latency_ms = self._elapsed_millis(context_started_at)
            observe_context_fetch(message.taskType, "succeeded", self._elapsed_seconds(context_started_at))
            log_info(
                logger,
                "worker.context.fetch.completed",
                "analysis context 조회가 완료되었습니다.",
                latencyMs=context_fetch_latency_ms,
                contextFetchLatencyMs=context_fetch_latency_ms,
            )

            analysis_started_at = monotonic()
            log_info(
                logger,
                "worker.analysis.started",
                "analysis LLM 처리를 시작합니다.",
            )
            try:
                llm_response, openai_request_id = self._analysis_openai_worker.analyze(context)
            except (RetryableWorkerError, NonRetryableWorkerError) as exc:
                log_warning(
                    logger,
                    "worker.analysis.failed",
                    "analysis LLM 처리에 실패했습니다.",
                    latencyMs=self._elapsed_millis(analysis_started_at),
                    errorCode=exc.failure_reason,
                    error=str(exc),
                    openaiRequestId=exc.openai_request_id,
                )
                raise
            log_info(
                logger,
                "worker.analysis.completed",
                "analysis LLM 처리가 완료되었습니다.",
                latencyMs=self._elapsed_millis(analysis_started_at),
                openaiRequestId=openai_request_id,
            )
            complete_request = self._build_analysis_complete_request(message, llm_response, queue_latency_millis)
            result_store_started_at = monotonic()
            try:
                self._store_analysis_result(message, llm_response)
            except Exception:
                observe_result_store(message.taskType, "failed", self._elapsed_seconds(result_store_started_at))
                raise
            result_store_latency_ms = self._elapsed_millis(result_store_started_at)
            observe_result_store(message.taskType, "succeeded", self._elapsed_seconds(result_store_started_at))
            log_info(
                logger,
                "worker.result.store.completed",
                "analysis result 저장이 완료되었습니다.",
                latencyMs=result_store_latency_ms,
                resultStoreLatencyMs=result_store_latency_ms,
            )
            pending_entry = self._enqueue_pending_delivery(
                message=message,
                delivery_kind="ANALYSIS_COMPLETE",
                delivery_path=f"/api/internal/worker/analysis/tasks/{message.taskId}/complete",
                payload=complete_request.model_dump(mode="json"),
                retry_count=message.retryCount,
            )
            delivery_started_at = monotonic()
            delivered = self._deliver_pending_entry(
                pending_entry,
                retry_count=message.retryCount,
                replayed=False,
            )
            complete_delivery_latency_ms = self._elapsed_millis(delivery_started_at)
            if not delivered:
                log_warning(
                    logger,
                    "worker.delivery.deferred",
                    "analysis complete 전달이 보류되어 recovery spool에 남겼습니다.",
                    deliveryKind=pending_entry.deliveryKind,
                    openaiRequestId=openai_request_id,
                    taskProcessingLatencyMs=self._elapsed_millis(task_started_at),
                    completeDeliveryLatencyMs=complete_delivery_latency_ms,
                )
                return
            log_info(
                logger,
                "worker.task.completed",
                "analysis 작업이 완료되었습니다.",
                openaiRequestId=openai_request_id,
                taskProcessingLatencyMs=self._elapsed_millis(task_started_at),
                contextFetchLatencyMs=context_fetch_latency_ms,
                resultStoreLatencyMs=result_store_latency_ms,
                completeDeliveryLatencyMs=complete_delivery_latency_ms,
            )

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
        request = JobPostingWorkerResultStoreRequest(
            userId=message.userId,
            result=finalize_request,
        )
        log_info(logger, "worker.result.store.started", "job posting result 저장을 시작합니다.")
        self._run_api_call_with_retry(
            operation_name="job posting result 저장",
            task_id=message.taskId,
            retry_count=message.retryCount,
            action=lambda: self._api_client.store_job_posting_result(message.taskId, request),
        )

    def _store_analysis_result(self, message: AnalysisTaskMessage, llm_response: AnalysisLlmResponse) -> None:
        request = AnalysisWorkerResultStoreRequest(
            userId=message.userId,
            mockApplyId=message.mockApplyId,
            llmResponse=llm_response,
        )
        log_info(logger, "worker.result.store.started", "analysis result 저장을 시작합니다.")
        self._run_api_call_with_retry(
            operation_name="analysis result 저장",
            task_id=message.taskId,
            retry_count=message.retryCount,
            action=lambda: self._api_client.store_analysis_result(message.taskId, request),
        )

    def _complete_low_confidence_job_posting(
        self,
        message: JobPostingIngestTaskMessage,
        result: JobPostingIngestResponse,
    ) -> None:
        response = self._observe_callback_operation(
            message.taskType,
            "succeeded",
            lambda: self._run_api_call_with_retry(
                operation_name="job posting complete",
                task_id=message.taskId,
                retry_count=message.retryCount,
                action=lambda: self._api_client.complete_task(message.taskId, result),
            ),
        )
        if response is None:
            raise RetryableWorkerError("job posting complete 응답이 없습니다.")

    def _enqueue_pending_delivery(
        self,
        *,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage,
        delivery_kind: str,
        delivery_path: str,
        payload: dict[str, Any],
        retry_count: int,
    ) -> PendingDeliveryEntry:
        try:
            entry = PendingDeliveryEntry(
                taskId=message.taskId,
                requestId=message.requestId,
                messageId=message.messageId,
                taskType=message.taskType,
                retryCount=retry_count,
                deliveryKind=delivery_kind,
                deliveryPath=delivery_path,
                payload=payload,
                storedAt=self._utcnow().isoformat(),
            )
            self._recovery_store.upsert(entry)
            log_info(
                logger,
                "worker.recovery.spool.stored",
                "pending delivery를 recovery spool에 기록했습니다.",
                deliveryKind=delivery_kind,
            )
            return entry
        except Exception as exc:
            raise RetryableWorkerError(f"recovery spool 기록 실패: {exc}") from exc

    def _deliver_pending_entry(
        self,
        entry: PendingDeliveryEntry,
        *,
        retry_count: int,
        replayed: bool,
    ) -> bool:
        if not entry.requestId:
            entry.requestId = self._generate_request_id()
            self._recovery_store.upsert(entry)
        with bind_log_context(**self._entry_log_context(entry)):
            log_info(
                logger,
                "worker.delivery.started",
                "pending delivery 전달을 시작합니다.",
                deliveryKind=entry.deliveryKind,
                replayed=replayed,
            )

            def action() -> None:
                if entry.deliveryKind == "ANALYSIS_COMPLETE":
                    request = AnalysisWorkerCompleteRequest.model_validate(entry.payload)
                    self._api_client.complete_analysis_task(entry.taskId, request)
                    return
                if entry.deliveryKind == "JOB_POSTING_FINALIZE":
                    request = JobPostingWorkerFinalizeRequest.model_validate(entry.payload)
                    self._api_client.finalize(request)
                    return
                raise NonRetryableWorkerError(f"지원하지 않는 deliveryKind입니다. deliveryKind={entry.deliveryKind}")

            def on_retryable_error(attempt: int, error_message: str, next_attempt_at: str | None) -> None:
                entry.attemptCount = attempt
                entry.lastError = error_message
                entry.nextAttemptAt = next_attempt_at
                self._recovery_store.upsert(entry)

            try:
                self._observe_callback_operation(
                    entry.taskType,
                    "succeeded",
                    lambda: self._run_api_call_with_retry(
                        operation_name=f"{entry.deliveryKind} 전달",
                        task_id=entry.taskId,
                        retry_count=retry_count,
                        action=action,
                        on_retryable_error=on_retryable_error,
                        replayed=replayed,
                    ),
                )
            except RetryableWorkerError:
                return False
            except NonRetryableWorkerError:
                self._recovery_store.delete(entry.taskId, entry.deliveryKind)
                raise

            self._recovery_store.delete(entry.taskId, entry.deliveryKind)
            log_info(
                logger,
                "worker.recovery.replayed" if replayed else "worker.delivery.completed",
                "pending delivery 전달이 완료되었습니다.",
                deliveryKind=entry.deliveryKind,
                replayed=replayed,
            )
            return True

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
        for candidate in candidates:
            if candidate.detailClassificationId == classification.detailClassificationId:
                return JobPostingClassificationResultResponse(
                    detailClassificationId=candidate.detailClassificationId,
                    detailClassificationName=candidate.detailClassificationName,
                    middleClassificationName=candidate.middleClassificationName,
                    bigClassificationName=candidate.bigClassificationName,
                    reason=classification.reason,
                    confidence=classification.confidence,
                )
        top = candidates[0]
        return JobPostingClassificationResultResponse(
            detailClassificationId=top.detailClassificationId,
            detailClassificationName=top.detailClassificationName,
            middleClassificationName=top.middleClassificationName,
            bigClassificationName=top.bigClassificationName,
            reason=classification.reason or "분류 결과를 후보와 정규화하는 과정에서 1순위 후보를 사용했습니다.",
            confidence=classification.confidence,
        )

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
                increment_retry(message.taskType, error.failure_reason)
                return "retry"
            self._nack_message(channel, delivery_tag, requeue=True, reason="retry-republish-failed", message=message)
            increment_retry(message.taskType, error.failure_reason)
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
                self._observe_callback_operation(
                    message.taskType,
                    "retry",
                    lambda: self._api_client.retry_job_posting_task(
                        message.taskId,
                        JobPostingWorkerRetryRequest(
                            errorMessage=error_message,
                            failureReason=failure_reason,
                            retryCount=retry_count,
                            workerId=self._worker_id,
                            queueLatencyMillis=queue_latency_millis,
                        ),
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
                self._observe_callback_operation(
                    message.taskType,
                    "failed",
                    lambda: self._api_client.fail_job_posting_task(
                        message.taskId,
                        JobPostingWorkerFailureRequest(
                            errorMessage=error_message,
                            failureReason=failure_reason,
                            retryCount=retry_count,
                            workerId=self._worker_id,
                            queueLatencyMillis=queue_latency_millis,
                        ),
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
                self._observe_callback_operation(
                    message.taskType,
                    "retry",
                    lambda: self._api_client.retry_analysis_task(
                        message.taskId,
                        AnalysisWorkerRetryRequest(
                            errorMessage=error_message,
                            failureReason=failure_reason,
                            retryCount=retry_count,
                            workerId=self._worker_id,
                            queueLatencyMillis=queue_latency_millis,
                        ),
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
                self._observe_callback_operation(
                    message.taskType,
                    "failed",
                    lambda: self._api_client.fail_analysis_task(
                        message.taskId,
                        AnalysisWorkerFailureRequest(
                            errorMessage=error_message,
                            failureReason=failure_reason,
                            retryCount=retry_count,
                            workerId=self._worker_id,
                            queueLatencyMillis=queue_latency_millis,
                        ),
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
            increment_inflight(task_type)
            return True

    def _release_inflight(self, task_id: str) -> None:
        with self._inflight_lock:
            task_type = self._inflight_task_types.pop(task_id, None)
        if task_type is not None:
            decrement_inflight(task_type)

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

    def _elapsed_seconds(self, started_at: float | None) -> float:
        if started_at is None:
            return 0.0
        return max(monotonic() - started_at, 0.0)

    def _observe_callback_operation(
        self,
        task_type: str | None,
        outcome: str,
        action: Callable[[], Any],
    ) -> Any:
        started_at = monotonic()
        try:
            return action()
        finally:
            observe_callback(task_type, outcome, self._elapsed_seconds(started_at))

    def _utcnow(self) -> datetime:
        return datetime.now(timezone.utc)
