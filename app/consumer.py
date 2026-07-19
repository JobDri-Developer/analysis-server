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

import pika

from app.api_client import SpringWorkerApiClient
from app.config import settings
from app.openai_client import AnalysisOpenAiWorker, JobPostingOpenAiWorker
from app.recovery import PendingDeliveryStore
from app.schemas import (
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
    NonRetryableWorkerError,
    PendingDeliveryEntry,
    RetryableWorkerError,
)

logger = logging.getLogger(__name__)


class RabbitMqConsumer:
    def __init__(
        self,
        *,
        api_client: SpringWorkerApiClient | None = None,
        openai_worker: JobPostingOpenAiWorker | None = None,
        analysis_openai_worker: AnalysisOpenAiWorker | None = None,
        recovery_store: PendingDeliveryStore | None = None,
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
        self._sleep_fn = sleep_fn or (lambda seconds: self._stop_event.wait(seconds))
        self._worker_id = f"{socket.gethostname()}:{os.getpid()}"
        self._inflight_task_ids: set[str] = set()
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
            try:
                self._channel.stop_consuming()
            except Exception:
                logger.exception("RabbitMQ consuming stop 중 오류가 발생했습니다.")
        if self._connection and self._connection.is_open:
            self._connection.close()

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
                logger.info(
                    "RabbitMQ consumer started. queues=%s,%s",
                    settings.rabbitmq_queue,
                    settings.analysis_rabbitmq_queue,
                    extra={"workerId": self._worker_id},
                )
                self._channel.start_consuming()
            except Exception:
                logger.exception(
                    "RabbitMQ consumer 연결/소비 중 오류가 발생했습니다.",
                    extra={"workerId": self._worker_id},
                )
                if self._stop_event.wait(5):
                    break

    def _recovery_loop(self) -> None:
        self._recover_pending_deliveries()
        while not self._stop_event.wait(settings.worker_recovery_poll_interval_seconds):
            self._recover_pending_deliveries()

    def _on_message(self, channel, method, properties, body: bytes) -> None:  # type: ignore[no-untyped-def]
        try:
            payload = json.loads(body.decode("utf-8"))
            message = self._deserialize_message(payload)
        except Exception:
            logger.exception("메시지 역직렬화에 실패했습니다.")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        if not self._register_inflight(message.taskId):
            logger.warning(
                "동일 taskId가 이미 처리 중이어서 메시지를 재큐잉합니다.",
                extra=self._log_context(message),
            )
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return

        try:
            if isinstance(message, AnalysisTaskMessage):
                self._process_analysis_task(message)
            else:
                self._process_job_posting_task(message)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except NonRetryableWorkerError as exc:
            logger.warning(
                "비재시도 에러로 작업을 실패 처리합니다. error=%s",
                exc,
                extra=self._log_context(message),
            )
            self._handle_non_retryable(channel, method.delivery_tag, message, body, properties, exc)
        except RetryableWorkerError as exc:
            logger.warning(
                "재시도 가능한 에러가 발생했습니다. error=%s",
                exc,
                extra=self._log_context(message),
            )
            self._retry_or_fail(channel, method.delivery_tag, properties, message, body, exc)
        except Exception as exc:
            logger.exception(
                "예상치 못한 worker 에러가 발생했습니다.",
                extra=self._log_context(message),
            )
            retryable_exc = RetryableWorkerError(str(exc), failure_reason="INTERNAL_ERROR")
            self._retry_or_fail(channel, method.delivery_tag, properties, message, body, retryable_exc)
        finally:
            self._release_inflight(message.taskId)

    def _deserialize_message(self, payload: dict[str, Any]) -> JobPostingIngestTaskMessage | AnalysisTaskMessage:
        task_type = payload.get("taskType")
        if task_type == "ANALYSIS":
            return AnalysisTaskMessage.model_validate(payload)
        return JobPostingIngestTaskMessage.model_validate(payload)

    def _process_job_posting_task(self, message: JobPostingIngestTaskMessage) -> None:
        if message.taskType != "JOB_POSTING_INGEST":
            raise NonRetryableWorkerError(f"지원하지 않는 taskType입니다. taskType={message.taskType}")

        queue_latency_millis = self._safe_compute_queue_latency(message.submittedAt)
        logger.info(
            "job posting 작업을 running 상태로 반영합니다. queueLatencyMillis=%s",
            queue_latency_millis,
            extra=self._log_context(message),
        )
        self._api_client.mark_job_posting_running(
            message.taskId,
            JobPostingWorkerRunningRequest(
                workerId=self._worker_id,
                retryCount=message.retryCount,
                submittedAt=message.submittedAt,
            ),
        )

        context = self._api_client.get_context(message.userId, message.imageObjectKey)
        openai_started_at = monotonic()
        extracted = self._openai_worker.extract(message.rawText, context.imageUrl)
        candidates = self._api_client.get_candidates(extracted)
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
            openai_latency_millis = self._elapsed_millis(openai_started_at)
            logger.info(
                "OpenAI job posting 처리 성공(저신뢰도 분기). openaiLatencyMs=%s",
                openai_latency_millis,
                extra=self._log_context(message),
            )
            result = JobPostingIngestResponse(
                savedToDatabase=False,
                message="소분류 분류 confidence가 낮아 저장을 보류했습니다.",
                extracted=extracted,
                candidates=candidates,
                classification=classification,
                generated=None,
                saved=None,
            )
            self._complete_low_confidence_job_posting(message, result)
            return

        generated = self._openai_worker.generate(extracted, classification)
        openai_latency_millis = self._elapsed_millis(openai_started_at)
        logger.info(
            "OpenAI job posting 처리 성공. openaiLatencyMs=%s",
            openai_latency_millis,
            extra=self._log_context(message),
        )

        finalize_request = self._build_job_posting_finalize_request(
            message,
            extracted=extracted,
            candidates=candidates,
            classification=classification,
            generated=generated,
        )
        self._store_job_posting_result(message, finalize_request)
        pending_entry = self._enqueue_pending_delivery(
            task_id=message.taskId,
            delivery_kind="JOB_POSTING_FINALIZE",
            delivery_path="/api/internal/worker/job-postings/ingest/finalize",
            payload=finalize_request.model_dump(mode="json"),
            retry_count=message.retryCount,
        )
        delivered = self._deliver_pending_entry(
            pending_entry,
            retry_count=message.retryCount,
            replayed=False,
        )
        if not delivered:
            logger.warning(
                "job posting finalize 전달이 보류되어 recovery spool에 남겼습니다.",
                extra=self._log_context(message),
            )

    def _process_analysis_task(self, message: AnalysisTaskMessage) -> None:
        queue_latency_millis = self._compute_queue_latency_millis(message.submittedAt)
        self._ensure_analysis_not_timed_out(queue_latency_millis)

        self._api_client.mark_analysis_running(
            message.taskId,
            AnalysisWorkerRunningRequest(
                workerId=self._worker_id,
                retryCount=message.retryCount,
                submittedAt=message.submittedAt,
            ),
        )
        context = self._api_client.get_analysis_context(
            AnalysisWorkerContextRequest(
                taskId=message.taskId,
                userId=message.userId,
                mockApplyId=message.mockApplyId,
            )
        )

        openai_started_at = monotonic()
        llm_response, openai_request_id = self._analysis_openai_worker.analyze(context)
        openai_latency_millis = self._elapsed_millis(openai_started_at)
        logger.info(
            "OpenAI analysis 처리 성공. openaiLatencyMs=%s openaiRequestId=%s",
            openai_latency_millis,
            openai_request_id,
            extra=self._log_context(message),
        )

        complete_request = self._build_analysis_complete_request(message, llm_response, queue_latency_millis)
        self._store_analysis_result(message, llm_response)
        pending_entry = self._enqueue_pending_delivery(
            task_id=message.taskId,
            delivery_kind="ANALYSIS_COMPLETE",
            delivery_path=f"/api/internal/worker/analysis/tasks/{message.taskId}/complete",
            payload=complete_request.model_dump(mode="json"),
            retry_count=message.retryCount,
        )
        delivered = self._deliver_pending_entry(
            pending_entry,
            retry_count=message.retryCount,
            replayed=False,
        )
        if not delivered:
            logger.warning(
                "analysis complete 전달이 보류되어 recovery spool에 남겼습니다.",
                extra=self._log_context(message),
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
        logger.info("job posting result 저장을 시작합니다.", extra=self._log_context(message))
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
        logger.info("analysis result 저장을 시작합니다.", extra=self._log_context(message))
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
        logger.info(
            "job posting complete(저신뢰도 분기)를 시작합니다.",
            extra=self._log_context(message),
        )
        response = self._run_api_call_with_retry(
            operation_name="job posting complete",
            task_id=message.taskId,
            retry_count=message.retryCount,
            action=lambda: self._api_client.complete_task(message.taskId, result),
        )
        if response is None:
            raise RetryableWorkerError("job posting complete 응답이 없습니다.")

    def _enqueue_pending_delivery(
        self,
        *,
        task_id: str,
        delivery_kind: str,
        delivery_path: str,
        payload: dict[str, Any],
        retry_count: int,
    ) -> PendingDeliveryEntry:
        try:
            entry = PendingDeliveryEntry(
                taskId=task_id,
                deliveryKind=delivery_kind,
                deliveryPath=delivery_path,
                payload=payload,
                storedAt=self._utcnow().isoformat(),
            )
            self._recovery_store.upsert(entry)
            logger.info(
                "pending delivery를 recovery spool에 기록했습니다. deliveryKind=%s",
                delivery_kind,
                extra=self._delivery_log_context(task_id, retry_count),
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
        logger.info(
            "%s 전달을 시작합니다. replayed=%s",
            entry.deliveryKind,
            replayed,
            extra=self._delivery_log_context(entry.taskId, retry_count),
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
            self._run_api_call_with_retry(
                operation_name=f"{entry.deliveryKind} 전달",
                task_id=entry.taskId,
                retry_count=retry_count,
                action=action,
                on_retryable_error=on_retryable_error,
                replayed=replayed,
            )
        except RetryableWorkerError:
            return False
        except NonRetryableWorkerError:
            self._recovery_store.delete(entry.taskId, entry.deliveryKind)
            raise

        self._recovery_store.delete(entry.taskId, entry.deliveryKind)
        logger.info(
            "%s 전달이 완료되었습니다. replayed=%s",
            entry.deliveryKind,
            replayed,
            extra=self._delivery_log_context(entry.taskId, retry_count),
        )
        return True

    def _recover_pending_deliveries(self) -> None:
        if not self._recovery_lock.acquire(blocking=False):
            return

        try:
            entries = self._recovery_store.list_entries()
            if not entries:
                return

            logger.info(
                "recovery spool 재전송을 시작합니다. pendingCount=%s",
                len(entries),
                extra={"workerId": self._worker_id},
            )
            for entry in entries:
                if self._stop_event.is_set():
                    return
                if not self._register_inflight(entry.taskId):
                    continue
                try:
                    delivered = self._deliver_pending_entry(
                        entry,
                        retry_count=entry.attemptCount,
                        replayed=True,
                    )
                    if not delivered:
                        logger.warning(
                            "recovery spool 재전송이 아직 완료되지 않았습니다. nextAttemptAt=%s lastError=%s",
                            entry.nextAttemptAt,
                            entry.lastError,
                            extra=self._delivery_log_context(entry.taskId, entry.attemptCount),
                        )
                except NonRetryableWorkerError:
                    logger.exception(
                        "recovery spool 항목 재전송이 비재시도 오류로 종료되었습니다. deliveryKind=%s",
                        entry.deliveryKind,
                        extra=self._delivery_log_context(entry.taskId, entry.attemptCount),
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
            started_at = monotonic()
            try:
                result = action()
                logger.info(
                    "%s 성공. attempt=%s latencyMs=%s replayed=%s",
                    operation_name,
                    attempt,
                    self._elapsed_millis(started_at),
                    replayed,
                    extra=self._delivery_log_context(task_id, retry_count),
                )
                return result
            except RetryableWorkerError as exc:
                next_attempt_at = None
                delay_seconds = 0.0
                if attempt < max_attempts:
                    delay_seconds = self._compute_backoff_seconds(attempt)
                    next_attempt_at = (self._utcnow() + timedelta(seconds=delay_seconds)).isoformat()
                logger.warning(
                    "%s 실패(재시도 가능). attempt=%s maxAttempts=%s latencyMs=%s nextAttemptAt=%s error=%s replayed=%s",
                    operation_name,
                    attempt,
                    max_attempts,
                    self._elapsed_millis(started_at),
                    next_attempt_at,
                    exc,
                    replayed,
                    extra=self._delivery_log_context(task_id, retry_count),
                )
                if on_retryable_error is not None:
                    on_retryable_error(attempt, str(exc), next_attempt_at)
                if attempt >= max_attempts:
                    raise
                self._sleep_fn(delay_seconds)
            except NonRetryableWorkerError as exc:
                logger.error(
                    "%s 실패(비재시도). attempt=%s latencyMs=%s error=%s replayed=%s",
                    operation_name,
                    attempt,
                    self._elapsed_millis(started_at),
                    exc,
                    replayed,
                    extra=self._delivery_log_context(task_id, retry_count),
                )
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
    ) -> None:
        next_retry_count = message.retryCount + 1
        max_retry_count = self._resolve_max_retry_count(message)
        queue_latency_millis = error.queue_latency_millis or self._safe_compute_queue_latency(message.submittedAt)

        if isinstance(message, AnalysisTaskMessage):
            if next_retry_count > max_retry_count:
                self._safe_fail_analysis_task(
                    message,
                    str(error),
                    error.failure_reason,
                    next_retry_count,
                    queue_latency_millis,
                    error.openai_request_id,
                )
                if self._publish_dlq(channel, body, properties, message):
                    channel.basic_ack(delivery_tag=delivery_tag)
                else:
                    channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                return

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
                self._safe_fail_job_posting_task(
                    message,
                    str(error),
                    error.failure_reason,
                    next_retry_count,
                    queue_latency_millis,
                )
                if self._publish_dlq(channel, body, properties, message):
                    channel.basic_ack(delivery_tag=delivery_tag)
                else:
                    channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                return
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
                headers={"x-retry-count": next_retry_count},
            ),
        )
        if published:
            channel.basic_ack(delivery_tag=delivery_tag)
            return
        channel.basic_nack(delivery_tag=delivery_tag, requeue=True)

    def _handle_non_retryable(
        self,
        channel,
        delivery_tag: int,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage,
        body: bytes,
        properties: Any,
        error: NonRetryableWorkerError,
    ) -> None:
        if isinstance(message, AnalysisTaskMessage):
            queue_latency_millis = error.queue_latency_millis or self._safe_compute_queue_latency(message.submittedAt)
            self._safe_fail_analysis_task(
                message,
                str(error),
                error.failure_reason,
                message.retryCount,
                queue_latency_millis,
                error.openai_request_id,
            )
            if self._publish_dlq(channel, body, properties, message):
                channel.basic_ack(delivery_tag=delivery_tag)
                return
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            return

        queue_latency_millis = error.queue_latency_millis or self._safe_compute_queue_latency(message.submittedAt)
        self._safe_fail_job_posting_task(
            message,
            str(error),
            error.failure_reason,
            message.retryCount,
            queue_latency_millis,
        )
        if self._publish_dlq(channel, body, properties, message):
            channel.basic_ack(delivery_tag=delivery_tag)
            return
        channel.basic_nack(delivery_tag=delivery_tag, requeue=True)

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
                headers=getattr(properties, "headers", None),
            ),
        )

    def _safe_retry_job_posting_task(
        self,
        message: JobPostingIngestTaskMessage,
        error_message: str,
        failure_reason: str,
        retry_count: int,
        queue_latency_millis: int | None,
    ) -> None:
        try:
            logger.info(
                "job posting 작업을 retry 상태로 반영합니다. failureReason=%s",
                failure_reason,
                extra=self._log_context(message, retry_count),
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
            logger.exception(
                "Spring API에 job posting retry 상태를 반영하지 못했습니다.",
                extra=self._log_context(message, retry_count),
            )

    def _safe_fail_job_posting_task(
        self,
        message: JobPostingIngestTaskMessage,
        error_message: str,
        failure_reason: str,
        retry_count: int,
        queue_latency_millis: int | None,
    ) -> None:
        try:
            logger.info(
                "job posting 작업을 failed 상태로 반영합니다. failureReason=%s",
                failure_reason,
                extra=self._log_context(message, retry_count),
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
            logger.exception(
                "Spring API에 job posting 실패 상태를 반영하지 못했습니다.",
                extra=self._log_context(message, retry_count),
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
        try:
            logger.info(
                "analysis 작업을 retry 상태로 반영합니다. failureReason=%s",
                failure_reason,
                extra=self._log_context(message, retry_count),
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
            logger.exception(
                "Spring API에 analysis retry 상태를 반영하지 못했습니다.",
                extra=self._log_context(message, retry_count),
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
        try:
            logger.info(
                "analysis 작업을 failed 상태로 반영합니다. failureReason=%s",
                failure_reason,
                extra=self._log_context(message, retry_count),
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
            logger.exception(
                "Spring API에 analysis 실패 상태를 반영하지 못했습니다.",
                extra=self._log_context(message, retry_count),
            )

    def _compute_queue_latency_millis(self, submitted_at: str) -> int:
        submitted_at_datetime = datetime.fromisoformat(submitted_at.replace("Z", "+00:00"))
        if submitted_at_datetime.tzinfo is None:
            submitted_at_datetime = submitted_at_datetime.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        latency = (now - submitted_at_datetime.astimezone(timezone.utc)).total_seconds() * 1000
        return max(int(latency), 0)

    def _safe_compute_queue_latency(self, submitted_at: str) -> int | None:
        try:
            return self._compute_queue_latency_millis(submitted_at)
        except Exception:
            logger.exception("queue latency 계산에 실패했습니다. submittedAt=%s", submitted_at)
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
            logger.exception(
                "RabbitMQ publish 확인에 실패했습니다. exchange=%s routingKey=%s",
                exchange,
                routing_key,
                extra={"workerId": self._worker_id},
            )
            return False

    def _register_inflight(self, task_id: str) -> bool:
        with self._inflight_lock:
            if task_id in self._inflight_task_ids:
                return False
            self._inflight_task_ids.add(task_id)
            return True

    def _release_inflight(self, task_id: str) -> None:
        with self._inflight_lock:
            self._inflight_task_ids.discard(task_id)

    def _log_context(
        self,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage,
        retry_count: int | None = None,
    ) -> dict[str, str | int]:
        return self._delivery_log_context(
            message.taskId,
            retry_count if retry_count is not None else message.retryCount,
        )

    def _delivery_log_context(self, task_id: str, retry_count: int) -> dict[str, str | int]:
        return {
            "taskId": task_id,
            "workerId": self._worker_id,
            "retryCount": retry_count,
        }

    def _elapsed_millis(self, started_at: float) -> int:
        return max(int((monotonic() - started_at) * 1000), 0)

    def _utcnow(self) -> datetime:
        return datetime.now(timezone.utc)
