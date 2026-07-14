from __future__ import annotations

import json
import logging
import os
import socket
import threading
from datetime import datetime, timezone
from typing import Any

import pika

from app.api_client import SpringWorkerApiClient
from app.config import settings
from app.openai_client import AnalysisOpenAiWorker, JobPostingOpenAiWorker
from app.schemas import (
    AnalysisTaskMessage,
    AnalysisWorkerCompleteRequest,
    AnalysisWorkerContextRequest,
    AnalysisWorkerFailureRequest,
    AnalysisWorkerRetryRequest,
    AnalysisWorkerRunningRequest,
    JobPostingClassificationResultResponse,
    JobPostingIngestResponse,
    JobPostingIngestTaskMessage,
    JobPostingWorkerFailureRequest,
    JobPostingWorkerFinalizeRequest,
    JobPostingWorkerRetryRequest,
    JobPostingWorkerRunningRequest,
    NonRetryableWorkerError,
    RetryableWorkerError,
)

logger = logging.getLogger(__name__)


class RabbitMqConsumer:
    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._connection: pika.BlockingConnection | None = None
        self._channel: pika.adapters.blocking_connection.BlockingChannel | None = None
        self._api_client = SpringWorkerApiClient()
        self._openai_worker = JobPostingOpenAiWorker()
        self._analysis_openai_worker = AnalysisOpenAiWorker()
        self._worker_id = f"{socket.gethostname()}:{os.getpid()}"
        self._inflight_task_ids: set[str] = set()
        self._inflight_lock = threading.Lock()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="rabbitmq-consumer", daemon=True)
        self._thread.start()

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
        logger.info("job posting 작업을 running 상태로 반영합니다.", extra=self._log_context(message))
        self._api_client.mark_job_posting_running(
            message.taskId,
            JobPostingWorkerRunningRequest(
                workerId=self._worker_id,
                retryCount=message.retryCount,
                submittedAt=message.submittedAt,
            ),
        )

        context = self._api_client.get_context(message.userId, message.imageObjectKey)
        extracted = self._openai_worker.extract(message.rawText, context.imageUrl)
        candidates = self._api_client.get_candidates(extracted)
        if not candidates:
            raise NonRetryableWorkerError(
                "소분류 후보를 찾을 수 없습니다.",
                failure_reason="VALIDATION_ERROR",
                queue_latency_millis=queue_latency_millis,
            )

        classification = self._openai_worker.classify(extracted, candidates)
        classification = self._normalize_classification(classification, candidates)

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
            self._api_client.complete_task(message.taskId, result)
            return

        generated = self._openai_worker.generate(extracted, classification)
        self._api_client.finalize(
            JobPostingWorkerFinalizeRequest(
                taskId=message.taskId,
                userId=message.userId,
                extracted=extracted,
                candidates=candidates,
                classification=classification,
                generated=generated,
            )
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
        llm_response, openai_request_id = self._analysis_openai_worker.analyze(context)
        self._api_client.complete_analysis_task(
            message.taskId,
            AnalysisWorkerCompleteRequest(
                userId=message.userId,
                mockApplyId=message.mockApplyId,
                workerId=self._worker_id,
                queueLatencyMillis=queue_latency_millis,
                llmResponse=llm_response,
            ),
        )

    def _normalize_classification(
        self,
        classification: JobPostingClassificationResultResponse,
        candidates,
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
        return {
            "taskId": message.taskId,
            "workerId": self._worker_id,
            "retryCount": retry_count if retry_count is not None else message.retryCount,
        }
