from __future__ import annotations

import asyncio
import json
import logging
from types import SimpleNamespace
from typing import Any

from app.config import settings
from app.logging_utils import bind_log_context, log_exception, log_info, log_warning
from app.metrics import increment_task_retry
from app.schemas import (
    AnalysisTaskMessage,
    AnalysisTaskStatusResponse,
    AnalysisWorkerFailureRequest,
    AnalysisWorkerRetryRequest,
    JobPostingIngestTaskMessage,
    JobPostingTaskStatusResponse,
    JobPostingWorkerFailureRequest,
    JobPostingWorkerRetryRequest,
    NonRetryableWorkerError,
    RetryableWorkerError,
)

logger = logging.getLogger(__name__)


class AsyncConsumerRuntime:
    def __init__(self, consumer) -> None:
        self._consumer = consumer
        self._thread = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._connection = None
        self._channel = None
        self._recovery_task: asyncio.Task[None] | None = None
        self._message_tasks: set[asyncio.Task[None]] = set()
        self._consumer_tags: list[tuple[Any, str]] = []

    def start(self) -> None:
        import threading

        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run_loop, name="rabbitmq-consumer-async", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._consumer._stop_event.set()
        if self._loop and self._loop.is_running():
            future = asyncio.run_coroutine_threadsafe(self._shutdown_async(), self._loop)
            try:
                future.result(timeout=5)
            except Exception:
                pass

    def _run_loop(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._run())
        finally:
            self._loop.run_until_complete(self._consumer._api_client.aclose())
            self._loop.close()

    async def _run(self) -> None:
        if self._recovery_task is None:
            self._recovery_task = asyncio.create_task(self._recovery_loop_async())

        while not self._consumer._stop_event.is_set():
            with bind_log_context(workerId=self._consumer._worker_id):
                try:
                    await self._consume_until_stopped()
                except Exception:
                    log_exception(
                        logger,
                        "worker.consumer.failed",
                        "RabbitMQ consumer 연결/소비 중 오류가 발생했습니다.",
                    )
                    if self._consumer._stop_event.is_set():
                        break
                    await asyncio.sleep(5)

        if self._recovery_task is not None:
            self._recovery_task.cancel()
            try:
                await self._recovery_task
            except asyncio.CancelledError:
                pass

    async def _consume_until_stopped(self) -> None:
        import aio_pika

        self._connection = await aio_pika.connect_robust(
            host=settings.rabbitmq_host,
            port=settings.rabbitmq_port,
            login=settings.rabbitmq_username,
            password=settings.rabbitmq_password,
            virtualhost=settings.rabbitmq_vhost,
            heartbeat=30,
        )
        self._channel = await self._connection.channel(publisher_confirms=True)
        await self._channel.set_qos(prefetch_count=settings.rabbitmq_prefetch_count)
        await self._recover_pending_deliveries_async()

        job_queue = await self._channel.declare_queue(settings.rabbitmq_queue, passive=True)
        analysis_queue = await self._channel.declare_queue(settings.analysis_rabbitmq_queue, passive=True)
        job_tag = await job_queue.consume(self._on_incoming_message)
        analysis_tag = await analysis_queue.consume(self._on_incoming_message)
        self._consumer_tags = [(job_queue, job_tag), (analysis_queue, analysis_tag)]

        log_info(
            logger,
            "worker.consumer.started",
            "RabbitMQ consumer를 시작합니다.",
            queues=[settings.rabbitmq_queue, settings.analysis_rabbitmq_queue],
            prefetchCount=settings.rabbitmq_prefetch_count,
        )

        try:
            while not self._consumer._stop_event.is_set():
                await asyncio.sleep(0.5)
        finally:
            await self._shutdown_async()

    async def _shutdown_async(self) -> None:
        for queue, tag in self._consumer_tags:
            try:
                await queue.cancel(tag)
            except Exception:
                continue
        self._consumer_tags.clear()

        if self._message_tasks:
            done, pending = await asyncio.wait(self._message_tasks, timeout=30)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            self._message_tasks = set(done)

        if self._channel is not None and not getattr(self._channel, "is_closed", True):
            try:
                await self._channel.close()
            except Exception:
                pass
        if self._connection is not None and not getattr(self._connection, "is_closed", True):
            try:
                await self._connection.close()
            except Exception:
                pass

    async def _on_incoming_message(self, incoming_message) -> None:
        task = asyncio.create_task(self._handle_incoming_message(incoming_message))
        self._message_tasks.add(task)
        task.add_done_callback(self._message_tasks.discard)

    async def _handle_incoming_message(self, incoming_message) -> None:
        properties = self._build_properties(incoming_message)
        incoming_context = self._consumer._extract_message_context(properties)
        processing_started_at: float | None = None
        slot_lease = None

        try:
            payload = json.loads(incoming_message.body.decode("utf-8"))
            incoming_context = self._consumer._extract_message_context(properties, payload)
            message = self._consumer._deserialize_message(payload, properties)
        except Exception:
            with bind_log_context(**incoming_context):
                log_exception(
                    logger,
                    "queue.consume.failed",
                    "메시지 역직렬화에 실패했습니다.",
                    deliveryTag=getattr(incoming_message, "delivery_tag", None),
                    taskProcessingLatencyMs=0,
                    failureReason="INVALID_PAYLOAD",
                    errorCode="INVALID_PAYLOAD",
                    bodySize=len(incoming_message.body),
                )
            await self._ack_message_async(incoming_message)
            return

        if not self._consumer._register_inflight(message.taskId, message.taskType):
            with bind_log_context(**self._consumer._message_log_context(message)):
                log_warning(
                    logger,
                    "queue.consume.failed",
                    "동일 taskId가 이미 처리 중이어서 메시지를 재큐잉합니다.",
                    deliveryTag=getattr(incoming_message, "delivery_tag", None),
                    requeue=True,
                    failureReason="TASK_ALREADY_INFLIGHT",
                    errorCode="TASK_ALREADY_INFLIGHT",
                )
                await self._nack_message_async(incoming_message, requeue=True)
            return

        slot_lease = self._consumer._concurrency_limiter.try_acquire(message.taskType)
        if slot_lease is None:
            with bind_log_context(**self._consumer._message_log_context(message)):
                log_warning(
                    logger,
                    "queue.consume.failed",
                    "task type 동시 처리 상한에 도달해 메시지를 재큐잉합니다.",
                    deliveryTag=getattr(incoming_message, "delivery_tag", None),
                    requeue=True,
                    failureReason="TASK_TYPE_LIMIT_REACHED",
                    errorCode="TASK_TYPE_LIMIT_REACHED",
                    concurrencyLimit=self._consumer._concurrency_limiter.limit_for(message.taskType),
                )
                await self._nack_message_async(incoming_message, requeue=True)
            self._consumer._release_inflight(message.taskId)
            return

        try:
            with bind_log_context(**self._consumer._message_log_context(message)):
                processing_started_at = self._consumer._now_monotonic()
                log_info(
                    logger,
                    "queue.consume.started",
                    "RabbitMQ 메시지 소비를 시작합니다.",
                    deliveryTag=getattr(incoming_message, "delivery_tag", None),
                    redelivered=getattr(incoming_message, "redelivered", False),
                )
                if isinstance(message, AnalysisTaskMessage):
                    await self._consumer._analysis_processor.process_async(message)
                else:
                    await self._consumer._job_posting_processor.process_async(message)
                processing_latency_ms = self._consumer._elapsed_millis(processing_started_at) or 0
                self._consumer._observe_processing_metric(message.taskType, "succeeded", processing_latency_ms)
                log_info(
                    logger,
                    "queue.consume.completed",
                    "RabbitMQ 메시지 소비가 완료되었습니다.",
                    deliveryTag=getattr(incoming_message, "delivery_tag", None),
                    taskProcessingLatencyMs=processing_latency_ms,
                )
                await self._ack_message_async(incoming_message)
        except NonRetryableWorkerError as exc:
            with bind_log_context(**self._consumer._message_log_context(message, queue_latency_millis=exc.queue_latency_millis)):
                processing_latency_ms = self._consumer._elapsed_millis(processing_started_at) or 0
                log_warning(
                    logger,
                    "queue.consume.failed",
                    "비재시도 에러로 작업을 실패 처리합니다.",
                    deliveryTag=getattr(incoming_message, "delivery_tag", None),
                    failureReason=exc.failure_reason,
                    errorCode=exc.failure_reason,
                    error=str(exc),
                    taskProcessingLatencyMs=processing_latency_ms,
                )
                outcome = await self._handle_non_retryable_async(incoming_message, properties, message, incoming_message.body, exc)
                self._consumer._observe_processing_metric(message.taskType, outcome, processing_latency_ms)
        except RetryableWorkerError as exc:
            with bind_log_context(**self._consumer._message_log_context(message, queue_latency_millis=exc.queue_latency_millis)):
                processing_latency_ms = self._consumer._elapsed_millis(processing_started_at) or 0
                log_warning(
                    logger,
                    "queue.consume.failed",
                    "재시도 가능한 에러가 발생했습니다.",
                    deliveryTag=getattr(incoming_message, "delivery_tag", None),
                    failureReason=exc.failure_reason,
                    errorCode=exc.failure_reason,
                    error=str(exc),
                    taskProcessingLatencyMs=processing_latency_ms,
                )
                outcome = await self._retry_or_fail_async(incoming_message, properties, message, incoming_message.body, exc)
                self._consumer._observe_processing_metric(message.taskType, outcome, processing_latency_ms)
        except Exception as exc:
            with bind_log_context(**self._consumer._message_log_context(message)):
                processing_latency_ms = self._consumer._elapsed_millis(processing_started_at) or 0
                log_exception(
                    logger,
                    "queue.consume.failed",
                    "예상치 못한 worker 에러가 발생했습니다.",
                    deliveryTag=getattr(incoming_message, "delivery_tag", None),
                    failureReason="INTERNAL_ERROR",
                    errorCode="INTERNAL_ERROR",
                    taskProcessingLatencyMs=processing_latency_ms,
                )
                retryable_exc = RetryableWorkerError(str(exc), failure_reason="INTERNAL_ERROR")
                outcome = await self._retry_or_fail_async(incoming_message, properties, message, incoming_message.body, retryable_exc)
                self._consumer._observe_processing_metric(message.taskType, outcome, processing_latency_ms)
        finally:
            if slot_lease is not None:
                slot_lease.release()
            self._consumer._release_inflight(message.taskId)

    async def _recover_pending_deliveries_async(self) -> None:
        if not self._consumer._recovery_lock.acquire(blocking=False):
            return

        try:
            entries = self._consumer._recovery_store.list_entries()
            if not entries:
                return

            with bind_log_context(workerId=self._consumer._worker_id):
                log_info(
                    logger,
                    "worker.recovery.scan.started",
                    "recovery spool 재전송을 시작합니다.",
                    pendingCount=len(entries),
                )
            for entry in entries:
                if self._consumer._stop_event.is_set():
                    return
                if not self._consumer._register_inflight(entry.taskId, entry.taskType):
                    continue
                try:
                    delivered = await self._consumer._delivery_service.deliver_pending_entry_async(
                        entry,
                        retry_count=entry.retryCount,
                        replayed=True,
                    )
                    if not delivered:
                        with bind_log_context(**self._consumer._entry_log_context(entry)):
                            log_warning(
                                logger,
                                "worker.recovery.replay_pending",
                                "recovery spool 재전송이 아직 완료되지 않았습니다.",
                                nextAttemptAt=entry.nextAttemptAt,
                                lastError=entry.lastError,
                                errorCode="RECOVERY_REPLAY_PENDING",
                            )
                except NonRetryableWorkerError:
                    with bind_log_context(**self._consumer._entry_log_context(entry)):
                        log_exception(
                            logger,
                            "worker.recovery.replay_failed",
                            "recovery spool 항목 재전송이 비재시도 오류로 종료되었습니다.",
                            deliveryKind=entry.deliveryKind,
                            errorCode="RECOVERY_REPLAY_FAILED",
                        )
                finally:
                    self._consumer._release_inflight(entry.taskId)
        finally:
            self._consumer._recovery_lock.release()

    async def _recovery_loop_async(self) -> None:
        await self._recover_pending_deliveries_async()
        while not self._consumer._stop_event.is_set():
            await asyncio.sleep(settings.worker_recovery_poll_interval_seconds)
            await self._recover_pending_deliveries_async()

    async def _retry_or_fail_async(
        self,
        incoming_message,
        properties: Any,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage,
        body: bytes,
        error: RetryableWorkerError,
    ) -> str:
        next_retry_count = message.retryCount + 1
        max_retry_count = self._consumer._resolve_max_retry_count(message)
        queue_latency_millis = error.queue_latency_millis or self._consumer._safe_compute_queue_latency(message.submittedAt)

        with bind_log_context(
            **self._consumer._message_log_context(
                message,
                retry_count=next_retry_count,
                queue_latency_millis=queue_latency_millis,
            )
        ):
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
                    return await self._finalize_failed_message_async(
                        incoming_message,
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

                await self._safe_retry_analysis_task_async(
                    message,
                    str(error),
                    error.failure_reason,
                    next_retry_count,
                    queue_latency_millis,
                    error.openai_request_id,
                )
            else:
                if next_retry_count > max_retry_count:
                    return await self._finalize_failed_message_async(
                        incoming_message,
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
                await self._safe_retry_job_posting_task_async(
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
            exchange, routing_key = self._consumer._resolve_publish_target(message)
            published = await self._publish_with_confirm_async(
                exchange=exchange,
                routing_key=routing_key,
                body=json.dumps(republished, ensure_ascii=True),
                properties={
                    "content_type": "application/json",
                    "message_id": message.messageId,
                    "headers": self._consumer._build_message_headers(message, retry_count=next_retry_count),
                },
            )
            if published:
                await self._ack_message_async(incoming_message)
                increment_task_retry(message.taskType, error.failure_reason)
                return "retry"
            await self._nack_message_async(incoming_message, requeue=True)
            increment_task_retry(message.taskType, error.failure_reason)
            return "retry"

    async def _handle_non_retryable_async(
        self,
        incoming_message,
        properties: Any,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage,
        body: bytes,
        error: NonRetryableWorkerError,
    ) -> str:
        if isinstance(message, AnalysisTaskMessage):
            queue_latency_millis = error.queue_latency_millis or self._consumer._safe_compute_queue_latency(message.submittedAt)
            log_warning(
                logger,
                "worker.analysis.failed",
                "analysis 작업이 실패했습니다.",
                errorCode=error.failure_reason,
                error=str(error),
                openaiRequestId=error.openai_request_id,
            )
            return await self._finalize_failed_message_async(
                incoming_message,
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

        queue_latency_millis = error.queue_latency_millis or self._consumer._safe_compute_queue_latency(message.submittedAt)
        return await self._finalize_failed_message_async(
            incoming_message,
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

    async def _finalize_failed_message_async(
        self,
        incoming_message,
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
        if self._consumer._terminal_message_store.contains(message.taskId, message.messageId):
            with bind_log_context(**self._consumer._message_log_context(message, retry_count=retry_count, queue_latency_millis=queue_latency_millis)):
                log_warning(
                    logger,
                    "worker.task.failed",
                    "이미 terminal 처리된 메시지여서 추가 실패 처리와 DLQ 적재를 건너뜁니다.",
                    failureReason=failure_reason,
                    errorCode=failure_reason,
                    outcome=outcome_reason,
                )
            await self._ack_message_async(incoming_message)
            return "failed"

        if await self._is_task_already_terminal_async(message):
            with bind_log_context(**self._consumer._message_log_context(message, retry_count=retry_count, queue_latency_millis=queue_latency_millis)):
                log_warning(
                    logger,
                    "worker.task.failed",
                    "이미 terminal 상태인 task여서 추가 실패 처리와 DLQ 적재를 건너뜁니다.",
                    failureReason=failure_reason,
                    errorCode=failure_reason,
                    outcome=outcome_reason,
                )
            await self._ack_message_async(incoming_message)
            return "failed"

        with bind_log_context(**self._consumer._message_log_context(message, retry_count=retry_count, queue_latency_millis=queue_latency_millis)):
            if isinstance(message, AnalysisTaskMessage):
                await self._safe_fail_analysis_task_async(
                    message,
                    error_message,
                    failure_reason,
                    retry_count,
                    queue_latency_millis,
                    openai_request_id,
                )
            else:
                await self._safe_fail_job_posting_task_async(
                    message,
                    error_message,
                    failure_reason,
                    retry_count,
                    queue_latency_millis,
                )

            published = await self._publish_dlq_once_async(
                body,
                properties,
                message,
                failure_reason=failure_reason,
            )
            if published:
                await self._ack_message_async(incoming_message)
                return "failed"
            log_warning(
                logger,
                "worker.task.failed",
                "DLQ publish가 실패했지만 task는 이미 terminal 상태로 반영되어 재큐잉하지 않습니다.",
                failureReason=failure_reason,
                errorCode=failure_reason,
                outcome=outcome_reason,
            )
            await self._ack_message_async(incoming_message)
            return "failed"

    async def _publish_dlq_once_async(
        self,
        body: bytes,
        properties: Any,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage,
        *,
        failure_reason: str,
    ) -> bool:
        if self._consumer._terminal_message_store.contains(message.taskId, message.messageId):
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
        published = await self._publish_dlq_async(body, properties, message)
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
            self._consumer._terminal_message_store.record(
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

    async def _publish_dlq_async(
        self,
        body: bytes,
        properties: Any,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage,
    ) -> bool:
        routing_key = settings.analysis_rabbitmq_dlq if isinstance(message, AnalysisTaskMessage) else settings.rabbitmq_dlq
        return await self._publish_with_confirm_async(
            exchange="",
            routing_key=routing_key,
            body=body,
            properties={
                "content_type": getattr(properties, "content_type", "application/json"),
                "message_id": message.messageId,
                "headers": self._consumer._merge_publish_headers(properties, message),
            },
        )

    async def _publish_with_confirm_async(
        self,
        *,
        exchange: str,
        routing_key: str,
        body: str | bytes,
        properties: dict[str, Any],
    ) -> bool:
        import aio_pika

        try:
            exchange_obj = self._channel.default_exchange if exchange == "" else await self._channel.get_exchange(exchange, ensure=False)
            await exchange_obj.publish(
                aio_pika.Message(
                    body=body.encode("utf-8") if isinstance(body, str) else body,
                    content_type=properties.get("content_type", "application/json"),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    message_id=properties.get("message_id"),
                    headers=properties.get("headers"),
                ),
                routing_key=routing_key,
                mandatory=True,
            )
            return True
        except Exception:
            with bind_log_context(workerId=self._consumer._worker_id):
                log_exception(
                    logger,
                    "worker.queue.publish_failed",
                    "RabbitMQ publish 확인에 실패했습니다.",
                    exchange=exchange,
                    routingKey=routing_key,
                )
            return False

    async def _is_task_already_terminal_async(
        self,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage,
    ) -> bool:
        try:
            task_status = await self._get_task_status_async(message)
        except Exception:
            log_exception(
                logger,
                "worker.task.terminal_check_failed",
                "task terminal 상태 확인에 실패했습니다. 보수적으로 실패 처리 경로를 계속 진행합니다.",
            )
            return False

        status = (task_status.status or "").upper()
        if status not in self._consumer.TERMINAL_TASK_STATUSES:
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

    async def _get_task_status_async(
        self,
        message: JobPostingIngestTaskMessage | AnalysisTaskMessage,
    ) -> JobPostingTaskStatusResponse | AnalysisTaskStatusResponse:
        if isinstance(message, AnalysisTaskMessage):
            return await self._consumer._api_client.get_analysis_task_async(message.taskId)
        return await self._consumer._api_client.get_job_posting_task_async(message.taskId)

    async def _safe_retry_job_posting_task_async(
        self,
        message: JobPostingIngestTaskMessage,
        error_message: str,
        failure_reason: str,
        retry_count: int,
        queue_latency_millis: int | None,
    ) -> None:
        with bind_log_context(**self._consumer._message_log_context(message, retry_count=retry_count, queue_latency_millis=queue_latency_millis)):
            try:
                log_warning(
                    logger,
                    "worker.task.retry",
                    "job posting 작업을 retry 상태로 반영합니다.",
                    failureReason=failure_reason,
                    errorCode=failure_reason,
                )
                await self._consumer._api_client.retry_job_posting_task_async(
                    message.taskId,
                    JobPostingWorkerRetryRequest(
                        errorMessage=error_message,
                        failureReason=failure_reason,
                        retryCount=retry_count,
                        workerId=self._consumer._worker_id,
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

    async def _safe_fail_job_posting_task_async(
        self,
        message: JobPostingIngestTaskMessage,
        error_message: str,
        failure_reason: str,
        retry_count: int,
        queue_latency_millis: int | None,
    ) -> None:
        with bind_log_context(**self._consumer._message_log_context(message, retry_count=retry_count, queue_latency_millis=queue_latency_millis)):
            try:
                log_warning(
                    logger,
                    "worker.task.failed",
                    "job posting 작업을 failed 상태로 반영합니다.",
                    failureReason=failure_reason,
                    errorCode=failure_reason,
                )
                await self._consumer._api_client.fail_job_posting_task_async(
                    message.taskId,
                    JobPostingWorkerFailureRequest(
                        errorMessage=error_message,
                        failureReason=failure_reason,
                        retryCount=retry_count,
                        workerId=self._consumer._worker_id,
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

    async def _safe_retry_analysis_task_async(
        self,
        message: AnalysisTaskMessage,
        error_message: str,
        failure_reason: str,
        retry_count: int,
        queue_latency_millis: int | None,
        openai_request_id: str | None,
    ) -> None:
        with bind_log_context(**self._consumer._message_log_context(message, retry_count=retry_count, queue_latency_millis=queue_latency_millis)):
            try:
                log_warning(
                    logger,
                    "worker.task.retry",
                    "analysis 작업을 retry 상태로 반영합니다.",
                    failureReason=failure_reason,
                    errorCode=failure_reason,
                    openaiRequestId=openai_request_id,
                )
                await self._consumer._api_client.retry_analysis_task_async(
                    message.taskId,
                    AnalysisWorkerRetryRequest(
                        errorMessage=error_message,
                        failureReason=failure_reason,
                        retryCount=retry_count,
                        workerId=self._consumer._worker_id,
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

    async def _safe_fail_analysis_task_async(
        self,
        message: AnalysisTaskMessage,
        error_message: str,
        failure_reason: str,
        retry_count: int,
        queue_latency_millis: int | None,
        openai_request_id: str | None,
    ) -> None:
        with bind_log_context(**self._consumer._message_log_context(message, retry_count=retry_count, queue_latency_millis=queue_latency_millis)):
            try:
                log_warning(
                    logger,
                    "worker.task.failed",
                    "analysis 작업을 failed 상태로 반영합니다.",
                    failureReason=failure_reason,
                    errorCode=failure_reason,
                    openaiRequestId=openai_request_id,
                )
                await self._consumer._api_client.fail_analysis_task_async(
                    message.taskId,
                    AnalysisWorkerFailureRequest(
                        errorMessage=error_message,
                        failureReason=failure_reason,
                        retryCount=retry_count,
                        workerId=self._consumer._worker_id,
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

    async def _ack_message_async(self, incoming_message) -> None:
        await incoming_message.ack()

    async def _nack_message_async(self, incoming_message, *, requeue: bool) -> None:
        await incoming_message.nack(requeue=requeue)

    def _build_properties(self, incoming_message) -> SimpleNamespace:
        return SimpleNamespace(
            headers=dict(getattr(incoming_message, "headers", {}) or {}),
            message_id=getattr(incoming_message, "message_id", None),
            content_type=getattr(incoming_message, "content_type", "application/json"),
        )
