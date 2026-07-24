from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Callable

from app.api_client import SpringWorkerApiClient
from app.logging_utils import bind_log_context, log_info, log_warning
from app.recovery import PendingDeliveryStore
from app.schemas import (
    AnalysisTaskMessage,
    AnalysisWorkerCompleteRequest,
    AnalysisWorkerResultStoreRequest,
    JobPostingIngestResponse,
    JobPostingIngestTaskMessage,
    JobPostingWorkerFinalizeRequest,
    JobPostingWorkerResultStoreRequest,
    NonRetryableWorkerError,
    PendingDeliveryEntry,
    RetryableWorkerError,
)

logger = logging.getLogger(__name__)


class WorkerDeliveryService:
    def __init__(
        self,
        *,
        api_client: SpringWorkerApiClient,
        recovery_store: PendingDeliveryStore,
        run_api_call_with_retry: Callable[..., Any],
        generate_request_id: Callable[[], str],
        utcnow: Callable[[], datetime],
        entry_log_context_factory: Callable[[PendingDeliveryEntry], dict[str, str | int | None]],
    ) -> None:
        self._api_client = api_client
        self._recovery_store = recovery_store
        self._run_api_call_with_retry = run_api_call_with_retry
        self._generate_request_id = generate_request_id
        self._utcnow = utcnow
        self._entry_log_context_factory = entry_log_context_factory

    def store_job_posting_result(
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

    def store_analysis_result(self, message: AnalysisTaskMessage, llm_response) -> None:
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

    def complete_low_confidence_job_posting(
        self,
        message: JobPostingIngestTaskMessage,
        result: JobPostingIngestResponse,
    ) -> None:
        response = self._run_api_call_with_retry(
            operation_name="job posting complete",
            task_id=message.taskId,
            retry_count=message.retryCount,
            action=lambda: self._api_client.complete_task(message.taskId, result),
        )
        if response is None:
            raise RetryableWorkerError("job posting complete 응답이 없습니다.")

    def enqueue_pending_delivery(
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

    def deliver_pending_entry(
        self,
        entry: PendingDeliveryEntry,
        *,
        retry_count: int,
        replayed: bool,
    ) -> bool:
        if not entry.requestId:
            entry.requestId = self._generate_request_id()
            self._recovery_store.upsert(entry)

        with bind_log_context(**self._entry_log_context_factory(entry)):
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
            log_info(
                logger,
                "worker.recovery.replayed" if replayed else "worker.delivery.completed",
                "pending delivery 전달이 완료되었습니다.",
                deliveryKind=entry.deliveryKind,
                replayed=replayed,
            )
            return True
