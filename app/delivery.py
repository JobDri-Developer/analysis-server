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
TERMINAL_SUCCESS_STATUSES = {"SUCCEEDED", "SUCCESS", "COMPLETED", "COMPLETE"}
TERMINAL_FAILURE_STATUSES = {"FAILED", "CANCELLED"}


class WorkerDeliveryService:
    def __init__(
        self,
        *,
        api_client: SpringWorkerApiClient,
        recovery_store: PendingDeliveryStore,
        run_api_call_with_retry: Callable[..., Any],
        run_api_call_with_retry_async: Callable[..., Any],
        generate_request_id: Callable[[], str],
        utcnow: Callable[[], datetime],
        entry_log_context_factory: Callable[[PendingDeliveryEntry], dict[str, str | int | None]],
    ) -> None:
        self._api_client = api_client
        self._recovery_store = recovery_store
        self._run_api_call_with_retry = run_api_call_with_retry
        self._run_api_call_with_retry_async = run_api_call_with_retry_async
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
            action=lambda: self._store_job_posting_result_with_state_check(message.taskId, request),
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
            action=lambda: self._store_analysis_result_with_state_check(message.taskId, request),
        )

    def complete_low_confidence_job_posting(
        self,
        message: JobPostingIngestTaskMessage,
        result: JobPostingIngestResponse,
    ) -> None:
        # Legacy compatibility only. The canonical job-posting flow is result -> finalize.
        self._run_api_call_with_retry(
            operation_name="job posting complete",
            task_id=message.taskId,
            retry_count=message.retryCount,
            action=lambda: self._api_client.complete_task(message.taskId, result),
        )

    async def store_job_posting_result_async(
        self,
        message: JobPostingIngestTaskMessage,
        finalize_request: JobPostingWorkerFinalizeRequest,
    ) -> None:
        request = JobPostingWorkerResultStoreRequest(
            userId=message.userId,
            result=finalize_request,
        )
        log_info(logger, "worker.result.store.started", "job posting result 저장을 시작합니다.")
        await self._run_api_call_with_retry_async(
            operation_name="job posting result 저장",
            task_id=message.taskId,
            retry_count=message.retryCount,
            action=lambda: self._store_job_posting_result_with_state_check_async(message.taskId, request),
        )

    async def store_analysis_result_async(self, message: AnalysisTaskMessage, llm_response) -> None:
        request = AnalysisWorkerResultStoreRequest(
            userId=message.userId,
            mockApplyId=message.mockApplyId,
            llmResponse=llm_response,
        )
        log_info(logger, "worker.result.store.started", "analysis result 저장을 시작합니다.")
        await self._run_api_call_with_retry_async(
            operation_name="analysis result 저장",
            task_id=message.taskId,
            retry_count=message.retryCount,
            action=lambda: self._store_analysis_result_with_state_check_async(message.taskId, request),
        )

    async def complete_low_confidence_job_posting_async(
        self,
        message: JobPostingIngestTaskMessage,
        result: JobPostingIngestResponse,
    ) -> None:
        # Legacy compatibility only. The canonical job-posting flow is result -> finalize.
        await self._run_api_call_with_retry_async(
            operation_name="job posting complete",
            task_id=message.taskId,
            retry_count=message.retryCount,
            action=lambda: self._api_client.complete_task_async(message.taskId, result),
        )

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

    def resume_job_posting_without_llm(
        self,
        message: JobPostingIngestTaskMessage,
    ) -> bool:
        terminal_state = self._get_job_posting_terminal_state(message.taskId)
        if terminal_state is not None:
            log_info(
                logger,
                "worker.task.reused",
                "job posting task가 이미 terminal 상태여서 기존 상태를 재사용합니다.",
                taskId=message.taskId,
                status=terminal_state,
            )
            return True

        pending_entry = self._recovery_store.get_entry(message.taskId, "JOB_POSTING_FINALIZE")
        if pending_entry is not None:
            self._deliver_pending_entry_with_recovery(pending_entry, retry_count=message.retryCount, replayed=True)
            return True

        stored_result = self._api_client.get_job_posting_result(message.taskId)
        if stored_result is None:
            return False

        pending_entry = self.enqueue_pending_delivery(
            message=message,
            delivery_kind="JOB_POSTING_FINALIZE",
            delivery_path="/api/internal/worker/job-postings/ingest/finalize",
            payload=stored_result.model_dump(mode="json"),
            retry_count=message.retryCount,
        )
        self._deliver_pending_entry_with_recovery(pending_entry, retry_count=message.retryCount, replayed=True)
        return True

    async def resume_job_posting_without_llm_async(
        self,
        message: JobPostingIngestTaskMessage,
    ) -> bool:
        terminal_state = await self._get_job_posting_terminal_state_async(message.taskId)
        if terminal_state is not None:
            log_info(
                logger,
                "worker.task.reused",
                "job posting task가 이미 terminal 상태여서 기존 상태를 재사용합니다.",
                taskId=message.taskId,
                status=terminal_state,
            )
            return True

        pending_entry = self._recovery_store.get_entry(message.taskId, "JOB_POSTING_FINALIZE")
        if pending_entry is not None:
            await self._deliver_pending_entry_with_recovery_async(
                pending_entry,
                retry_count=message.retryCount,
                replayed=True,
            )
            return True

        stored_result = await self._api_client.get_job_posting_result_async(message.taskId)
        if stored_result is None:
            return False

        pending_entry = self.enqueue_pending_delivery(
            message=message,
            delivery_kind="JOB_POSTING_FINALIZE",
            delivery_path="/api/internal/worker/job-postings/ingest/finalize",
            payload=stored_result.model_dump(mode="json"),
            retry_count=message.retryCount,
        )
        await self._deliver_pending_entry_with_recovery_async(
            pending_entry,
            retry_count=message.retryCount,
            replayed=True,
        )
        return True

    def resume_analysis_without_llm(
        self,
        message: AnalysisTaskMessage,
        *,
        worker_id: str,
        queue_latency_millis: int | None,
    ) -> bool:
        terminal_state = self._get_analysis_terminal_state(message.taskId)
        if terminal_state is not None:
            log_info(
                logger,
                "worker.task.reused",
                "analysis task가 이미 terminal 상태여서 기존 상태를 재사용합니다.",
                taskId=message.taskId,
                status=terminal_state,
            )
            return True

        pending_entry = self._recovery_store.get_entry(message.taskId, "ANALYSIS_COMPLETE")
        if pending_entry is not None:
            self._deliver_pending_entry_with_recovery(pending_entry, retry_count=message.retryCount, replayed=True)
            return True

        stored_result = self._api_client.get_analysis_result(message.taskId)
        if stored_result is None:
            return False

        complete_request = AnalysisWorkerCompleteRequest(
            userId=stored_result.userId,
            mockApplyId=stored_result.mockApplyId,
            workerId=worker_id,
            queueLatencyMillis=queue_latency_millis,
            llmResponse=stored_result.llmResponse,
        )
        pending_entry = self.enqueue_pending_delivery(
            message=message,
            delivery_kind="ANALYSIS_COMPLETE",
            delivery_path=f"/api/internal/worker/analysis/tasks/{message.taskId}/complete",
            payload=complete_request.model_dump(mode="json"),
            retry_count=message.retryCount,
        )
        self._deliver_pending_entry_with_recovery(pending_entry, retry_count=message.retryCount, replayed=True)
        return True

    async def resume_analysis_without_llm_async(
        self,
        message: AnalysisTaskMessage,
        *,
        worker_id: str,
        queue_latency_millis: int | None,
    ) -> bool:
        terminal_state = await self._get_analysis_terminal_state_async(message.taskId)
        if terminal_state is not None:
            log_info(
                logger,
                "worker.task.reused",
                "analysis task가 이미 terminal 상태여서 기존 상태를 재사용합니다.",
                taskId=message.taskId,
                status=terminal_state,
            )
            return True

        pending_entry = self._recovery_store.get_entry(message.taskId, "ANALYSIS_COMPLETE")
        if pending_entry is not None:
            await self._deliver_pending_entry_with_recovery_async(
                pending_entry,
                retry_count=message.retryCount,
                replayed=True,
            )
            return True

        stored_result = await self._api_client.get_analysis_result_async(message.taskId)
        if stored_result is None:
            return False

        complete_request = AnalysisWorkerCompleteRequest(
            userId=stored_result.userId,
            mockApplyId=stored_result.mockApplyId,
            workerId=worker_id,
            queueLatencyMillis=queue_latency_millis,
            llmResponse=stored_result.llmResponse,
        )
        pending_entry = self.enqueue_pending_delivery(
            message=message,
            delivery_kind="ANALYSIS_COMPLETE",
            delivery_path=f"/api/internal/worker/analysis/tasks/{message.taskId}/complete",
            payload=complete_request.model_dump(mode="json"),
            retry_count=message.retryCount,
        )
        await self._deliver_pending_entry_with_recovery_async(
            pending_entry,
            retry_count=message.retryCount,
            replayed=True,
        )
        return True

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
                    action=lambda: self._deliver_entry_once_with_state_check(entry, action),
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

    async def deliver_pending_entry_async(
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

            def on_retryable_error(attempt: int, error_message: str, next_attempt_at: str | None) -> None:
                entry.attemptCount = attempt
                entry.lastError = error_message
                entry.nextAttemptAt = next_attempt_at
                self._recovery_store.upsert(entry)

            try:
                await self._run_api_call_with_retry_async(
                    operation_name=f"{entry.deliveryKind} 전달",
                    task_id=entry.taskId,
                    retry_count=retry_count,
                    action=lambda: self._deliver_entry_once_with_state_check_async(entry),
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

    async def _deliver_entry_once_async(self, entry: PendingDeliveryEntry) -> None:
        if entry.deliveryKind == "ANALYSIS_COMPLETE":
            request = AnalysisWorkerCompleteRequest.model_validate(entry.payload)
            await self._api_client.complete_analysis_task_async(entry.taskId, request)
            return
        if entry.deliveryKind == "JOB_POSTING_FINALIZE":
            request = JobPostingWorkerFinalizeRequest.model_validate(entry.payload)
            await self._api_client.finalize_async(request)
            return
        raise NonRetryableWorkerError(f"지원하지 않는 deliveryKind입니다. deliveryKind={entry.deliveryKind}")

    def _store_job_posting_result_with_state_check(
        self,
        task_id: str,
        request: JobPostingWorkerResultStoreRequest,
    ) -> None:
        try:
            self._api_client.store_job_posting_result(task_id, request)
        except RetryableWorkerError:
            stored_result = self._api_client.get_job_posting_result(task_id)
            if stored_result is not None:
                return
            raise

    async def _store_job_posting_result_with_state_check_async(
        self,
        task_id: str,
        request: JobPostingWorkerResultStoreRequest,
    ) -> None:
        try:
            await self._api_client.store_job_posting_result_async(task_id, request)
        except RetryableWorkerError:
            stored_result = await self._api_client.get_job_posting_result_async(task_id)
            if stored_result is not None:
                return
            raise

    def _store_analysis_result_with_state_check(
        self,
        task_id: str,
        request: AnalysisWorkerResultStoreRequest,
    ) -> None:
        try:
            self._api_client.store_analysis_result(task_id, request)
        except RetryableWorkerError:
            stored_result = self._api_client.get_analysis_result(task_id)
            if stored_result is not None:
                return
            raise

    async def _store_analysis_result_with_state_check_async(
        self,
        task_id: str,
        request: AnalysisWorkerResultStoreRequest,
    ) -> None:
        try:
            await self._api_client.store_analysis_result_async(task_id, request)
        except RetryableWorkerError:
            stored_result = await self._api_client.get_analysis_result_async(task_id)
            if stored_result is not None:
                return
            raise

    def _deliver_pending_entry_with_recovery(
        self,
        entry: PendingDeliveryEntry,
        *,
        retry_count: int,
        replayed: bool,
    ) -> bool:
        delivered = self.deliver_pending_entry(entry, retry_count=retry_count, replayed=replayed)
        if not delivered:
            log_warning(
                logger,
                "worker.delivery.deferred",
                "기존 결과를 재사용한 callback 전달이 보류되었습니다.",
                deliveryKind=entry.deliveryKind,
                taskId=entry.taskId,
            )
        return delivered

    async def _deliver_pending_entry_with_recovery_async(
        self,
        entry: PendingDeliveryEntry,
        *,
        retry_count: int,
        replayed: bool,
    ) -> bool:
        delivered = await self.deliver_pending_entry_async(entry, retry_count=retry_count, replayed=replayed)
        if not delivered:
            log_warning(
                logger,
                "worker.delivery.deferred",
                "기존 결과를 재사용한 callback 전달이 보류되었습니다.",
                deliveryKind=entry.deliveryKind,
                taskId=entry.taskId,
            )
        return delivered

    def _deliver_entry_once_with_state_check(
        self,
        entry: PendingDeliveryEntry,
        action: Callable[[], None],
    ) -> None:
        terminal_state = self._get_terminal_state(entry)
        if terminal_state in TERMINAL_SUCCESS_STATUSES:
            return
        if terminal_state in TERMINAL_FAILURE_STATUSES:
            raise NonRetryableWorkerError(f"task가 이미 실패 상태입니다. taskId={entry.taskId}")

        self._ensure_result_stored_for_entry(entry)
        try:
            action()
        except RetryableWorkerError:
            terminal_state = self._get_terminal_state(entry)
            if terminal_state in TERMINAL_SUCCESS_STATUSES:
                return
            if terminal_state in TERMINAL_FAILURE_STATUSES:
                raise NonRetryableWorkerError(f"task가 이미 실패 상태입니다. taskId={entry.taskId}")
            raise
        except NonRetryableWorkerError:
            terminal_state = self._get_terminal_state(entry)
            if terminal_state in TERMINAL_SUCCESS_STATUSES:
                return
            raise

    async def _deliver_entry_once_with_state_check_async(self, entry: PendingDeliveryEntry) -> None:
        terminal_state = await self._get_terminal_state_async(entry)
        if terminal_state in TERMINAL_SUCCESS_STATUSES:
            return
        if terminal_state in TERMINAL_FAILURE_STATUSES:
            raise NonRetryableWorkerError(f"task가 이미 실패 상태입니다. taskId={entry.taskId}")

        await self._ensure_result_stored_for_entry_async(entry)
        try:
            await self._deliver_entry_once_async(entry)
        except RetryableWorkerError:
            terminal_state = await self._get_terminal_state_async(entry)
            if terminal_state in TERMINAL_SUCCESS_STATUSES:
                return
            if terminal_state in TERMINAL_FAILURE_STATUSES:
                raise NonRetryableWorkerError(f"task가 이미 실패 상태입니다. taskId={entry.taskId}")
            raise
        except NonRetryableWorkerError:
            terminal_state = await self._get_terminal_state_async(entry)
            if terminal_state in TERMINAL_SUCCESS_STATUSES:
                return
            raise

    def _ensure_result_stored_for_entry(self, entry: PendingDeliveryEntry) -> None:
        if entry.deliveryKind == "ANALYSIS_COMPLETE":
            stored_result = self._api_client.get_analysis_result(entry.taskId)
            if stored_result is not None:
                return
            request = AnalysisWorkerCompleteRequest.model_validate(entry.payload)
            self._store_analysis_result_with_state_check(
                entry.taskId,
                AnalysisWorkerResultStoreRequest(
                    userId=request.userId,
                    mockApplyId=request.mockApplyId,
                    llmResponse=request.llmResponse,
                ),
            )
            return
        if entry.deliveryKind == "JOB_POSTING_FINALIZE":
            stored_result = self._api_client.get_job_posting_result(entry.taskId)
            if stored_result is not None:
                return
            request = JobPostingWorkerFinalizeRequest.model_validate(entry.payload)
            self._store_job_posting_result_with_state_check(
                entry.taskId,
                JobPostingWorkerResultStoreRequest(userId=request.userId, result=request),
            )
            return
        raise NonRetryableWorkerError(f"지원하지 않는 deliveryKind입니다. deliveryKind={entry.deliveryKind}")

    async def _ensure_result_stored_for_entry_async(self, entry: PendingDeliveryEntry) -> None:
        if entry.deliveryKind == "ANALYSIS_COMPLETE":
            stored_result = await self._api_client.get_analysis_result_async(entry.taskId)
            if stored_result is not None:
                return
            request = AnalysisWorkerCompleteRequest.model_validate(entry.payload)
            await self._store_analysis_result_with_state_check_async(
                entry.taskId,
                AnalysisWorkerResultStoreRequest(
                    userId=request.userId,
                    mockApplyId=request.mockApplyId,
                    llmResponse=request.llmResponse,
                ),
            )
            return
        if entry.deliveryKind == "JOB_POSTING_FINALIZE":
            stored_result = await self._api_client.get_job_posting_result_async(entry.taskId)
            if stored_result is not None:
                return
            request = JobPostingWorkerFinalizeRequest.model_validate(entry.payload)
            await self._store_job_posting_result_with_state_check_async(
                entry.taskId,
                JobPostingWorkerResultStoreRequest(userId=request.userId, result=request),
            )
            return
        raise NonRetryableWorkerError(f"지원하지 않는 deliveryKind입니다. deliveryKind={entry.deliveryKind}")

    def _get_terminal_state(self, entry: PendingDeliveryEntry) -> str | None:
        if entry.taskType == "ANALYSIS":
            return self._get_analysis_terminal_state(entry.taskId)
        if entry.taskType == "JOB_POSTING_INGEST":
            return self._get_job_posting_terminal_state(entry.taskId)
        return None

    async def _get_terminal_state_async(self, entry: PendingDeliveryEntry) -> str | None:
        if entry.taskType == "ANALYSIS":
            return await self._get_analysis_terminal_state_async(entry.taskId)
        if entry.taskType == "JOB_POSTING_INGEST":
            return await self._get_job_posting_terminal_state_async(entry.taskId)
        return None

    def _get_job_posting_terminal_state(self, task_id: str) -> str | None:
        status = (self._api_client.get_job_posting_task(task_id).status or "").upper()
        return status if status in TERMINAL_SUCCESS_STATUSES | TERMINAL_FAILURE_STATUSES else None

    async def _get_job_posting_terminal_state_async(self, task_id: str) -> str | None:
        status = (await self._api_client.get_job_posting_task_async(task_id)).status or ""
        status = status.upper()
        return status if status in TERMINAL_SUCCESS_STATUSES | TERMINAL_FAILURE_STATUSES else None

    def _get_analysis_terminal_state(self, task_id: str) -> str | None:
        status = (self._api_client.get_analysis_task(task_id).status or "").upper()
        return status if status in TERMINAL_SUCCESS_STATUSES | TERMINAL_FAILURE_STATUSES else None

    async def _get_analysis_terminal_state_async(self, task_id: str) -> str | None:
        status = (await self._api_client.get_analysis_task_async(task_id)).status or ""
        status = status.upper()
        return status if status in TERMINAL_SUCCESS_STATUSES | TERMINAL_FAILURE_STATUSES else None
