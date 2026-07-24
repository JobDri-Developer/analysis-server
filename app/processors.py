from __future__ import annotations

import logging
from datetime import datetime, timezone
from time import monotonic

from app.api_client import SpringWorkerApiClient
from app.config import settings
from app.delivery import WorkerDeliveryService
from app.logging_utils import bind_log_context, log_info, log_warning
from app.metrics import observe_task_queue_wait
from app.openai_client import AnalysisOpenAiWorker, JobPostingOpenAiWorker
from app.schemas import (
    AnalysisTaskMessage,
    AnalysisWorkerCompleteRequest,
    AnalysisWorkerContextRequest,
    AnalysisWorkerRunningRequest,
    JobPostingClassificationCandidateResponse,
    JobPostingClassificationResultResponse,
    JobPostingIngestResponse,
    JobPostingIngestTaskMessage,
    JobPostingWorkerFinalizeRequest,
    JobPostingWorkerRunningRequest,
    NonRetryableWorkerError,
    RetryableWorkerError,
)

logger = logging.getLogger(__name__)


class JobPostingTaskProcessor:
    def __init__(
        self,
        *,
        api_client: SpringWorkerApiClient,
        openai_worker: JobPostingOpenAiWorker,
        delivery_service: WorkerDeliveryService,
        worker_id: str,
    ) -> None:
        self._api_client = api_client
        self._openai_worker = openai_worker
        self._delivery_service = delivery_service
        self._worker_id = worker_id

    def process(self, message: JobPostingIngestTaskMessage) -> None:
        if message.taskType != "JOB_POSTING_INGEST":
            raise NonRetryableWorkerError(f"지원하지 않는 taskType입니다. taskType={message.taskType}")

        task_started_at = monotonic()
        queue_latency_millis = self._safe_compute_queue_latency(message.submittedAt)
        if queue_latency_millis is not None:
            observe_task_queue_wait(message.taskType, queue_latency_millis / 1000)

        with bind_log_context(queueLatencyMillis=queue_latency_millis):
            log_info(logger, "worker.task.started", "job posting 작업을 시작합니다.")
            self._api_client.mark_job_posting_running(
                message.taskId,
                JobPostingWorkerRunningRequest(
                    workerId=self._worker_id,
                    retryCount=message.retryCount,
                    submittedAt=message.submittedAt,
                ),
            )

            context_started_at = monotonic()
            context = self._api_client.get_context(message.userId, message.imageObjectKey)
            context_fetch_latency_ms = self._elapsed_millis(context_started_at)
            log_info(
                logger,
                "worker.context.fetch.completed",
                "job posting context 조회가 완료되었습니다.",
                latencyMs=context_fetch_latency_ms,
                contextFetchLatencyMs=context_fetch_latency_ms,
            )

            extracted = self._openai_worker.extract(message.rawText, context.imageUrl)
            candidates_started_at = monotonic()
            candidates = self._api_client.get_candidates(extracted)
            candidate_fetch_latency_ms = self._elapsed_millis(candidates_started_at)
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
                self._delivery_service.complete_low_confidence_job_posting(message, result)
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
            finalize_request = JobPostingWorkerFinalizeRequest(
                taskId=message.taskId,
                userId=message.userId,
                extracted=extracted,
                candidates=candidates,
                classification=classification,
                generated=generated,
            )
            result_store_started_at = monotonic()
            self._delivery_service.store_job_posting_result(message, finalize_request)
            result_store_latency_ms = self._elapsed_millis(result_store_started_at)
            log_info(
                logger,
                "worker.result.store.completed",
                "job posting result 저장이 완료되었습니다.",
                latencyMs=result_store_latency_ms,
                resultStoreLatencyMs=result_store_latency_ms,
            )
            pending_entry = self._delivery_service.enqueue_pending_delivery(
                message=message,
                delivery_kind="JOB_POSTING_FINALIZE",
                delivery_path="/api/internal/worker/job-postings/ingest/finalize",
                payload=finalize_request.model_dump(mode="json"),
                retry_count=message.retryCount,
            )
            delivery_started_at = monotonic()
            delivered = self._delivery_service.deliver_pending_entry(
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

    def _safe_compute_queue_latency(self, submitted_at: datetime) -> int | None:
        try:
            return _compute_queue_latency_millis(submitted_at)
        except Exception:
            log_warning(
                logger,
                "worker.queue_latency.failed",
                "queue latency 계산에 실패했습니다.",
                submittedAt=submitted_at,
            )
            return None

    def _elapsed_millis(self, started_at: float) -> int:
        return max(int((monotonic() - started_at) * 1000), 0)


class AnalysisTaskProcessor:
    def __init__(
        self,
        *,
        api_client: SpringWorkerApiClient,
        openai_worker: AnalysisOpenAiWorker,
        delivery_service: WorkerDeliveryService,
        worker_id: str,
    ) -> None:
        self._api_client = api_client
        self._openai_worker = openai_worker
        self._delivery_service = delivery_service
        self._worker_id = worker_id

    def process(self, message: AnalysisTaskMessage) -> None:
        task_started_at = monotonic()
        queue_latency_millis = _compute_queue_latency_millis(message.submittedAt)
        observe_task_queue_wait(message.taskType, queue_latency_millis / 1000)
        self._ensure_not_timed_out(queue_latency_millis)

        with bind_log_context(queueLatencyMillis=queue_latency_millis):
            log_info(logger, "worker.task.started", "analysis 작업을 시작합니다.")
            self._api_client.mark_analysis_running(
                message.taskId,
                AnalysisWorkerRunningRequest(
                    workerId=self._worker_id,
                    retryCount=message.retryCount,
                    submittedAt=message.submittedAt,
                ),
            )
            context_started_at = monotonic()
            context = self._api_client.get_analysis_context(
                AnalysisWorkerContextRequest(
                    taskId=message.taskId,
                    userId=message.userId,
                    mockApplyId=message.mockApplyId,
                )
            )
            context_fetch_latency_ms = self._elapsed_millis(context_started_at)
            log_info(
                logger,
                "worker.context.fetch.completed",
                "analysis context 조회가 완료되었습니다.",
                latencyMs=context_fetch_latency_ms,
                contextFetchLatencyMs=context_fetch_latency_ms,
            )

            analysis_started_at = monotonic()
            log_info(logger, "worker.analysis.started", "analysis LLM 처리를 시작합니다.")
            try:
                llm_response, openai_request_id = self._openai_worker.analyze(context)
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
            complete_request = AnalysisWorkerCompleteRequest(
                userId=message.userId,
                mockApplyId=message.mockApplyId,
                workerId=self._worker_id,
                queueLatencyMillis=queue_latency_millis,
                llmResponse=llm_response,
            )
            result_store_started_at = monotonic()
            self._delivery_service.store_analysis_result(message, llm_response)
            result_store_latency_ms = self._elapsed_millis(result_store_started_at)
            log_info(
                logger,
                "worker.result.store.completed",
                "analysis result 저장이 완료되었습니다.",
                latencyMs=result_store_latency_ms,
                resultStoreLatencyMs=result_store_latency_ms,
            )
            pending_entry = self._delivery_service.enqueue_pending_delivery(
                message=message,
                delivery_kind="ANALYSIS_COMPLETE",
                delivery_path=f"/api/internal/worker/analysis/tasks/{message.taskId}/complete",
                payload=complete_request.model_dump(mode="json"),
                retry_count=message.retryCount,
            )
            delivery_started_at = monotonic()
            delivered = self._delivery_service.deliver_pending_entry(
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

    def _ensure_not_timed_out(self, queue_latency_millis: int) -> None:
        if queue_latency_millis > settings.analysis_queue_timeout_millis:
            raise NonRetryableWorkerError(
                (
                    f"analysis 작업이 queue timeout을 초과했습니다. "
                    f"latency={queue_latency_millis}ms threshold={settings.analysis_queue_timeout_millis}ms"
                ),
                failure_reason="QUEUE_TIMEOUT",
                queue_latency_millis=queue_latency_millis,
            )

    def _elapsed_millis(self, started_at: float) -> int:
        return max(int((monotonic() - started_at) * 1000), 0)


def _compute_queue_latency_millis(submitted_at: datetime) -> int:
    submitted_at_datetime = submitted_at
    if submitted_at_datetime.tzinfo is None:
        submitted_at_datetime = submitted_at_datetime.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    latency = (now - submitted_at_datetime.astimezone(timezone.utc)).total_seconds() * 1000
    return max(int(latency), 0)
