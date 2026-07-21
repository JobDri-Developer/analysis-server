from __future__ import annotations

import logging
from time import monotonic
from typing import Any, TypeVar

import requests
from pydantic import TypeAdapter

from app.config import settings
from app.logging_utils import ensure_request_id, log_error, log_info, log_warning
from app.schemas import (
    AnalysisTaskStatusResponse,
    AnalysisWorkerCompleteRequest,
    AnalysisWorkerContextRequest,
    AnalysisWorkerContextResponse,
    AnalysisWorkerFailureRequest,
    AnalysisWorkerResultStoreRequest,
    AnalysisWorkerRetryRequest,
    AnalysisWorkerRunningRequest,
    ApiEnvelope,
    JobPostingClassificationCandidateResponse,
    JobPostingExtractResponse,
    JobPostingIngestResponse,
    JobPostingTaskStatusResponse,
    JobPostingWorkerContextRequest,
    JobPostingWorkerContextResponse,
    JobPostingWorkerFailureRequest,
    JobPostingWorkerFinalizeRequest,
    JobPostingWorkerResultStoreRequest,
    JobPostingWorkerRetryRequest,
    JobPostingWorkerRunningRequest,
    NonRetryableWorkerError,
    RetryableWorkerError,
)

T = TypeVar("T")

logger = logging.getLogger(__name__)


class SpringWorkerApiClient:
    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Content-Type": "application/json",
                "X-Internal-Api-Key": settings.spring_internal_api_key,
            }
        )

    def mark_job_posting_running(self, task_id: str, request: JobPostingWorkerRunningRequest) -> None:
        self._post(
            f"/api/internal/worker/job-postings/tasks/{task_id}/running",
            request.model_dump(mode="json"),
        )

    def complete_task(self, task_id: str, result: JobPostingIngestResponse) -> JobPostingIngestResponse:
        payload = result.model_dump(mode="json")
        response = self._post(f"/api/internal/worker/job-postings/tasks/{task_id}/complete", payload)
        return self._parse_result(response, JobPostingIngestResponse)

    def store_job_posting_result(self, task_id: str, request: JobPostingWorkerResultStoreRequest) -> None:
        self._post(
            f"/api/internal/worker/job-postings/tasks/{task_id}/result",
            request.model_dump(mode="json"),
            idempotent_conflict_as_success=True,
        )

    def retry_job_posting_task(self, task_id: str, request: JobPostingWorkerRetryRequest) -> None:
        self._post(
            f"/api/internal/worker/job-postings/tasks/{task_id}/retry",
            request.model_dump(mode="json"),
        )

    def fail_job_posting_task(self, task_id: str, request: JobPostingWorkerFailureRequest) -> None:
        self._post(
            f"/api/internal/worker/job-postings/tasks/{task_id}/failed",
            request.model_dump(mode="json"),
        )

    def get_job_posting_task(self, task_id: str) -> JobPostingTaskStatusResponse:
        response = self._get(f"/api/internal/worker/job-postings/tasks/{task_id}")
        return self._parse_result(response, JobPostingTaskStatusResponse)

    def get_context(self, user_id: int, image_object_key: str | None) -> JobPostingWorkerContextResponse:
        payload = JobPostingWorkerContextRequest(userId=user_id, imageObjectKey=image_object_key).model_dump(mode="json")
        response = self._post("/api/internal/worker/job-postings/ingest/context", payload)
        return self._parse_result(response, JobPostingWorkerContextResponse)

    def get_candidates(
        self, extracted: JobPostingExtractResponse
    ) -> list[JobPostingClassificationCandidateResponse]:
        response = self._post(
            "/api/internal/worker/job-postings/classification/candidates",
            extracted.model_dump(mode="json"),
        )
        return self._parse_result(response, list[JobPostingClassificationCandidateResponse])

    def finalize(self, request: JobPostingWorkerFinalizeRequest) -> None:
        self._post(
            "/api/internal/worker/job-postings/ingest/finalize",
            request.model_dump(mode="json"),
            idempotent_conflict_as_success=True,
        )

    def mark_analysis_running(self, task_id: str, request: AnalysisWorkerRunningRequest) -> None:
        self._post(
            f"/api/internal/worker/analysis/tasks/{task_id}/running",
            request.model_dump(mode="json"),
        )

    def get_analysis_context(self, request: AnalysisWorkerContextRequest) -> AnalysisWorkerContextResponse:
        response = self._post(
            "/api/internal/worker/analysis/context",
            request.model_dump(mode="json"),
        )
        return self._parse_result(response, AnalysisWorkerContextResponse)

    def retry_analysis_task(self, task_id: str, request: AnalysisWorkerRetryRequest) -> None:
        self._post(
            f"/api/internal/worker/analysis/tasks/{task_id}/retry",
            request.model_dump(mode="json"),
        )

    def fail_analysis_task(self, task_id: str, request: AnalysisWorkerFailureRequest) -> None:
        self._post(
            f"/api/internal/worker/analysis/tasks/{task_id}/failed",
            request.model_dump(mode="json"),
        )

    def complete_analysis_task(self, task_id: str, request: AnalysisWorkerCompleteRequest) -> None:
        self._post(
            f"/api/internal/worker/analysis/tasks/{task_id}/complete",
            request.model_dump(mode="json"),
            idempotent_conflict_as_success=True,
        )

    def store_analysis_result(self, task_id: str, request: AnalysisWorkerResultStoreRequest) -> None:
        self._post(
            f"/api/internal/worker/analysis/tasks/{task_id}/result",
            request.model_dump(mode="json"),
            idempotent_conflict_as_success=True,
        )

    def get_analysis_task(self, task_id: str) -> AnalysisTaskStatusResponse:
        response = self._get(f"/api/internal/worker/analysis/tasks/{task_id}")
        return self._parse_result(response, AnalysisTaskStatusResponse)

    def _post(
        self,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        idempotent_conflict_as_success: bool = False,
    ) -> ApiEnvelope:
        url = settings.spring_api_base_url.rstrip("/") + path
        request_headers = self._build_request_headers()
        log_info(
            logger,
            "worker.api.request",
            "Spring API 요청을 전송합니다.",
            method="POST",
            path=path,
            forwardedRequestId=request_headers["X-Request-Id"],
        )
        started_at = monotonic()
        try:
            response = self._session.post(url, json=payload, headers=request_headers, timeout=30)
        except requests.RequestException as exc:
            log_warning(
                logger,
                "worker.api.failed",
                "Spring API 요청이 전송되지 못했습니다.",
                method="POST",
                path=path,
                latencyMs=self._elapsed_millis(started_at),
                errorCode="SPRING_API_REQUEST_FAILED",
                error=str(exc),
            )
            raise RetryableWorkerError(f"Spring API 호출 실패: {exc}") from exc

        return self._validate_response(
            response,
            path=path,
            method="POST",
            idempotent_conflict_as_success=idempotent_conflict_as_success,
            latency_ms=self._elapsed_millis(started_at),
        )

    def _get(self, path: str) -> ApiEnvelope:
        url = settings.spring_api_base_url.rstrip("/") + path
        request_headers = self._build_request_headers()
        log_info(
            logger,
            "worker.api.request",
            "Spring API 요청을 전송합니다.",
            method="GET",
            path=path,
            forwardedRequestId=request_headers["X-Request-Id"],
        )
        started_at = monotonic()
        try:
            response = self._session.get(url, headers=request_headers, timeout=30)
        except requests.RequestException as exc:
            log_warning(
                logger,
                "worker.api.failed",
                "Spring API 요청이 전송되지 못했습니다.",
                method="GET",
                path=path,
                latencyMs=self._elapsed_millis(started_at),
                errorCode="SPRING_API_REQUEST_FAILED",
                error=str(exc),
            )
            raise RetryableWorkerError(f"Spring API 호출 실패: {exc}") from exc

        return self._validate_response(
            response,
            path=path,
            method="GET",
            idempotent_conflict_as_success=False,
            latency_ms=self._elapsed_millis(started_at),
        )

    def _validate_response(
        self,
        response: Any,
        *,
        path: str,
        method: str,
        idempotent_conflict_as_success: bool,
        latency_ms: int,
    ) -> ApiEnvelope:
        if response.status_code == 409 and idempotent_conflict_as_success:
            log_info(
                logger,
                "worker.api.response",
                "Spring API 멱등 충돌을 성공으로 처리했습니다.",
                method=method,
                path=path,
                status=response.status_code,
                latencyMs=latency_ms,
                idempotentConflictAsSuccess=True,
                responseCode="IDEMPOTENT_CONFLICT",
            )
            return ApiEnvelope(
                isSuccess=True,
                code="IDEMPOTENT_CONFLICT",
                message="409 conflict를 멱등 성공으로 처리했습니다.",
                result=None,
                error=None,
            )
        if response.status_code >= 500:
            log_warning(
                logger,
                "worker.api.failed",
                "Spring API 서버 오류를 수신했습니다.",
                method=method,
                path=path,
                status=response.status_code,
                latencyMs=latency_ms,
                errorCode="SPRING_API_SERVER_ERROR",
            )
            raise RetryableWorkerError(f"Spring API 서버 오류: {response.status_code}")

        try:
            payload_data = response.json()
        except Exception as exc:
            if 400 <= response.status_code < 500:
                log_error(
                    logger,
                    "worker.api.failed",
                    "Spring API 클라이언트 오류 응답을 파싱하지 못했습니다.",
                    method=method,
                    path=path,
                    status=response.status_code,
                    latencyMs=latency_ms,
                    errorCode="SPRING_API_CLIENT_ERROR",
                )
                raise NonRetryableWorkerError(f"Spring API 클라이언트 오류: {response.status_code}") from exc
            log_warning(
                logger,
                "worker.api.failed",
                "Spring API 응답 파싱에 실패했습니다.",
                method=method,
                path=path,
                status=response.status_code,
                latencyMs=latency_ms,
                errorCode="SPRING_API_RESPONSE_PARSE_FAILED",
            )
            raise RetryableWorkerError("Spring API 응답 파싱 실패") from exc

        try:
            envelope = ApiEnvelope.model_validate(payload_data)
        except Exception as exc:
            if 400 <= response.status_code < 500:
                log_error(
                    logger,
                    "worker.api.failed",
                    "Spring API 클라이언트 오류 응답이 스키마와 다릅니다.",
                    method=method,
                    path=path,
                    status=response.status_code,
                    latencyMs=latency_ms,
                    errorCode="SPRING_API_CLIENT_ERROR",
                )
                raise NonRetryableWorkerError(f"Spring API 클라이언트 오류: {response.status_code}") from exc
            log_warning(
                logger,
                "worker.api.failed",
                "Spring API 응답 스키마 검증에 실패했습니다.",
                method=method,
                path=path,
                status=response.status_code,
                latencyMs=latency_ms,
                errorCode="SPRING_API_RESPONSE_SCHEMA_INVALID",
            )
            raise RetryableWorkerError("Spring API 응답 파싱 실패") from exc

        if not envelope.isSuccess:
            log_error(
                logger,
                "worker.api.failed",
                "Spring API가 비성공 응답을 반환했습니다.",
                method=method,
                path=path,
                status=response.status_code,
                latencyMs=latency_ms,
                responseCode=envelope.code,
                errorCode=envelope.code,
            )
            raise NonRetryableWorkerError(str(envelope.error or envelope.message))

        log_info(
            logger,
            "worker.api.response",
            "Spring API 응답을 수신했습니다.",
            method=method,
            path=path,
            status=response.status_code,
            latencyMs=latency_ms,
            responseCode=envelope.code,
        )
        return envelope

    def _build_request_headers(self) -> dict[str, str]:
        return {"X-Request-Id": ensure_request_id()}

    def _parse_result(self, envelope: ApiEnvelope, expected_type: type[T] | Any) -> T:
        adapter = TypeAdapter(expected_type)
        return adapter.validate_python(envelope.result)

    def _elapsed_millis(self, started_at: float) -> int:
        return max(int((monotonic() - started_at) * 1000), 0)
