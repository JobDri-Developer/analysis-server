from __future__ import annotations

from typing import Any, TypeVar

import requests
from pydantic import BaseModel, TypeAdapter

from app.config import settings
from app.schemas import (
    AnalysisTaskStatusResponse,
    AnalysisWorkerCompleteRequest,
    AnalysisWorkerContextRequest,
    AnalysisWorkerContextResponse,
    AnalysisWorkerFailureRequest,
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
    JobPostingWorkerRetryRequest,
    JobPostingWorkerRunningRequest,
    NonRetryableWorkerError,
    RetryableWorkerError,
)

T = TypeVar("T")


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

    def retry_job_posting_task(self, task_id: str, request: JobPostingWorkerRetryRequest) -> None:
        self._post(
            f"/api/internal/worker/job-postings/tasks/{task_id}/retry",
            request.model_dump(mode="json"),
        )

    def fail_job_posting_task(self, task_id: str, request: JobPostingWorkerFailureRequest) -> None:
        payload = request.model_dump(mode="json")
        self._post(f"/api/internal/worker/job-postings/tasks/{task_id}/failed", payload)

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

    def finalize(self, request: JobPostingWorkerFinalizeRequest) -> JobPostingIngestResponse:
        response = self._post(
            "/api/internal/worker/job-postings/ingest/finalize",
            request.model_dump(mode="json"),
        )
        return self._parse_result(response, JobPostingIngestResponse)

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
        )

    def get_analysis_task(self, task_id: str) -> AnalysisTaskStatusResponse:
        response = self._get(f"/api/internal/worker/analysis/tasks/{task_id}")
        return self._parse_result(response, AnalysisTaskStatusResponse)

    def _post(self, path: str, payload: dict[str, Any] | None = None) -> ApiEnvelope:
        url = settings.spring_api_base_url.rstrip("/") + path
        try:
            response = self._session.post(url, json=payload, timeout=30)
        except requests.RequestException as exc:
            raise RetryableWorkerError(f"Spring API 호출 실패: {exc}") from exc

        if response.status_code >= 500:
            raise RetryableWorkerError(f"Spring API 서버 오류: {response.status_code}")

        try:
            envelope = ApiEnvelope.model_validate(response.json())
        except Exception as exc:
            raise RetryableWorkerError("Spring API 응답 파싱 실패") from exc

        if not envelope.isSuccess:
            raise NonRetryableWorkerError(str(envelope.error or envelope.message))
        return envelope

    def _get(self, path: str) -> ApiEnvelope:
        url = settings.spring_api_base_url.rstrip("/") + path
        try:
            response = self._session.get(url, timeout=30)
        except requests.RequestException as exc:
            raise RetryableWorkerError(f"Spring API 호출 실패: {exc}") from exc

        if response.status_code >= 500:
            raise RetryableWorkerError(f"Spring API 서버 오류: {response.status_code}")

        try:
            envelope = ApiEnvelope.model_validate(response.json())
        except Exception as exc:
            raise RetryableWorkerError("Spring API 응답 파싱 실패") from exc

        if not envelope.isSuccess:
            raise NonRetryableWorkerError(str(envelope.error or envelope.message))
        return envelope

    def _parse_result(self, envelope: ApiEnvelope, expected_type: type[T] | Any) -> T:
        adapter = TypeAdapter(expected_type)
        return adapter.validate_python(envelope.result)
