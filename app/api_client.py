from __future__ import annotations

import logging
from time import monotonic
from typing import Any, TypeVar

import httpx
import requests
from pydantic import TypeAdapter

from app.config import settings
from app.logging_utils import ensure_request_id, log_error, log_info, log_warning
from app.metrics import observe_internal_api
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
        default_headers = {
            "Content-Type": "application/json",
            "X-Internal-Api-Key": settings.spring_internal_api_key,
        }
        self._session = requests.Session()
        self._session.headers.update(default_headers)
        self._async_client = httpx.AsyncClient(
            base_url=settings.spring_api_base_url.rstrip("/"),
            headers=default_headers,
            timeout=30.0,
        )

    def mark_job_posting_running(self, task_id: str, request: JobPostingWorkerRunningRequest) -> None:
        self._post(
            f"/api/internal/worker/job-postings/tasks/{task_id}/running",
            request.model_dump(mode="json"),
            task_type="JOB_POSTING_INGEST",
            endpoint="job_posting_running",
            method="POST",
        )

    def complete_task(self, task_id: str, result: JobPostingIngestResponse) -> JobPostingIngestResponse | None:
        payload = result.model_dump(mode="json")
        response = self._post(
            f"/api/internal/worker/job-postings/tasks/{task_id}/complete",
            payload,
            task_type="JOB_POSTING_INGEST",
            endpoint="job_posting_complete",
            method="POST",
        )
        return self._parse_optional_result(response, JobPostingIngestResponse)

    def store_job_posting_result(self, task_id: str, request: JobPostingWorkerResultStoreRequest) -> None:
        self._post(
            f"/api/internal/worker/job-postings/tasks/{task_id}/result",
            request.model_dump(mode="json"),
            task_type="JOB_POSTING_INGEST",
            endpoint="job_posting_result",
            method="POST",
        )

    def retry_job_posting_task(self, task_id: str, request: JobPostingWorkerRetryRequest) -> None:
        self._post(
            f"/api/internal/worker/job-postings/tasks/{task_id}/retry",
            request.model_dump(mode="json"),
            task_type="JOB_POSTING_INGEST",
            endpoint="job_posting_retry",
            method="POST",
        )

    def fail_job_posting_task(self, task_id: str, request: JobPostingWorkerFailureRequest) -> None:
        self._post(
            f"/api/internal/worker/job-postings/tasks/{task_id}/failed",
            request.model_dump(mode="json"),
            task_type="JOB_POSTING_INGEST",
            endpoint="job_posting_failed",
            method="POST",
        )

    def get_job_posting_task(self, task_id: str) -> JobPostingTaskStatusResponse:
        response = self._get(
            f"/api/internal/worker/job-postings/tasks/{task_id}",
            task_type="JOB_POSTING_INGEST",
            endpoint="job_posting_task_status",
            method="GET",
        )
        return self._parse_result(response, JobPostingTaskStatusResponse)

    def get_context(self, user_id: int, image_object_key: str | None) -> JobPostingWorkerContextResponse:
        payload = JobPostingWorkerContextRequest(userId=user_id, imageObjectKey=image_object_key).model_dump(mode="json")
        response = self._post(
            "/api/internal/worker/job-postings/ingest/context",
            payload,
            task_type="JOB_POSTING_INGEST",
            endpoint="job_posting_context",
            method="POST",
        )
        return self._parse_result(response, JobPostingWorkerContextResponse)

    def get_candidates(
        self, extracted: JobPostingExtractResponse
    ) -> list[JobPostingClassificationCandidateResponse]:
        response = self._post(
            "/api/internal/worker/job-postings/classification/candidates",
            extracted.model_dump(mode="json"),
            task_type="JOB_POSTING_INGEST",
            endpoint="job_posting_candidates",
            method="POST",
        )
        return self._parse_result(response, list[JobPostingClassificationCandidateResponse])

    def finalize(self, request: JobPostingWorkerFinalizeRequest) -> None:
        self._post(
            "/api/internal/worker/job-postings/ingest/finalize",
            request.model_dump(mode="json"),
            task_type="JOB_POSTING_INGEST",
            endpoint="job_posting_finalize",
            method="POST",
        )

    def mark_analysis_running(self, task_id: str, request: AnalysisWorkerRunningRequest) -> None:
        self._post(
            f"/api/internal/worker/analysis/tasks/{task_id}/running",
            request.model_dump(mode="json"),
            task_type="ANALYSIS",
            endpoint="analysis_running",
            method="POST",
        )

    def get_analysis_context(self, request: AnalysisWorkerContextRequest) -> AnalysisWorkerContextResponse:
        response = self._post(
            "/api/internal/worker/analysis/context",
            request.model_dump(mode="json"),
            task_type="ANALYSIS",
            endpoint="analysis_context",
            method="POST",
        )
        return self._parse_result(response, AnalysisWorkerContextResponse)

    def retry_analysis_task(self, task_id: str, request: AnalysisWorkerRetryRequest) -> None:
        self._post(
            f"/api/internal/worker/analysis/tasks/{task_id}/retry",
            request.model_dump(mode="json"),
            task_type="ANALYSIS",
            endpoint="analysis_retry",
            method="POST",
        )

    def fail_analysis_task(self, task_id: str, request: AnalysisWorkerFailureRequest) -> None:
        self._post(
            f"/api/internal/worker/analysis/tasks/{task_id}/failed",
            request.model_dump(mode="json"),
            task_type="ANALYSIS",
            endpoint="analysis_failed",
            method="POST",
        )

    def complete_analysis_task(self, task_id: str, request: AnalysisWorkerCompleteRequest) -> None:
        self._post(
            f"/api/internal/worker/analysis/tasks/{task_id}/complete",
            request.model_dump(mode="json"),
            task_type="ANALYSIS",
            endpoint="analysis_complete",
            method="POST",
        )

    def store_analysis_result(self, task_id: str, request: AnalysisWorkerResultStoreRequest) -> None:
        self._post(
            f"/api/internal/worker/analysis/tasks/{task_id}/result",
            request.model_dump(mode="json"),
            task_type="ANALYSIS",
            endpoint="analysis_result",
            method="POST",
        )

    def get_analysis_task(self, task_id: str) -> AnalysisTaskStatusResponse:
        response = self._get(
            f"/api/internal/worker/analysis/tasks/{task_id}",
            task_type="ANALYSIS",
            endpoint="analysis_task_status",
            method="GET",
        )
        return self._parse_result(response, AnalysisTaskStatusResponse)

    async def aclose(self) -> None:
        await self._async_client.aclose()

    async def mark_job_posting_running_async(self, task_id: str, request: JobPostingWorkerRunningRequest) -> None:
        await self._post_async(
            f"/api/internal/worker/job-postings/tasks/{task_id}/running",
            request.model_dump(mode="json"),
            task_type="JOB_POSTING_INGEST",
            endpoint="job_posting_running",
            method="POST",
        )

    async def complete_task_async(self, task_id: str, result: JobPostingIngestResponse) -> JobPostingIngestResponse | None:
        response = await self._post_async(
            f"/api/internal/worker/job-postings/tasks/{task_id}/complete",
            result.model_dump(mode="json"),
            task_type="JOB_POSTING_INGEST",
            endpoint="job_posting_complete",
            method="POST",
        )
        return self._parse_optional_result(response, JobPostingIngestResponse)

    async def store_job_posting_result_async(self, task_id: str, request: JobPostingWorkerResultStoreRequest) -> None:
        await self._post_async(
            f"/api/internal/worker/job-postings/tasks/{task_id}/result",
            request.model_dump(mode="json"),
            task_type="JOB_POSTING_INGEST",
            endpoint="job_posting_result",
            method="POST",
        )

    async def retry_job_posting_task_async(self, task_id: str, request: JobPostingWorkerRetryRequest) -> None:
        await self._post_async(
            f"/api/internal/worker/job-postings/tasks/{task_id}/retry",
            request.model_dump(mode="json"),
            task_type="JOB_POSTING_INGEST",
            endpoint="job_posting_retry",
            method="POST",
        )

    async def fail_job_posting_task_async(self, task_id: str, request: JobPostingWorkerFailureRequest) -> None:
        await self._post_async(
            f"/api/internal/worker/job-postings/tasks/{task_id}/failed",
            request.model_dump(mode="json"),
            task_type="JOB_POSTING_INGEST",
            endpoint="job_posting_failed",
            method="POST",
        )

    async def get_job_posting_task_async(self, task_id: str) -> JobPostingTaskStatusResponse:
        response = await self._get_async(
            f"/api/internal/worker/job-postings/tasks/{task_id}",
            task_type="JOB_POSTING_INGEST",
            endpoint="job_posting_task_status",
            method="GET",
        )
        return self._parse_result(response, JobPostingTaskStatusResponse)

    async def get_context_async(self, user_id: int, image_object_key: str | None) -> JobPostingWorkerContextResponse:
        response = await self._post_async(
            "/api/internal/worker/job-postings/ingest/context",
            JobPostingWorkerContextRequest(userId=user_id, imageObjectKey=image_object_key).model_dump(mode="json"),
            task_type="JOB_POSTING_INGEST",
            endpoint="job_posting_context",
            method="POST",
        )
        return self._parse_result(response, JobPostingWorkerContextResponse)

    async def get_candidates_async(
        self,
        extracted: JobPostingExtractResponse,
    ) -> list[JobPostingClassificationCandidateResponse]:
        response = await self._post_async(
            "/api/internal/worker/job-postings/classification/candidates",
            extracted.model_dump(mode="json"),
            task_type="JOB_POSTING_INGEST",
            endpoint="job_posting_candidates",
            method="POST",
        )
        return self._parse_result(response, list[JobPostingClassificationCandidateResponse])

    async def finalize_async(self, request: JobPostingWorkerFinalizeRequest) -> None:
        await self._post_async(
            "/api/internal/worker/job-postings/ingest/finalize",
            request.model_dump(mode="json"),
            task_type="JOB_POSTING_INGEST",
            endpoint="job_posting_finalize",
            method="POST",
        )

    async def mark_analysis_running_async(self, task_id: str, request: AnalysisWorkerRunningRequest) -> None:
        await self._post_async(
            f"/api/internal/worker/analysis/tasks/{task_id}/running",
            request.model_dump(mode="json"),
            task_type="ANALYSIS",
            endpoint="analysis_running",
            method="POST",
        )

    async def get_analysis_context_async(self, request: AnalysisWorkerContextRequest) -> AnalysisWorkerContextResponse:
        response = await self._post_async(
            "/api/internal/worker/analysis/context",
            request.model_dump(mode="json"),
            task_type="ANALYSIS",
            endpoint="analysis_context",
            method="POST",
        )
        return self._parse_result(response, AnalysisWorkerContextResponse)

    async def retry_analysis_task_async(self, task_id: str, request: AnalysisWorkerRetryRequest) -> None:
        await self._post_async(
            f"/api/internal/worker/analysis/tasks/{task_id}/retry",
            request.model_dump(mode="json"),
            task_type="ANALYSIS",
            endpoint="analysis_retry",
            method="POST",
        )

    async def fail_analysis_task_async(self, task_id: str, request: AnalysisWorkerFailureRequest) -> None:
        await self._post_async(
            f"/api/internal/worker/analysis/tasks/{task_id}/failed",
            request.model_dump(mode="json"),
            task_type="ANALYSIS",
            endpoint="analysis_failed",
            method="POST",
        )

    async def complete_analysis_task_async(self, task_id: str, request: AnalysisWorkerCompleteRequest) -> None:
        await self._post_async(
            f"/api/internal/worker/analysis/tasks/{task_id}/complete",
            request.model_dump(mode="json"),
            task_type="ANALYSIS",
            endpoint="analysis_complete",
            method="POST",
        )

    async def store_analysis_result_async(self, task_id: str, request: AnalysisWorkerResultStoreRequest) -> None:
        await self._post_async(
            f"/api/internal/worker/analysis/tasks/{task_id}/result",
            request.model_dump(mode="json"),
            task_type="ANALYSIS",
            endpoint="analysis_result",
            method="POST",
        )

    async def get_analysis_task_async(self, task_id: str) -> AnalysisTaskStatusResponse:
        response = await self._get_async(
            f"/api/internal/worker/analysis/tasks/{task_id}",
            task_type="ANALYSIS",
            endpoint="analysis_task_status",
            method="GET",
        )
        return self._parse_result(response, AnalysisTaskStatusResponse)

    def _post(
        self,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        task_type: str,
        endpoint: str,
        method: str,
    ) -> ApiEnvelope:
        return self._request(
            http_method="POST",
            path=path,
            payload=payload,
            task_type=task_type,
            endpoint=endpoint,
            method=method,
        )

    def _get(self, path: str, *, task_type: str, endpoint: str, method: str) -> ApiEnvelope:
        return self._request(
            http_method="GET",
            path=path,
            payload=None,
            task_type=task_type,
            endpoint=endpoint,
            method=method,
        )

    async def _post_async(
        self,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        task_type: str,
        endpoint: str,
        method: str,
    ) -> ApiEnvelope:
        return await self._request_async(
            http_method="POST",
            path=path,
            payload=payload,
            task_type=task_type,
            endpoint=endpoint,
            method=method,
        )

    async def _get_async(self, path: str, *, task_type: str, endpoint: str, method: str) -> ApiEnvelope:
        return await self._request_async(
            http_method="GET",
            path=path,
            payload=None,
            task_type=task_type,
            endpoint=endpoint,
            method=method,
        )

    def _request(
        self,
        *,
        http_method: str,
        path: str,
        payload: dict[str, Any] | None,
        task_type: str,
        endpoint: str,
        method: str,
    ) -> ApiEnvelope:
        url = settings.spring_api_base_url.rstrip("/") + path
        request_headers = self._build_request_headers()
        log_info(
            logger,
            "worker.api.request",
            "Spring API 요청을 전송합니다.",
            method=http_method,
            path=path,
            forwardedRequestId=request_headers["X-Request-Id"],
        )
        started_at = monotonic()
        try:
            response = self._send_request(
                http_method=http_method,
                url=url,
                payload=payload,
                headers=request_headers,
            )
        except requests.RequestException as exc:
            latency_ms = self._elapsed_millis(started_at)
            observe_internal_api(task_type, endpoint, method, "failed", latency_ms / 1000)
            log_warning(
                logger,
                "worker.api.failed",
                "Spring API 요청이 전송되지 못했습니다.",
                method=http_method,
                path=path,
                latencyMs=latency_ms,
                errorCode="SPRING_API_REQUEST_FAILED",
                error=str(exc),
            )
            raise RetryableWorkerError(f"Spring API 호출 실패: {exc}") from exc

        latency_ms = self._elapsed_millis(started_at)
        try:
            envelope = self._validate_response(
                response,
                path=path,
                method=method,
                latency_ms=latency_ms,
            )
        except Exception:
            observe_internal_api(task_type, endpoint, method, "failed", latency_ms / 1000)
            raise
        observe_internal_api(task_type, endpoint, method, "succeeded", latency_ms / 1000)
        return envelope

    async def _request_async(
        self,
        *,
        http_method: str,
        path: str,
        payload: dict[str, Any] | None,
        task_type: str,
        endpoint: str,
        method: str,
    ) -> ApiEnvelope:
        request_headers = self._build_request_headers()
        log_info(
            logger,
            "worker.api.request",
            "Spring API 요청을 전송합니다.",
            method=http_method,
            path=path,
            forwardedRequestId=request_headers["X-Request-Id"],
        )
        started_at = monotonic()
        try:
            response = await self._send_request_async(
                http_method=http_method,
                path=path,
                payload=payload,
                headers=request_headers,
            )
        except httpx.RequestError as exc:
            latency_ms = self._elapsed_millis(started_at)
            observe_internal_api(task_type, endpoint, method, "failed", latency_ms / 1000)
            log_warning(
                logger,
                "worker.api.failed",
                "Spring API 요청이 전송되지 못했습니다.",
                method=http_method,
                path=path,
                latencyMs=latency_ms,
                errorCode="SPRING_API_REQUEST_FAILED",
                error=str(exc),
            )
            raise RetryableWorkerError(f"Spring API 호출 실패: {exc}") from exc

        latency_ms = self._elapsed_millis(started_at)
        try:
            envelope = self._validate_response(
                response,
                path=path,
                method=method,
                latency_ms=latency_ms,
            )
        except Exception:
            observe_internal_api(task_type, endpoint, method, "failed", latency_ms / 1000)
            raise
        observe_internal_api(task_type, endpoint, method, "succeeded", latency_ms / 1000)
        return envelope

    def _send_request(
        self,
        *,
        http_method: str,
        url: str,
        payload: dict[str, Any] | None,
        headers: dict[str, str],
    ):
        if http_method == "POST":
            return self._session.post(url, json=payload, headers=headers, timeout=30)
        return self._session.get(url, headers=headers, timeout=30)

    async def _send_request_async(
        self,
        *,
        http_method: str,
        path: str,
        payload: dict[str, Any] | None,
        headers: dict[str, str],
    ) -> httpx.Response:
        if http_method == "POST":
            return await self._async_client.post(path, json=payload, headers=headers)
        return await self._async_client.get(path, headers=headers)

    def _validate_response(
        self,
        response: Any,
        *,
        path: str,
        method: str,
        latency_ms: int,
    ) -> ApiEnvelope:
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

    def _parse_optional_result(self, envelope: ApiEnvelope, expected_type: type[T] | Any) -> T | None:
        if envelope.result is None:
            return None
        return self._parse_result(envelope, expected_type)

    def _elapsed_millis(self, started_at: float) -> int:
        return max(int((monotonic() - started_at) * 1000), 0)
