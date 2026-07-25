from __future__ import annotations

import json
import logging
from time import monotonic
from typing import Any

from pydantic import ValidationError
from openai import APIConnectionError, APIStatusError, APITimeoutError, BadRequestError, OpenAI, RateLimitError

from app.async_utils import await_if_needed
from app.error_codes import FailureReasonCode, classify_openai_failure
from app.openai_prompts import (
    build_analysis_prompt,
    build_job_posting_classification_prompt,
    build_job_posting_extract_prompt,
    build_job_posting_generation_prompt,
)
from app.openai_response_parser import (
    build_job_posting_classification_fallback,
    build_job_posting_generate_fallback,
    parse_analysis_response,
    parse_job_posting_classification_response,
    parse_job_posting_extract_response,
    parse_job_posting_generate_response,
)

try:
    from openai import AsyncOpenAI
except ImportError:  # pragma: no cover - compatibility fallback for lightweight test stubs
    AsyncOpenAI = None  # type: ignore[assignment]

from app.config import settings
from app.logging_utils import log_info, log_warning
from app.metrics import increment_llm_request_error, observe_llm_request
from app.schemas import (
    AnalysisLlmResponse,
    AnalysisWorkerContextResponse,
    JobPostingClassificationCandidateResponse,
    JobPostingClassificationResultResponse,
    JobPostingExtractResponse,
    JobPostingGenerateResponse,
    NonRetryableWorkerError,
    RetryableWorkerError,
)

logger = logging.getLogger(__name__)


def _openai_timeout_seconds() -> float:
    return max(settings.analysis_queue_timeout_millis / 1000, 1.0)


class JobPostingOpenAiWorker:
    def __init__(self) -> None:
        timeout = _openai_timeout_seconds()
        self._client = OpenAI(api_key=settings.openai_api_key, timeout=timeout)
        self._async_client = (
            AsyncOpenAI(api_key=settings.openai_api_key, timeout=timeout)
            if AsyncOpenAI is not None
            else OpenAI(api_key=settings.openai_api_key, timeout=timeout)
        )
        self._model = settings.openai_job_posting_model
        self._task_type = "JOB_POSTING_INGEST"

    def extract(self, raw_text: str | None, image_url: str | None) -> JobPostingExtractResponse:
        operation = "job-posting-extract"
        prompt = build_job_posting_extract_prompt(raw_text or "", image_url is not None)
        content = [{"type": "input_text", "text": prompt}]
        if image_url:
            content.append({"type": "input_image", "image_url": image_url})

        started_at = monotonic()
        log_info(
            logger,
            "openai.extract.started",
            "OpenAI extract 호출을 시작합니다.",
            model=self._model,
            hasImage=image_url is not None,
        )
        response = self._create_response(
            input_payload=[{"role": "user", "content": content}],
            temperature=0.1,
            operation="extract",
            event_prefix="openai.extract",
        )
        try:
            result = parse_job_posting_extract_response(response.output_text)
        except (ValidationError, json.JSONDecodeError, TypeError, ValueError) as exc:
            observe_llm_request(self._task_type, operation, "failed", self._elapsed_seconds(started_at))
            increment_llm_request_error(self._task_type, operation, FailureReasonCode.VALIDATION_ERROR.value)
            log_warning(
                logger,
                "openai.extract.failed",
                "OpenAI extract 응답 검증에 실패했습니다.",
                model=self._model,
                latencyMs=self._elapsed_millis(started_at),
                openaiRequestId=self._extract_request_id(response),
                errorCode=FailureReasonCode.VALIDATION_ERROR.value,
                error=str(exc),
            )
            raise NonRetryableWorkerError(
                f"OpenAI extract 응답 검증 실패: {exc}",
                failure_reason=FailureReasonCode.VALIDATION_ERROR.value,
                openai_request_id=self._extract_request_id(response),
            ) from exc
        except (RetryableWorkerError, NonRetryableWorkerError):
            observe_llm_request(self._task_type, operation, "failed", self._elapsed_seconds(started_at))
            raise
        observe_llm_request(self._task_type, operation, "succeeded", self._elapsed_seconds(started_at))
        log_info(
            logger,
            "openai.extract.completed",
            "OpenAI extract 호출이 완료되었습니다.",
            model=self._model,
            latencyMs=self._elapsed_millis(started_at),
            openaiRequestId=self._extract_request_id(response),
        )
        return result

    async def extract_async(self, raw_text: str | None, image_url: str | None) -> JobPostingExtractResponse:
        operation = "job-posting-extract"
        prompt = build_job_posting_extract_prompt(raw_text or "", image_url is not None)
        content = [{"type": "input_text", "text": prompt}]
        if image_url:
            content.append({"type": "input_image", "image_url": image_url})

        started_at = monotonic()
        log_info(
            logger,
            "openai.extract.started",
            "OpenAI extract 호출을 시작합니다.",
            model=self._model,
            hasImage=image_url is not None,
        )
        response = await self._create_response_async(
            input_payload=[{"role": "user", "content": content}],
            temperature=0.1,
            operation="extract",
            event_prefix="openai.extract",
        )
        try:
            result = parse_job_posting_extract_response(response.output_text)
        except (ValidationError, json.JSONDecodeError, TypeError, ValueError) as exc:
            observe_llm_request(self._task_type, operation, "failed", self._elapsed_seconds(started_at))
            increment_llm_request_error(self._task_type, operation, FailureReasonCode.VALIDATION_ERROR.value)
            log_warning(
                logger,
                "openai.extract.failed",
                "OpenAI extract 응답 검증에 실패했습니다.",
                model=self._model,
                latencyMs=self._elapsed_millis(started_at),
                openaiRequestId=self._extract_request_id(response),
                errorCode=FailureReasonCode.VALIDATION_ERROR.value,
                error=str(exc),
            )
            raise NonRetryableWorkerError(
                f"OpenAI extract 응답 검증 실패: {exc}",
                failure_reason=FailureReasonCode.VALIDATION_ERROR.value,
                openai_request_id=self._extract_request_id(response),
            ) from exc
        except (RetryableWorkerError, NonRetryableWorkerError):
            observe_llm_request(self._task_type, operation, "failed", self._elapsed_seconds(started_at))
            raise
        observe_llm_request(self._task_type, operation, "succeeded", self._elapsed_seconds(started_at))
        log_info(
            logger,
            "openai.extract.completed",
            "OpenAI extract 호출이 완료되었습니다.",
            model=self._model,
            latencyMs=self._elapsed_millis(started_at),
            openaiRequestId=self._extract_request_id(response),
        )
        return result

    def classify(
        self,
        extracted: JobPostingExtractResponse,
        candidates: list[JobPostingClassificationCandidateResponse],
    ) -> JobPostingClassificationResultResponse:
        operation = "job-posting-classify"
        prompt = build_job_posting_classification_prompt(extracted, candidates)
        started_at = monotonic()
        log_info(
            logger,
            "openai.classify.started",
            "OpenAI classify 호출을 시작합니다.",
            model=self._model,
            candidateCount=len(candidates),
        )
        response = self._create_response(
            input_payload=prompt,
            temperature=0.1,
            operation="classify",
            event_prefix="openai.classify",
        )
        try:
            result = parse_job_posting_classification_response(response.output_text)
        except (ValidationError, json.JSONDecodeError, TypeError, ValueError) as exc:
            observe_llm_request(self._task_type, operation, "fallback", self._elapsed_seconds(started_at))
            increment_llm_request_error(self._task_type, operation, FailureReasonCode.VALIDATION_ERROR.value)
            log_warning(
                logger,
                "openai.classify.fallback",
                "OpenAI classify 응답 검증에 실패해 fallback을 사용합니다.",
                model=self._model,
                latencyMs=self._elapsed_millis(started_at),
                openaiRequestId=self._extract_request_id(response),
                errorCode=FailureReasonCode.VALIDATION_ERROR.value,
                error=str(exc),
            )
            return build_job_posting_classification_fallback(candidates)
        except (RetryableWorkerError, NonRetryableWorkerError):
            observe_llm_request(self._task_type, operation, "failed", self._elapsed_seconds(started_at))
            raise
        observe_llm_request(self._task_type, operation, "succeeded", self._elapsed_seconds(started_at))
        log_info(
            logger,
            "openai.classify.completed",
            "OpenAI classify 호출이 완료되었습니다.",
            model=self._model,
            latencyMs=self._elapsed_millis(started_at),
            openaiRequestId=self._extract_request_id(response),
        )
        return result

    async def classify_async(
        self,
        extracted: JobPostingExtractResponse,
        candidates: list[JobPostingClassificationCandidateResponse],
    ) -> JobPostingClassificationResultResponse:
        operation = "job-posting-classify"
        prompt = build_job_posting_classification_prompt(extracted, candidates)
        started_at = monotonic()
        log_info(
            logger,
            "openai.classify.started",
            "OpenAI classify 호출을 시작합니다.",
            model=self._model,
            candidateCount=len(candidates),
        )
        response = await self._create_response_async(
            input_payload=prompt,
            temperature=0.1,
            operation="classify",
            event_prefix="openai.classify",
        )
        try:
            result = parse_job_posting_classification_response(response.output_text)
        except (ValidationError, json.JSONDecodeError, TypeError, ValueError) as exc:
            observe_llm_request(self._task_type, operation, "fallback", self._elapsed_seconds(started_at))
            increment_llm_request_error(self._task_type, operation, FailureReasonCode.VALIDATION_ERROR.value)
            log_warning(
                logger,
                "openai.classify.fallback",
                "OpenAI classify 응답 검증에 실패해 fallback을 사용합니다.",
                model=self._model,
                latencyMs=self._elapsed_millis(started_at),
                openaiRequestId=self._extract_request_id(response),
                errorCode=FailureReasonCode.VALIDATION_ERROR.value,
                error=str(exc),
            )
            return build_job_posting_classification_fallback(candidates)
        except (RetryableWorkerError, NonRetryableWorkerError):
            observe_llm_request(self._task_type, operation, "failed", self._elapsed_seconds(started_at))
            raise
        observe_llm_request(self._task_type, operation, "succeeded", self._elapsed_seconds(started_at))
        log_info(
            logger,
            "openai.classify.completed",
            "OpenAI classify 호출이 완료되었습니다.",
            model=self._model,
            latencyMs=self._elapsed_millis(started_at),
            openaiRequestId=self._extract_request_id(response),
        )
        return result

    def generate(
        self,
        extracted: JobPostingExtractResponse,
        classification: JobPostingClassificationResultResponse,
    ) -> JobPostingGenerateResponse:
        operation = "job-posting-generate"
        prompt = build_job_posting_generation_prompt(extracted, classification)
        started_at = monotonic()
        log_info(
            logger,
            "openai.generate.started",
            "OpenAI generate 호출을 시작합니다.",
            model=self._model,
        )
        response = self._create_response(
            input_payload=prompt,
            temperature=0.7,
            operation="generate",
            event_prefix="openai.generate",
        )
        try:
            result = parse_job_posting_generate_response(response.output_text)
        except (ValidationError, json.JSONDecodeError, TypeError, ValueError) as exc:
            observe_llm_request(self._task_type, operation, "fallback", self._elapsed_seconds(started_at))
            increment_llm_request_error(self._task_type, operation, FailureReasonCode.VALIDATION_ERROR.value)
            log_warning(
                logger,
                "openai.generate.fallback",
                "OpenAI generate 응답 검증에 실패해 fallback을 사용합니다.",
                model=self._model,
                latencyMs=self._elapsed_millis(started_at),
                openaiRequestId=self._extract_request_id(response),
                errorCode=FailureReasonCode.VALIDATION_ERROR.value,
                error=str(exc),
            )
            return build_job_posting_generate_fallback(extracted)
        except (RetryableWorkerError, NonRetryableWorkerError):
            observe_llm_request(self._task_type, operation, "failed", self._elapsed_seconds(started_at))
            raise
        observe_llm_request(self._task_type, operation, "succeeded", self._elapsed_seconds(started_at))
        log_info(
            logger,
            "openai.generate.completed",
            "OpenAI generate 호출이 완료되었습니다.",
            model=self._model,
            latencyMs=self._elapsed_millis(started_at),
            openaiRequestId=self._extract_request_id(response),
        )
        return result

    async def generate_async(
        self,
        extracted: JobPostingExtractResponse,
        classification: JobPostingClassificationResultResponse,
    ) -> JobPostingGenerateResponse:
        operation = "job-posting-generate"
        prompt = build_job_posting_generation_prompt(extracted, classification)
        started_at = monotonic()
        log_info(
            logger,
            "openai.generate.started",
            "OpenAI generate 호출을 시작합니다.",
            model=self._model,
        )
        response = await self._create_response_async(
            input_payload=prompt,
            temperature=0.7,
            operation="generate",
            event_prefix="openai.generate",
        )
        try:
            result = parse_job_posting_generate_response(response.output_text)
        except (ValidationError, json.JSONDecodeError, TypeError, ValueError) as exc:
            observe_llm_request(self._task_type, operation, "fallback", self._elapsed_seconds(started_at))
            increment_llm_request_error(self._task_type, operation, FailureReasonCode.VALIDATION_ERROR.value)
            log_warning(
                logger,
                "openai.generate.fallback",
                "OpenAI generate 응답 검증에 실패해 fallback을 사용합니다.",
                model=self._model,
                latencyMs=self._elapsed_millis(started_at),
                openaiRequestId=self._extract_request_id(response),
                errorCode=FailureReasonCode.VALIDATION_ERROR.value,
                error=str(exc),
            )
            return build_job_posting_generate_fallback(extracted)
        except (RetryableWorkerError, NonRetryableWorkerError):
            observe_llm_request(self._task_type, operation, "failed", self._elapsed_seconds(started_at))
            raise
        observe_llm_request(self._task_type, operation, "succeeded", self._elapsed_seconds(started_at))
        log_info(
            logger,
            "openai.generate.completed",
            "OpenAI generate 호출이 완료되었습니다.",
            model=self._model,
            latencyMs=self._elapsed_millis(started_at),
            openaiRequestId=self._extract_request_id(response),
        )
        return result

    def _create_response(self, *, input_payload: object, temperature: float, operation: str, event_prefix: str):
        started_at = monotonic()
        try:
            return self._client.responses.create(
                model=self._model,
                temperature=temperature,
                input=input_payload,
            )
        except Exception as exc:
            self._raise_create_response_error(
                operation=operation,
                event_prefix=event_prefix,
                started_at=started_at,
                exc=exc,
            )

    async def _create_response_async(self, *, input_payload: object, temperature: float, operation: str, event_prefix: str):
        started_at = monotonic()
        try:
            return await await_if_needed(
                self._async_client.responses.create(
                    model=self._model,
                    temperature=temperature,
                    input=input_payload,
                )
            )
        except Exception as exc:
            self._raise_create_response_error(
                operation=operation,
                event_prefix=event_prefix,
                started_at=started_at,
                exc=exc,
            )

    def _raise_create_response_error(
        self,
        *,
        operation: str,
        event_prefix: str,
        started_at: float,
        exc: Exception,
    ) -> None:
        failure_reason = classify_openai_failure(exc)
        if isinstance(exc, RateLimitError):
            increment_llm_request_error(self._task_type, operation, failure_reason.value)
            self._log_openai_failure(event_prefix, started_at, exc)
            raise RetryableWorkerError(
                f"OpenAI {operation} rate limit 발생: {exc}",
                failure_reason=failure_reason.value,
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        if isinstance(exc, (APITimeoutError, APIConnectionError)):
            increment_llm_request_error(self._task_type, operation, failure_reason.value)
            self._log_openai_failure(event_prefix, started_at, exc)
            raise RetryableWorkerError(
                f"OpenAI {operation} timeout 발생: {exc}",
                failure_reason=failure_reason.value,
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        if isinstance(exc, BadRequestError):
            increment_llm_request_error(self._task_type, operation, failure_reason.value)
            self._log_openai_failure(event_prefix, started_at, exc)
            raise NonRetryableWorkerError(
                f"OpenAI {operation} 요청 검증 실패: {exc}",
                failure_reason=failure_reason.value,
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        if isinstance(exc, APIStatusError):
            status_code = getattr(exc, "status_code", None)
            increment_llm_request_error(self._task_type, operation, failure_reason.value)
            self._log_openai_failure(event_prefix, started_at, exc)
            if status_code == 429:
                raise RetryableWorkerError(
                    f"OpenAI {operation} rate limit 발생: {exc}",
                    failure_reason=FailureReasonCode.RATE_LIMIT.value,
                    openai_request_id=self._extract_request_id(exc),
                ) from exc
            if status_code is not None and status_code >= 500:
                raise RetryableWorkerError(
                    f"OpenAI {operation} API 상태 오류: {exc}",
                    failure_reason=FailureReasonCode.INTERNAL_ERROR.value,
                    openai_request_id=self._extract_request_id(exc),
                ) from exc
            raise NonRetryableWorkerError(
                f"OpenAI {operation} 요청 실패: {exc}",
                failure_reason=FailureReasonCode.VALIDATION_ERROR.value,
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        increment_llm_request_error(self._task_type, operation, failure_reason.value)
        self._log_openai_failure(event_prefix, started_at, exc)
        raise RetryableWorkerError(
            f"OpenAI {operation} 처리 중 알 수 없는 오류가 발생했습니다: {exc}",
            failure_reason=failure_reason.value,
            openai_request_id=self._extract_request_id(exc),
        ) from exc

    def _extract_request_id(self, response_or_exc: object) -> str | None:
        for attr_name in ("_request_id", "request_id", "id"):
            value = getattr(response_or_exc, attr_name, None)
            if isinstance(value, str) and value:
                return value

        response = getattr(response_or_exc, "response", None)
        if response is not None:
            headers = getattr(response, "headers", None)
            if headers:
                request_id = headers.get("x-request-id") or headers.get("request-id")
                if request_id:
                    return request_id
        return None

    def _elapsed_millis(self, started_at: float) -> int:
        return max(int((monotonic() - started_at) * 1000), 0)

    def _elapsed_seconds(self, started_at: float) -> float:
        return max(monotonic() - started_at, 0.0)

    def _log_openai_failure(self, event_prefix: str, started_at: float, exc: Exception) -> None:
        log_warning(
            logger,
            f"{event_prefix}.failed",
            "OpenAI 호출이 실패했습니다.",
            model=self._model,
            latencyMs=self._elapsed_millis(started_at),
            openaiRequestId=self._extract_request_id(exc),
            errorCode=classify_openai_failure(exc).value,
            error=str(exc),
        )

class AnalysisOpenAiWorker:
    def __init__(self) -> None:
        timeout = _openai_timeout_seconds()
        self._client = OpenAI(api_key=settings.openai_api_key, timeout=timeout)
        self._async_client = (
            AsyncOpenAI(api_key=settings.openai_api_key, timeout=timeout)
            if AsyncOpenAI is not None
            else OpenAI(api_key=settings.openai_api_key, timeout=timeout)
        )
        self._model = settings.openai_analysis_model
        self._task_type = "ANALYSIS"

    def _build_analysis_prompt(self, context: AnalysisWorkerContextResponse) -> str:
        return build_analysis_prompt(context)

    def analyze(self, context: AnalysisWorkerContextResponse) -> tuple[AnalysisLlmResponse, str | None]:
        operation = "analysis-final"
        prompt = self._build_analysis_prompt(context)
        started_at = monotonic()
        log_info(
            logger,
            "openai.generate.started",
            "OpenAI analysis 호출을 시작합니다.",
            model=self._model,
            operation="analysis",
            questionCount=len(context.questions),
        )

        try:
            response = self._client.responses.create(
                model=self._model,
                temperature=0.2,
                input=prompt,
            )
        except Exception as exc:
            self._raise_create_response_error(
                operation=operation,
                event_prefix="openai.generate",
                started_at=started_at,
                exc=exc,
                event_operation="analysis",
            )

        try:
            result = parse_analysis_response(response.output_text)
            request_id = self._extract_request_id(response)
            usage_fields = self._extract_usage_fields(response)
            log_info(
                logger,
                "openai.generate.completed",
                "OpenAI analysis 호출이 완료되었습니다.",
                model=self._model,
                operation="analysis",
                latencyMs=self._elapsed_millis(started_at),
                openaiRequestId=request_id,
                **usage_fields,
            )
            observe_llm_request(self._task_type, operation, "succeeded", self._elapsed_seconds(started_at))
            return result, request_id
        except (BadRequestError, ValidationError, json.JSONDecodeError, TypeError, ValueError) as exc:
            increment_llm_request_error(self._task_type, operation, FailureReasonCode.VALIDATION_ERROR.value)
            self._log_openai_failure("openai.generate", started_at, exc, operation="analysis")
            observe_llm_request(self._task_type, operation, "failed", self._elapsed_seconds(started_at))
            raise NonRetryableWorkerError(
                f"OpenAI 입력/응답 검증 실패: {exc}",
                failure_reason=FailureReasonCode.VALIDATION_ERROR.value,
                openai_request_id=self._extract_request_id(exc),
            ) from exc

    async def analyze_async(self, context: AnalysisWorkerContextResponse) -> tuple[AnalysisLlmResponse, str | None]:
        operation = "analysis-final"
        prompt = self._build_analysis_prompt(context)
        started_at = monotonic()
        log_info(
            logger,
            "openai.generate.started",
            "OpenAI analysis 호출을 시작합니다.",
            model=self._model,
            operation="analysis",
            questionCount=len(context.questions),
        )

        try:
            response = await await_if_needed(
                self._async_client.responses.create(
                    model=self._model,
                    temperature=0.2,
                    input=prompt,
                )
            )
        except Exception as exc:
            self._raise_create_response_error(
                operation=operation,
                event_prefix="openai.generate",
                started_at=started_at,
                exc=exc,
                event_operation="analysis",
            )

        try:
            result = parse_analysis_response(response.output_text)
            request_id = self._extract_request_id(response)
            usage_fields = self._extract_usage_fields(response)
            log_info(
                logger,
                "openai.generate.completed",
                "OpenAI analysis 호출이 완료되었습니다.",
                model=self._model,
                operation="analysis",
                latencyMs=self._elapsed_millis(started_at),
                openaiRequestId=request_id,
                **usage_fields,
            )
            observe_llm_request(self._task_type, operation, "succeeded", self._elapsed_seconds(started_at))
            return result, request_id
        except (BadRequestError, ValidationError, json.JSONDecodeError, TypeError, ValueError) as exc:
            increment_llm_request_error(self._task_type, operation, FailureReasonCode.VALIDATION_ERROR.value)
            self._log_openai_failure("openai.generate", started_at, exc, operation="analysis")
            observe_llm_request(self._task_type, operation, "failed", self._elapsed_seconds(started_at))
            raise NonRetryableWorkerError(
                f"OpenAI 입력/응답 검증 실패: {exc}",
                failure_reason=FailureReasonCode.VALIDATION_ERROR.value,
                openai_request_id=self._extract_request_id(exc),
            ) from exc

    def _extract_request_id(self, response_or_exc: object) -> str | None:
        for attr_name in ("_request_id", "request_id", "id"):
            value = getattr(response_or_exc, attr_name, None)
            if isinstance(value, str) and value:
                return value

        response = getattr(response_or_exc, "response", None)
        if response is not None:
            headers = getattr(response, "headers", None)
            if headers:
                request_id = headers.get("x-request-id") or headers.get("request-id")
                if request_id:
                    return request_id
        return None

    def _elapsed_millis(self, started_at: float) -> int:
        return max(int((monotonic() - started_at) * 1000), 0)

    def _elapsed_seconds(self, started_at: float) -> float:
        return max(monotonic() - started_at, 0.0)

    def _extract_usage_fields(self, response: object) -> dict[str, int]:
        usage = getattr(response, "usage", None)
        if usage is None:
            return {}

        input_tokens = self._read_usage_int(usage, "input_tokens")
        output_tokens = self._read_usage_int(usage, "output_tokens")
        total_tokens = self._read_usage_int(usage, "total_tokens")
        input_details = self._read_usage_value(usage, "input_tokens_details")
        output_details = self._read_usage_value(usage, "output_tokens_details")

        fields: dict[str, int] = {}
        if input_tokens is not None:
            fields["inputTokens"] = input_tokens
        if output_tokens is not None:
            fields["outputTokens"] = output_tokens
        if total_tokens is not None:
            fields["totalTokens"] = total_tokens

        cached_tokens = self._read_usage_int(input_details, "cached_tokens")
        if cached_tokens is not None:
            fields["cachedInputTokens"] = cached_tokens

        reasoning_tokens = self._read_usage_int(output_details, "reasoning_tokens")
        if reasoning_tokens is not None:
            fields["reasoningOutputTokens"] = reasoning_tokens

        return fields

    def _read_usage_int(self, source: Any, key: str) -> int | None:
        value = self._read_usage_value(source, key)
        return value if isinstance(value, int) else None

    def _read_usage_value(self, source: Any, key: str) -> Any:
        if source is None:
            return None
        if isinstance(source, dict):
            return source.get(key)
        return getattr(source, key, None)

    def _log_openai_failure(
        self,
        event_prefix: str,
        started_at: float,
        exc: Exception,
        *,
        operation: str | None = None,
    ) -> None:
        log_warning(
            logger,
            f"{event_prefix}.failed",
            "OpenAI 호출이 실패했습니다.",
            model=self._model,
            operation=operation,
            latencyMs=self._elapsed_millis(started_at),
            openaiRequestId=self._extract_request_id(exc),
            errorCode=classify_openai_failure(exc).value,
            error=str(exc),
        )

    def _raise_create_response_error(
        self,
        *,
        operation: str,
        event_prefix: str,
        started_at: float,
        exc: Exception,
        event_operation: str | None = None,
    ) -> None:
        failure_reason = classify_openai_failure(exc)
        increment_llm_request_error(self._task_type, operation, failure_reason.value)
        self._log_openai_failure(event_prefix, started_at, exc, operation=event_operation)
        observe_llm_request(self._task_type, operation, "failed", self._elapsed_seconds(started_at))
        request_id = self._extract_request_id(exc)

        if failure_reason == FailureReasonCode.RATE_LIMIT:
            raise RetryableWorkerError(
                f"OpenAI rate limit 발생: {exc}",
                failure_reason=failure_reason.value,
                openai_request_id=request_id,
            ) from exc
        if failure_reason == FailureReasonCode.OPENAI_TIMEOUT:
            raise RetryableWorkerError(
                f"OpenAI timeout 발생: {exc}",
                failure_reason=failure_reason.value,
                openai_request_id=request_id,
            ) from exc
        if failure_reason == FailureReasonCode.VALIDATION_ERROR:
            raise NonRetryableWorkerError(
                f"OpenAI 입력/응답 검증 실패: {exc}",
                failure_reason=failure_reason.value,
                openai_request_id=request_id,
            ) from exc
        raise RetryableWorkerError(
            f"OpenAI 처리 중 알 수 없는 오류가 발생했습니다: {exc}",
            failure_reason=failure_reason.value,
            openai_request_id=request_id,
        ) from exc
