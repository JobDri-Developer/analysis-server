from __future__ import annotations

import json
import logging
from time import monotonic
from typing import Any

from pydantic import ValidationError
from openai import APIConnectionError, APIStatusError, APITimeoutError, BadRequestError, OpenAI, RateLimitError

from app.config import settings
from app.logging_utils import log_info, log_warning
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


class JobPostingOpenAiWorker:
    def __init__(self) -> None:
        self._client = OpenAI(api_key=settings.openai_api_key)
        self._model = settings.openai_job_posting_model

    def extract(self, raw_text: str | None, image_url: str | None) -> JobPostingExtractResponse:
        prompt = self._build_extract_prompt(raw_text or "", image_url is not None)
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
            payload = self._parse_json(response.output_text)
            result = JobPostingExtractResponse.model_validate(payload)
        except (ValidationError, json.JSONDecodeError, TypeError, ValueError) as exc:
            log_warning(
                logger,
                "openai.extract.failed",
                "OpenAI extract 응답 검증에 실패했습니다.",
                model=self._model,
                latencyMs=self._elapsed_millis(started_at),
                openaiRequestId=self._extract_request_id(response),
                error=str(exc),
            )
            raise NonRetryableWorkerError(
                f"OpenAI extract 응답 검증 실패: {exc}",
                failure_reason="VALIDATION_ERROR",
                openai_request_id=self._extract_request_id(response),
            ) from exc
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
        prompt = self._build_classification_prompt(extracted, candidates)
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
            payload = self._parse_json(response.output_text)
            result = JobPostingClassificationResultResponse.model_validate(payload)
        except (ValidationError, json.JSONDecodeError, TypeError, ValueError) as exc:
            log_warning(
                logger,
                "openai.classify.failed",
                "OpenAI classify 응답 검증에 실패해 fallback을 사용합니다.",
                model=self._model,
                latencyMs=self._elapsed_millis(started_at),
                openaiRequestId=self._extract_request_id(response),
                error=str(exc),
            )
            top = candidates[0]
            return JobPostingClassificationResultResponse(
                detailClassificationId=top.detailClassificationId,
                detailClassificationName=top.detailClassificationName,
                middleClassificationName=top.middleClassificationName,
                bigClassificationName=top.bigClassificationName,
                reason="LLM 분류 실패로 1순위 후보를 fallback으로 사용했습니다.",
                confidence=top.score,
            )
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
        prompt = self._build_generation_prompt(extracted, classification)
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
            payload = self._parse_json(response.output_text)
            result = JobPostingGenerateResponse.model_validate(payload)
        except (ValidationError, json.JSONDecodeError, TypeError, ValueError) as exc:
            log_warning(
                logger,
                "openai.generate.failed",
                "OpenAI generate 응답 검증에 실패해 fallback을 사용합니다.",
                model=self._model,
                latencyMs=self._elapsed_millis(started_at),
                openaiRequestId=self._extract_request_id(response),
                error=str(exc),
            )
            return JobPostingGenerateResponse(
                companyName=extracted.companyName,
                jobTitle=extracted.jobTitle,
                task=extracted.task,
                requirements=extracted.requirements,
                preferredQualifications=extracted.preferredQualifications,
                summary="생성 실패로 추출 결과를 기반으로 fallback 응답을 사용했습니다.",
            )
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
        except RateLimitError as exc:
            self._log_openai_failure(event_prefix, started_at, exc)
            raise RetryableWorkerError(
                f"OpenAI {operation} rate limit 발생: {exc}",
                failure_reason="RATE_LIMIT",
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        except APITimeoutError as exc:
            self._log_openai_failure(event_prefix, started_at, exc)
            raise RetryableWorkerError(
                f"OpenAI {operation} timeout 발생: {exc}",
                failure_reason="OPENAI_TIMEOUT",
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        except APIConnectionError as exc:
            self._log_openai_failure(event_prefix, started_at, exc)
            raise RetryableWorkerError(
                f"OpenAI {operation} connection error 발생: {exc}",
                failure_reason="OPENAI_TIMEOUT",
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        except BadRequestError as exc:
            self._log_openai_failure(event_prefix, started_at, exc)
            raise NonRetryableWorkerError(
                f"OpenAI {operation} 요청 검증 실패: {exc}",
                failure_reason="VALIDATION_ERROR",
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        except APIStatusError as exc:
            self._log_openai_failure(event_prefix, started_at, exc)
            status_code = getattr(exc, "status_code", None)
            if status_code == 429:
                raise RetryableWorkerError(
                    f"OpenAI {operation} rate limit 발생: {exc}",
                    failure_reason="RATE_LIMIT",
                    openai_request_id=self._extract_request_id(exc),
                ) from exc
            if status_code is not None and status_code >= 500:
                raise RetryableWorkerError(
                    f"OpenAI {operation} API 상태 오류: {exc}",
                    failure_reason="INTERNAL_ERROR",
                    openai_request_id=self._extract_request_id(exc),
                ) from exc
            raise NonRetryableWorkerError(
                f"OpenAI {operation} 요청 실패: {exc}",
                failure_reason="VALIDATION_ERROR",
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        except Exception as exc:
            self._log_openai_failure(event_prefix, started_at, exc)
            raise RetryableWorkerError(
                f"OpenAI {operation} 처리 중 알 수 없는 오류가 발생했습니다: {exc}",
                failure_reason="INTERNAL_ERROR",
                openai_request_id=self._extract_request_id(exc),
            ) from exc

    def _parse_json(self, raw_text: str) -> dict:
        start = raw_text.find("{")
        end = raw_text.rfind("}")
        candidate = raw_text[start : end + 1] if start >= 0 and end >= 0 else raw_text
        return json.loads(candidate)

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

    def _log_openai_failure(self, event_prefix: str, started_at: float, exc: Exception) -> None:
        log_warning(
            logger,
            f"{event_prefix}.failed",
            "OpenAI 호출이 실패했습니다.",
            model=self._model,
            latencyMs=self._elapsed_millis(started_at),
            openaiRequestId=self._extract_request_id(exc),
            error=str(exc),
        )

    def _build_extract_prompt(self, raw_text: str, has_image: bool) -> str:
        return f"""
이 {"이미지 또는 텍스트" if has_image else "텍스트"}는 채용 공고입니다.
회사명, 직무명, 주요 업무, 자격 요건, 우대 사항을 추출해주세요.
반드시 아래 JSON 형식만 반환하세요.

{{
  "companyName": "string",
  "jobTitle": "string",
  "task": "string",
  "requirements": "string",
  "preferredQualifications": "string",
  "rawText": "string",
  "confidence": 0.0
}}

[채용 공고 텍스트]
{raw_text}
""".strip()

    def _build_classification_prompt(
        self,
        extracted: JobPostingExtractResponse,
        candidates: list[JobPostingClassificationCandidateResponse],
    ) -> str:
        candidate_text = "\n".join(
            [
                (
                    f"- id={candidate.detailClassificationId} | 대분류={candidate.bigClassificationName} "
                    f"| 중분류={candidate.middleClassificationName} | 소분류={candidate.detailClassificationName} "
                    f"| score={candidate.score:.4f}"
                )
                for candidate in candidates
            ]
        )
        return f"""
다음 채용 공고 정보에 가장 적합한 소분류 후보를 하나 선택하세요.
반드시 JSON만 반환하세요.

{{
  "detailClassificationId": 1,
  "detailClassificationName": "string",
  "middleClassificationName": "string",
  "bigClassificationName": "string",
  "reason": "string",
  "confidence": 0.0
}}

[추출 결과]
- 회사명: {extracted.companyName}
- 직무명: {extracted.jobTitle}
- 주요 업무: {extracted.task}
- 자격 요건: {extracted.requirements}
- 우대 사항: {extracted.preferredQualifications}

[후보]
{candidate_text}
""".strip()

    def _build_generation_prompt(
        self,
        extracted: JobPostingExtractResponse,
        classification: JobPostingClassificationResultResponse,
    ) -> str:
        return f"""
다음 정보를 기반으로 저장 가능한 채용 공고 정제 결과를 JSON으로 생성하세요.
반드시 JSON만 반환하세요.

{{
  "companyName": "string",
  "jobTitle": "string",
  "task": "string",
  "requirements": "string",
  "preferredQualifications": "string",
  "summary": "string"
}}

[추출 결과]
- 회사명: {extracted.companyName}
- 직무명: {extracted.jobTitle}
- 주요 업무: {extracted.task}
- 자격 요건: {extracted.requirements}
- 우대 사항: {extracted.preferredQualifications}

[분류 결과]
- 대분류: {classification.bigClassificationName}
- 중분류: {classification.middleClassificationName}
- 소분류: {classification.detailClassificationName}
""".strip()


class AnalysisOpenAiWorker:
    def __init__(self) -> None:
        self._client = OpenAI(api_key=settings.openai_api_key)
        self._model = settings.openai_analysis_model

    def analyze(self, context: AnalysisWorkerContextResponse) -> tuple[AnalysisLlmResponse, str | None]:
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
            payload = self._parse_json(response.output_text)
            result = AnalysisLlmResponse.model_validate(payload)
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
            return result, request_id
        except RateLimitError as exc:
            self._log_openai_failure("openai.generate", started_at, exc, operation="analysis")
            raise RetryableWorkerError(
                f"OpenAI rate limit 발생: {exc}",
                failure_reason="RATE_LIMIT",
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        except APITimeoutError as exc:
            self._log_openai_failure("openai.generate", started_at, exc, operation="analysis")
            raise RetryableWorkerError(
                f"OpenAI timeout 발생: {exc}",
                failure_reason="OPENAI_TIMEOUT",
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        except (BadRequestError, json.JSONDecodeError, ValueError) as exc:
            self._log_openai_failure("openai.generate", started_at, exc, operation="analysis")
            raise NonRetryableWorkerError(
                f"OpenAI 입력/응답 검증 실패: {exc}",
                failure_reason="VALIDATION_ERROR",
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        except APIConnectionError as exc:
            self._log_openai_failure("openai.generate", started_at, exc, operation="analysis")
            raise RetryableWorkerError(
                f"OpenAI 연결 실패: {exc}",
                failure_reason="OPENAI_TIMEOUT",
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        except APIStatusError as exc:
            self._log_openai_failure("openai.generate", started_at, exc, operation="analysis")
            failure_reason = "INTERNAL_ERROR"
            if getattr(exc, "status_code", None) == 429:
                failure_reason = "RATE_LIMIT"
            raise RetryableWorkerError(
                f"OpenAI API 상태 오류: {exc}",
                failure_reason=failure_reason,
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        except Exception as exc:
            self._log_openai_failure("openai.generate", started_at, exc, operation="analysis")
            raise RetryableWorkerError(
                f"OpenAI 처리 중 알 수 없는 오류가 발생했습니다: {exc}",
                failure_reason="INTERNAL_ERROR",
                openai_request_id=self._extract_request_id(exc),
            ) from exc

    def _parse_json(self, raw_text: str) -> dict:
        start = raw_text.find("{")
        end = raw_text.rfind("}")
        candidate = raw_text[start : end + 1] if start >= 0 and end >= 0 else raw_text
        return json.loads(candidate)

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
            error=str(exc),
        )

    def _build_analysis_prompt(self, context: AnalysisWorkerContextResponse) -> str:
        question_block = "\n".join(
            [
                (
                    f"- questionId={question.questionId}\n"
                    f"  question={question.question}\n"
                    f"  answer={question.answer}\n"
                    f"  charLimit={question.charLimit}"
                )
                for question in context.questions
            ]
        )
        return f"""
당신은 자기소개서 분석 평가자입니다.
지원 직무 적합도, 답변의 임팩트, 전체 완성도를 0부터 100 사이 정수로 평가하고,
전체 피드백, 핵심 강점/약점, 누락 키워드, 각 문항별 분석을 JSON으로만 반환하세요.

반드시 아래 스키마만 반환하세요.
{{
  "jobFit": 0,
  "impact": 0,
  "completeness": 0,
  "feedback": "string",
  "keyStrengths": [
    {{
      "title": "짧은 핵심 강점 문장",
      "quote": "자소서 답변에 실제 포함된 정확한 부분 문자열"
    }}
  ],
  "keyWeaknesses": [
    {{
      "title": "짧은 핵심 약점 문장",
      "quote": "JD 또는 자소서 답변에 실제 포함된 정확한 부분 문자열"
    }}
  ],
  "missingKeywords": [
    {{
      "keyword": "JD에는 있지만 답변에서 충분히 드러나지 않은 짧은 역량/요건",
      "source": "qualification|preference|mainTask"
    }}
  ],
  "questionAnalyses": [
    {{
      "questionId": 1,
      "sentence": "string",
      "status": "proven|mentioned|fabricated",
      "reason": "string",
      "improvement": "string"
    }}
  ]
}}

[판정 규칙]
- jobFit, impact, completeness는 0부터 100 사이 정수만 사용한다.
- questionAnalyses의 questionId는 입력된 questionId 중 하나만 사용한다.
- questionAnalyses의 sentence는 반드시 해당 questionId의 answer에 실제 포함된 정확한 substring이어야 한다.
- answer가 비어 있지 않은 모든 입력 문항은 questionAnalyses에 최소 1개 이상 포함한다.
- questionAnalyses는 비어 있지 않은 answer를 가진 모든 questionId를 빠짐없이 커버해야 한다.
- 각 문항에서 가장 평가 가치가 큰 실제 문장 1개를 우선 선택하고, 필요하면 문항당 최대 2개까지 포함한다.
- 강한 긍정 근거가 부족한 문항도 생략하지 말고, 해당 answer의 실제 문장 1개를 골라 mentioned 또는 fabricated로 평가한다.
- 원문 매칭이 불확실하면 문장을 요약하거나 재작성하지 말고, 해당 answer에서 더 짧고 정확히 일치하는 substring을 다시 선택한다.
- status는 proven, mentioned, fabricated 중 하나만 사용한다.
- proven: 답변에 구체적인 근거, 행동, 결과가 충분히 드러남
- mentioned: 관련 키워드나 경험은 있으나 구체적인 근거, 에피소드, 결과가 부족함
- fabricated: 답변에 없는 내용을 있는 것처럼 주장하거나 과장 위험이 큼
- 관련 언급이 전혀 없는 missing 사례는 원문 sentence가 없으므로 questionAnalyses에는 사용하지 말고 missingKeywords와 keyWeaknesses로만 표현한다.
- keyStrengths와 keyWeaknesses는 각각 최대 3개이며, 없으면 []로 출력한다.
- keyStrengths의 quote는 자소서 answer에 실제 포함된 substring만 사용한다.
- missingKeywords는 최대 3개이며, 없으면 []로 출력한다.
- missingKeywords의 source는 qualification, preference, mainTask 중 하나만 사용한다.
- keyWeaknesses의 첫 항목들은 가능하면 missingKeywords와 같은 누락 요건을 다룬다.
- missingKeywords 기반 keyWeaknesses의 quote는 JD의 주요 업무, 자격 요건, 우대 사항에 실제 포함된 표현을 사용한다.
- missingKeywords가 없으면 keyWeaknesses는 questionAnalyses의 보완 대상 문장 quote를 우선 사용한다.
- 모든 title은 한 문장으로 짧게 작성한다.

[채용 공고]
- 회사명: {context.companyName}
- 직무명: {context.jobTitle}
- 주요 업무: {context.task}
- 자격 요건: {context.requirements}
- 우대 사항: {context.preferredQualifications}
- 직무 분류: {context.bigClassificationName} > {context.middleClassificationName} > {context.detailClassificationName}

[문항 및 답변]
{question_block}
""".strip()
