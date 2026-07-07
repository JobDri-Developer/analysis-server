from __future__ import annotations

import json
import logging

from pydantic import ValidationError
from openai import APIConnectionError, APIStatusError, APITimeoutError, BadRequestError, OpenAI, RateLimitError

from app.config import settings
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

        response = self._create_response(
            input_payload=[{"role": "user", "content": content}],
            temperature=0.1,
            operation="extract",
        )
        try:
            payload = self._parse_json(response.output_text)
            return JobPostingExtractResponse.model_validate(payload)
        except (ValidationError, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise NonRetryableWorkerError(
                f"OpenAI extract 응답 검증 실패: {exc}",
                failure_reason="VALIDATION_ERROR",
                openai_request_id=self._extract_request_id(response),
            ) from exc

    def classify(
        self,
        extracted: JobPostingExtractResponse,
        candidates: list[JobPostingClassificationCandidateResponse],
    ) -> JobPostingClassificationResultResponse:
        prompt = self._build_classification_prompt(extracted, candidates)
        response = self._create_response(
            input_payload=prompt,
            temperature=0.1,
            operation="classify",
        )
        try:
            payload = self._parse_json(response.output_text)
            return JobPostingClassificationResultResponse.model_validate(payload)
        except (ValidationError, json.JSONDecodeError, TypeError, ValueError) as exc:
            top = candidates[0]
            logger.warning(
                "OpenAI classify 응답 검증 실패로 1순위 후보 fallback을 사용합니다. requestId=%s error=%s",
                self._extract_request_id(response),
                exc,
            )
            return JobPostingClassificationResultResponse(
                detailClassificationId=top.detailClassificationId,
                detailClassificationName=top.detailClassificationName,
                middleClassificationName=top.middleClassificationName,
                bigClassificationName=top.bigClassificationName,
                reason="LLM 분류 실패로 1순위 후보를 fallback으로 사용했습니다.",
                confidence=top.score,
            )

    def generate(
        self,
        extracted: JobPostingExtractResponse,
        classification: JobPostingClassificationResultResponse,
    ) -> JobPostingGenerateResponse:
        prompt = self._build_generation_prompt(extracted, classification)
        response = self._create_response(
            input_payload=prompt,
            temperature=0.7,
            operation="generate",
        )
        try:
            payload = self._parse_json(response.output_text)
            return JobPostingGenerateResponse.model_validate(payload)
        except (ValidationError, json.JSONDecodeError, TypeError, ValueError) as exc:
            logger.warning(
                "OpenAI generate 응답 검증 실패로 추출 기반 fallback을 사용합니다. requestId=%s error=%s",
                self._extract_request_id(response),
                exc,
            )
            return JobPostingGenerateResponse(
                companyName=extracted.companyName,
                jobTitle=extracted.jobTitle,
                task=extracted.task,
                requirements=extracted.requirements,
                preferredQualifications=extracted.preferredQualifications,
                summary="생성 실패로 추출 결과를 기반으로 fallback 응답을 사용했습니다.",
            )

    def _create_response(self, *, input_payload: object, temperature: float, operation: str):
        try:
            return self._client.responses.create(
                model=self._model,
                temperature=temperature,
                input=input_payload,
            )
        except RateLimitError as exc:
            raise RetryableWorkerError(
                f"OpenAI {operation} rate limit 발생: {exc}",
                failure_reason="RATE_LIMIT",
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        except APITimeoutError as exc:
            raise RetryableWorkerError(
                f"OpenAI {operation} timeout 발생: {exc}",
                failure_reason="OPENAI_TIMEOUT",
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        except APIConnectionError as exc:
            raise RetryableWorkerError(
                f"OpenAI {operation} connection error 발생: {exc}",
                failure_reason="OPENAI_TIMEOUT",
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        except BadRequestError as exc:
            raise NonRetryableWorkerError(
                f"OpenAI {operation} 요청 검증 실패: {exc}",
                failure_reason="VALIDATION_ERROR",
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        except APIStatusError as exc:
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

        try:
            response = self._client.responses.create(
                model=self._model,
                temperature=0.2,
                input=prompt,
            )
            payload = self._parse_json(response.output_text)
            return AnalysisLlmResponse.model_validate(payload), self._extract_request_id(response)
        except RateLimitError as exc:
            raise RetryableWorkerError(
                f"OpenAI rate limit 발생: {exc}",
                failure_reason="RATE_LIMIT",
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        except APITimeoutError as exc:
            raise RetryableWorkerError(
                f"OpenAI timeout 발생: {exc}",
                failure_reason="OPENAI_TIMEOUT",
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        except (BadRequestError, json.JSONDecodeError, ValueError) as exc:
            raise NonRetryableWorkerError(
                f"OpenAI 입력/응답 검증 실패: {exc}",
                failure_reason="VALIDATION_ERROR",
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        except APIConnectionError as exc:
            raise RetryableWorkerError(
                f"OpenAI 연결 실패: {exc}",
                failure_reason="OPENAI_TIMEOUT",
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        except APIStatusError as exc:
            failure_reason = "INTERNAL_ERROR"
            if getattr(exc, "status_code", None) == 429:
                failure_reason = "RATE_LIMIT"
            raise RetryableWorkerError(
                f"OpenAI API 상태 오류: {exc}",
                failure_reason=failure_reason,
                openai_request_id=self._extract_request_id(exc),
            ) from exc
        except Exception as exc:
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
전체 피드백과 각 문항별 분석을 JSON으로만 반환하세요.

반드시 아래 스키마만 반환하세요.
{{
  "jobFit": 0,
  "impact": 0,
  "completeness": 0,
  "feedback": "string",
  "questionAnalyses": [
    {{
      "questionId": 1,
      "sentence": "string",
      "status": "GOOD|NEEDS_IMPROVEMENT|RISK",
      "reason": "string",
      "improvement": "string"
    }}
  ]
}}

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
