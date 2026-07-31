from __future__ import annotations

import json

from app.schemas import (
    AnalysisLlmResponse,
    AnalysisQuestionAnalysesRecoveryResponse,
    JobPostingClassificationCandidateResponse,
    JobPostingClassificationResultResponse,
    JobPostingExtractResponse,
    JobPostingGenerateResponse,
)


def parse_job_posting_extract_response(raw_text: str) -> JobPostingExtractResponse:
    return JobPostingExtractResponse.model_validate(parse_json_object(raw_text))


def parse_job_posting_classification_response(raw_text: str) -> JobPostingClassificationResultResponse:
    return JobPostingClassificationResultResponse.model_validate(parse_json_object(raw_text))


def build_job_posting_classification_fallback(
    candidates: list[JobPostingClassificationCandidateResponse],
) -> JobPostingClassificationResultResponse:
    top = candidates[0]
    return JobPostingClassificationResultResponse(
        detailClassificationId=top.detailClassificationId,
        detailClassificationName=top.detailClassificationName,
        middleClassificationName=top.middleClassificationName,
        bigClassificationName=top.bigClassificationName,
        reason="LLM 분류 실패로 1순위 후보를 fallback으로 사용했습니다.",
        confidence=top.score,
    )


def parse_job_posting_generate_response(raw_text: str) -> JobPostingGenerateResponse:
    return JobPostingGenerateResponse.model_validate(parse_json_object(raw_text))


def build_job_posting_generate_fallback(extracted: JobPostingExtractResponse) -> JobPostingGenerateResponse:
    return JobPostingGenerateResponse(
        postingName=extracted.postingName,
        companyName=extracted.companyName,
        jobTitle=extracted.jobTitle,
        task=extracted.task,
        requirements=extracted.requirements,
        preferredQualifications=extracted.preferredQualifications,
        summary="생성 실패로 추출 결과를 기반으로 fallback 응답을 사용했습니다.",
    )


def parse_analysis_response(raw_text: str) -> AnalysisLlmResponse:
    return AnalysisLlmResponse.model_validate(parse_json_object(raw_text))


def parse_analysis_question_analyses_recovery_response(
    raw_text: str,
) -> AnalysisQuestionAnalysesRecoveryResponse:
    return AnalysisQuestionAnalysesRecoveryResponse.model_validate(parse_json_object(raw_text))


def parse_json_object(raw_text: str) -> dict:
    start = raw_text.find("{")
    end = raw_text.rfind("}")
    candidate = raw_text[start : end + 1] if start >= 0 and end >= 0 else raw_text
    return json.loads(candidate)
