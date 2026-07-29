import json
from types import SimpleNamespace
from unittest.mock import patch

from app.openai_client import JobPostingOpenAiWorker
from app.openai_prompts import (
    build_job_posting_extract_prompt,
    build_job_posting_generation_prompt,
)
from app.openai_response_parser import build_job_posting_generate_fallback
from app.processors import JobPostingTaskProcessor
from app.schemas import (
    JobPostingClassificationResultResponse,
    JobPostingExtractResponse,
)


def test_extract_prompt_requires_original_posting_name_or_empty_string() -> None:
    prompt = build_job_posting_extract_prompt("채용 공고 원문", has_image=False)

    assert '"postingName": "string"' in prompt
    assert "원문에 명시된 채용 공고 제목을 그대로 추출" in prompt
    assert "postingName은 반드시 빈 문자열" in prompt
    assert "회사명과 직무명으로 새로 만들거나 요약하지 마세요" in prompt


def test_generation_prompt_and_fallback_preserve_extracted_posting_name() -> None:
    extracted = JobPostingExtractResponse(
        postingName="2026 백엔드 개발자 공개채용",
        companyName="잡드리",
        jobTitle="백엔드 개발자",
    )
    classification = JobPostingClassificationResultResponse(
        detailClassificationId=1,
        detailClassificationName="백엔드",
        middleClassificationName="개발",
        bigClassificationName="IT",
    )

    prompt = build_job_posting_generation_prompt(extracted, classification)
    fallback = build_job_posting_generate_fallback(extracted)

    assert "- 공고명: 2026 백엔드 개발자 공개채용" in prompt
    assert fallback.postingName == "2026 백엔드 개발자 공개채용"


def test_generation_fallback_keeps_missing_posting_name_empty() -> None:
    extracted = JobPostingExtractResponse(
        postingName="",
        companyName="잡드리",
        jobTitle="백엔드 개발자",
    )

    fallback = build_job_posting_generate_fallback(extracted)

    assert fallback.postingName == ""


def test_final_generation_replaces_model_posting_name_with_extracted_value() -> None:
    worker = object.__new__(JobPostingOpenAiWorker)
    worker._task_type = "JOB_POSTING_INGEST"
    worker._model = "test-model"
    extracted = JobPostingExtractResponse(
        postingName="2026 백엔드 개발자 공개채용",
        companyName="잡드리",
        jobTitle="백엔드 개발자",
    )
    response = SimpleNamespace(
        output_text=json.dumps(
            {
                "postingName": "잡드리 백엔드 채용",
                "companyName": "잡드리",
                "jobTitle": "백엔드 개발자",
            }
        ),
        id="response-id",
    )

    with (
        patch("app.openai_client.observe_llm_request"),
        patch("app.openai_client.log_info"),
    ):
        result = worker._finalize_generation_response(
            response=response,
            operation="job-posting-generate",
            started_at=0.0,
            extracted=extracted,
        )

    assert result.postingName == "2026 백엔드 개발자 공개채용"


def test_low_confidence_generation_preserves_extracted_posting_name() -> None:
    processor = object.__new__(JobPostingTaskProcessor)
    extracted = JobPostingExtractResponse(
        postingName="2026 백엔드 개발자 공개채용",
        companyName="잡드리",
        jobTitle="백엔드 개발자",
    )

    generated = processor._build_low_confidence_generated(extracted)

    assert generated.postingName == "2026 백엔드 개발자 공개채용"
