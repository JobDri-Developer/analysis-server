from app.openai_prompts import (
    build_job_posting_extract_prompt,
    build_job_posting_generation_prompt,
)
from app.openai_response_parser import build_job_posting_generate_fallback
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
